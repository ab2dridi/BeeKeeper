r"""Create a fragmented Hive external table for lakekeeper demo/testing.

Generates a partitioned table (date) with many small files per partition,
simulating a real incremental pipeline that has accumulated small-file debt.

Usage on a Kerberized cluster:
    spark-submit \
        --master yarn --deploy-mode client \
        --principal user@REALM.COM \
        --keytab /etc/security/keytabs/user.keytab \
        demo/create_demo_table.py

Optional arguments:
    --database          Target Hive database (default: lakekeeper_demo)
    --table             Table name (default: events)
    --files-per-part    Number of small files per date partition (default: 200)
    --rows-per-part     Number of rows per date partition (default: 100000)
    --days              Number of date partitions to create (default: 3)
    --warehouse-path    Override HDFS warehouse dir (auto-detected by default)

After the script completes, run:
    lakekeeper analyze --table lakekeeper_demo.events
    lakekeeper compact --table lakekeeper_demo.events
    lakekeeper analyze --table lakekeeper_demo.events   # verify result
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SCHEMA = StructType(
    [
        StructField("event_id", LongType(), nullable=False),
        StructField("user_id", LongType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=True),
    ]
)


def _get_conf(spark: SparkSession, *keys: str, default: str = "") -> str:
    """Try multiple Spark/Hive config keys and return the first non-empty value."""
    for key in keys:
        try:
            value = spark.conf.get(key)
            if value:
                return value
        except Exception:  # noqa: BLE001
            logger.debug("Config key not found: %s", key)
    return default


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description="Create a fragmented Hive external table for lakekeeper demo")
    p.add_argument("--database", default="lakekeeper_demo", help="Hive database name")
    p.add_argument("--table", default="events", help="Table name")
    p.add_argument(
        "--files-per-part",
        type=int,
        default=200,
        help="Number of small files per date partition (default: 200)",
    )
    p.add_argument(
        "--rows-per-part",
        type=int,
        default=100_000,
        help="Number of rows per date partition (default: 100 000)",
    )
    p.add_argument(
        "--days",
        type=int,
        default=3,
        help="Number of date partitions (default: 3)",
    )
    p.add_argument(
        "--warehouse-path",
        default=None,
        help="Override HDFS warehouse directory (auto-detected from cluster config by default)",
    )
    return p.parse_args()


def build_event_df(spark: SparkSession, n_rows: int, n_files: int):
    """Generate n_rows synthetic events, split across n_files Parquet files."""
    return (
        spark.range(n_rows)
        .withColumnRenamed("id", "event_id")
        .withColumn("_r1", F.rand())
        .withColumn("_r2", F.rand())
        .withColumn("user_id", (F.col("_r1") * 50_000).cast(LongType()))
        .withColumn(
            "event_type",
            F.when(F.col("_r2") < 0.50, "view").when(F.col("_r2") < 0.80, "click").otherwise("purchase"),
        )
        .withColumn(
            "amount",
            F.when(
                F.col("event_type") == "purchase",
                F.round(F.rand() * 499 + 1, 2),
            ).otherwise(None),
        )
        .drop("_r1", "_r2")
        .repartition(n_files)  # deliberately fragmented
    )


def main() -> None:
    """Entry point: create the demo table and populate it with fragmented data."""
    args = parse_args()
    full_table = f"{args.database}.{args.table}"

    spark = SparkSession.builder.appName(f"lakekeeper-demo-{args.table}").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Detect warehouse directory from cluster configuration
    if args.warehouse_path:
        warehouse_dir = args.warehouse_path
    else:
        warehouse_dir = _get_conf(
            spark,
            "hive.metastore.warehouse.external.dir",  # CDP 7.x external warehouse
            "hive.metastore.warehouse.dir",  # classic Hive warehouse
            "spark.sql.warehouse.dir",  # Spark warehouse
            default="hdfs:///warehouse",
        )

    table_location = f"{warehouse_dir}/{args.database}.db/{args.table}"
    logger.info("Warehouse dir : %s", warehouse_dir)
    logger.info("Table location: %s", table_location)

    # Setup database and table
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")
    spark.sql(f"DROP TABLE IF EXISTS `{args.database}`.`{args.table}`")
    spark.sql(f"""
        CREATE EXTERNAL TABLE `{args.database}`.`{args.table}` (
            event_id    BIGINT   COMMENT 'Unique event identifier',
            user_id     BIGINT   COMMENT 'User identifier',
            event_type  STRING   COMMENT 'view | click | purchase',
            amount      DOUBLE   COMMENT 'Transaction amount (purchase only)'
        )
        PARTITIONED BY (date STRING COMMENT 'Event date YYYY-MM-DD')
        STORED AS PARQUET
        LOCATION '{table_location}'
        TBLPROPERTIES ('external.table.purge' = 'false')
    """)
    logger.info("External table created: %s", full_table)

    # Generate one date partition at a time with many small files
    dates = [(date.today() - timedelta(days=args.days - 1 - i)).isoformat() for i in range(args.days)]

    total_files = 0
    total_rows = 0

    for d in dates:
        partition_path = f"{table_location}/date={d}"
        df = build_event_df(spark, args.rows_per_part, args.files_per_part)
        df.write.mode("overwrite").parquet(partition_path)
        total_files += args.files_per_part
        total_rows += args.rows_per_part
        logger.info(
            "Written partition date=%s  →  %d files  (%d rows)",
            d,
            args.files_per_part,
            args.rows_per_part,
        )

    # Register all partitions in the Hive Metastore
    spark.sql(f"MSCK REPAIR TABLE `{args.database}`.`{args.table}`")
    logger.info("Partitions registered in Hive Metastore.")

    # Verify row count
    actual_rows = spark.table(full_table).count()

    spark.stop()

    # Summary
    sep = "=" * 60
    print(f"\n{sep}")
    print("  DEMO TABLE READY — lakekeeper")
    print(sep)
    print(f"  Table      : {full_table}")
    print(f"  Location   : {table_location}")
    print(f"  Partitions : {len(dates)}  (date)")
    print(f"  Files      : {total_files}  ({args.files_per_part} per partition)")
    print(f"  Rows       : {actual_rows:,}")
    print()
    print("  Next steps:")
    print(f"    lakekeeper analyze --table {full_table}")
    print(f"    lakekeeper compact --table {full_table}")
    print(f"    lakekeeper analyze --table {full_table}   # verify result")
    print(f"    lakekeeper cleanup --table {full_table}   # free up disk")
    print(sep + "\n")


if __name__ == "__main__":
    main()
