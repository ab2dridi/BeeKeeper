"""Small CLI for the compactor."""
import argparse
from .compactor import Compactor


def main(argv=None):
    parser = argparse.ArgumentParser(description="Hive external table compactor (PySpark)")
    parser.add_argument("--database", help="Hive database name", required=True)
    parser.add_argument("--table", help="Hive table name (optional)")
    parser.add_argument("--backup-db", help="Backup Hive database name", default="bkp")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("hive-external-compactor").enableHiveSupport().getOrCreate()
    comp = Compactor(spark)

    if args.table:
        res = comp.compact_table(args.database, args.table, backup_db=args.backup_db, dry_run=args.dry_run)
        print(res)
    else:
        tables = [t.name for t in spark.catalog.listTables(args.database) if not t.isTemporary]
        results = []
        for t in tables:
            try:
                r = comp.compact_table(args.database, t, backup_db=args.backup_db, dry_run=args.dry_run)
                results.append(r)
            except Exception as e:
                results.append({"table": t, "error": str(e)})
        print(results)
