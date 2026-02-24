import math
import time
from typing import Optional, Tuple

from .hdfs_utils import HdfsUtils


class Compactor:
    """Compactor for Hive external tables.

    Usage:
        compactor = Compactor(spark, block_size=128*1024*1024)
        compactor.compact_table(db, table, backup_db='bkp')
    """

    def __init__(self, spark, block_size: int = 128 * 1024 * 1024):
        self.spark = spark
        self.hdfs = HdfsUtils(spark)
        self.block_size = block_size

    def _describe_extended(self, database: str, table: str):
        q = f"DESCRIBE EXTENDED {database}.{table}"
        return self.spark.sql(q).collect()

    def get_table_location(self, database: str, table: str) -> Optional[str]:
        rows = self._describe_extended(database, table)
        for r in rows:
            # DESCRIBE EXTENDED returns (col_name, data_type, comment)
            # Location appears in the second column rows
            if len(r) >= 2 and r[0] and "Location" in str(r[0]):
                return str(r[1]).strip()
            # older Spark may have 'location' as key
            if len(r) >= 2 and r[0] and str(r[0]).strip().lower() == "location":
                return str(r[1]).strip()
        return None

    def analyze_path(self, path: str) -> Tuple[int, int]:
        total_size, file_count = self.hdfs.get_total_size_and_count(path)
        return total_size, file_count

    def compute_target_files(self, total_size: int) -> int:
        n = max(1, math.ceil(total_size / self.block_size))
        return int(n)

    def needs_compaction(self, total_size: int, file_count: int, min_ratio: float = 0.5) -> bool:
        if file_count <= 1:
            return False
        avg = total_size / file_count if file_count else 0
        return avg < (self.block_size * min_ratio)

    def compact_table(self, database: str, table: str, backup_db: str = "bkp", dry_run: bool = False) -> dict:
        """Main compaction flow for a single table.

        Returns a dict with before/after metrics and paths.
        """
        loc = self.get_table_location(database, table)
        if not loc:
            raise ValueError(f"Could not determine location for {database}.{table}")

        total_before, files_before = self.analyze_path(loc)
        result = {
            "database": database,
            "table": table,
            "location": loc,
            "total_before": total_before,
            "files_before": files_before,
        }

        if not self.needs_compaction(total_before, files_before):
            result["status"] = "not_needed"
            return result

        ts = int(time.time())
        backup_path = f"{loc.rstrip('/')}_bkp_{ts}"
        backup_table = f"{table}_bkp_{ts}"

        result["backup_path"] = backup_path

        if dry_run:
            result["status"] = "dry_run_compaction_required"
            return result

        # Move original data to backup (metadata-only rename in HDFS)
        ok = self.hdfs.rename(loc, backup_path)
        if not ok:
            raise RuntimeError(f"Failed to rename {loc} to {backup_path}")

        # Create an external backup table pointing to backup_path
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {backup_db}")
        self.spark.sql(
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {backup_db}.{backup_table} LIKE {database}.{table} LOCATION '{backup_path}'"
        )

        # Read from backup table path and write compacted data back to original location
        df = self.spark.read.format("parquet").load(backup_path)
        total_records = df.count()

        target_files = self.compute_target_files(total_before)
        df_repart = df.repartition(target_files)

        # Write compacted files to original location
        df_repart.write.mode("overwrite").parquet(loc)

        # Validate
        total_after, files_after = self.analyze_path(loc)
        result.update({"total_after": total_after, "files_after": files_after, "records": total_records, "status": "ok"})
        return result
