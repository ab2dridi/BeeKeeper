"""Hive external table compaction engine."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from beekeeper.core.analyzer import TableAnalyzer
from beekeeper.core.backup import BackupManager
from beekeeper.core.compactor import Compactor
from beekeeper.engine.base import CompactionEngine
from beekeeper.utils.hdfs import HdfsClient

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from beekeeper.config import BeekeeperConfig
    from beekeeper.models import BackupInfo, CompactionReport, TableInfo

logger = logging.getLogger(__name__)


class HiveExternalEngine(CompactionEngine):
    """Compaction engine for Hive external tables."""

    def __init__(self, spark: SparkSession, config: BeekeeperConfig) -> None:
        """Initialize the engine.

        Args:
            spark: Active SparkSession with Hive support.
            config: Beekeeper configuration.
        """
        self._spark = spark
        self._config = config
        self._hdfs = HdfsClient(spark)
        self._analyzer = TableAnalyzer(spark, self._hdfs, config)
        self._backup_mgr = BackupManager(spark, config)
        self._compactor = Compactor(spark, self._hdfs, config)

    def analyze(self, database: str, table_name: str) -> TableInfo:
        """Analyze a table and return detailed information.

        Args:
            database: Database name.
            table_name: Table name.

        Returns:
            TableInfo with analysis results.
        """
        return self._analyzer.analyze_table(database, table_name)

    def create_backup(self, table_info: TableInfo) -> BackupInfo:
        """Create a zero-copy backup of a table.

        Args:
            table_info: Table information from analysis.

        Returns:
            BackupInfo with backup details.
        """
        return self._backup_mgr.create_backup(table_info)

    def compact(self, table_info: TableInfo, backup_info: BackupInfo) -> CompactionReport:
        """Perform compaction on a table.

        Args:
            table_info: Table information from analysis.
            backup_info: Backup information.

        Returns:
            CompactionReport with results.
        """
        return self._compactor.compact_table(table_info, backup_info)

    def rollback(self, database: str, table_name: str, backup_table: str | None = None) -> None:
        """Rollback a table to its pre-compaction state.

        Args:
            database: Database name.
            table_name: Table name.
            backup_table: Specific backup table to use. If None, uses most recent.

        Raises:
            ValueError: If no backup is found.
        """
        full_name = f"{database}.{table_name}"
        logger.info("Rolling back %s", full_name)

        backup_info = self._backup_mgr.find_latest_backup(database, table_name)
        if backup_info is None:
            msg = f"No backup found for {full_name}"
            raise ValueError(msg)

        if backup_info.partition_locations:
            for spec_str, original_location in backup_info.partition_locations.items():
                parts = spec_str.split("/")
                spec_sql = ", ".join(f"{p.split('=')[0]}='{p.split('=')[1]}'" for p in parts)
                self._spark.sql(f"ALTER TABLE {full_name} PARTITION({spec_sql}) SET LOCATION '{original_location}'")
                logger.info("Restored partition %s -> %s", spec_str, original_location)
        else:
            self._spark.sql(f"ALTER TABLE {full_name} SET LOCATION '{backup_info.original_location}'")
            logger.info("Restored table location -> %s", backup_info.original_location)

        self._cleanup_compacted_files(backup_info)
        self._backup_mgr.drop_backup(database, backup_info.backup_table.split(".")[-1])
        logger.info("Rollback complete for %s", full_name)

    def cleanup(self, database: str, table_name: str, older_than_days: int | None = None) -> int:
        """Clean up backup tables and compacted data.

        Args:
            database: Database name.
            table_name: Table name.
            older_than_days: Only clean backups older than this many days.

        Returns:
            Number of backups cleaned up.
        """
        backups = self._backup_mgr.list_backups(database, table_name)
        cleaned = 0
        cutoff = None
        if older_than_days is not None:
            cutoff = datetime.now() - timedelta(days=older_than_days)

        for backup_name in backups:
            if cutoff is not None:
                prefix = f"{self._config.backup_prefix}_{table_name}_"
                ts_str = backup_name.replace(prefix, "")
                try:
                    backup_ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                    if backup_ts >= cutoff:
                        continue
                except ValueError:
                    pass

            self._backup_mgr.drop_backup(database, backup_name)
            cleaned += 1

        logger.info("Cleaned up %d backups for %s.%s", cleaned, database, table_name)
        return cleaned

    def list_tables(self, database: str) -> list[str]:
        """List external tables in a database.

        Args:
            database: Database name.

        Returns:
            List of external table names (excluding backup tables).
        """
        tables = self._spark.sql(f"SHOW TABLES IN {database}").collect()
        external_tables = []

        for row in tables:
            tbl = row["tableName"]
            if tbl.startswith(self._config.backup_prefix):
                continue

            try:
                desc_rows = self._spark.sql(f"DESCRIBE FORMATTED {database}.{tbl}").collect()
                desc_map = {r[0].strip(): (r[1] or "").strip() for r in desc_rows if r[0]}
                table_type = desc_map.get("Table Type", desc_map.get("Table Type:", ""))
                if "EXTERNAL" in table_type.upper():
                    external_tables.append(tbl)
            except Exception:
                logger.debug("Skipping table %s.%s (could not describe)", database, tbl)

        return external_tables

    def _cleanup_compacted_files(self, backup_info: BackupInfo) -> None:
        """Delete compacted files that are no longer needed after rollback."""
        # Compacted locations follow the pattern: <original>__compacted_<ts>
        # We find and delete them by checking for sibling directories with __compacted_ suffix
        if backup_info.partition_locations:
            for _spec, location in backup_info.partition_locations.items():
                self._delete_compacted_siblings(location)
        else:
            self._delete_compacted_siblings(backup_info.original_location)

    def _delete_compacted_siblings(self, original_location: str) -> None:
        """Find and delete __compacted_ directories next to a location."""
        parent = original_location.rsplit("/", 1)[0] if "/" in original_location else original_location
        try:
            hadoop_path = self._spark._jvm.org.apache.hadoop.fs.Path(parent)  # type: ignore[union-attr]
            conf = self._spark._jsc.hadoopConfiguration()  # type: ignore[union-attr]
            fs = hadoop_path.getFileSystem(conf)
            statuses = fs.listStatus(hadoop_path)
            for status in statuses:
                path_name = status.getPath().getName()
                if "__compacted_" in path_name:
                    logger.info("Deleting compacted data: %s", status.getPath().toString())
                    fs.delete(status.getPath(), True)
        except Exception:
            logger.debug("Could not clean compacted siblings for %s", original_location)
