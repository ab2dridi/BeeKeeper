"""Zero-copy backup management for Hive external tables."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from beekeeper.models import BackupInfo

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from beekeeper.config import BeekeeperConfig
    from beekeeper.models import TableInfo

logger = logging.getLogger(__name__)


class BackupManager:
    """Manages zero-copy backups of Hive external tables."""

    def __init__(self, spark: SparkSession, config: BeekeeperConfig) -> None:
        """Initialize the backup manager.

        Args:
            spark: Active SparkSession.
            config: Beekeeper configuration.
        """
        self._spark = spark
        self._config = config

    def create_backup(self, table_info: TableInfo) -> BackupInfo:
        """Create a zero-copy backup of a table.

        Creates an external table pointing to the same HDFS location(s).
        No data is copied - only metadata references.

        Args:
            table_info: Table information from analysis.

        Returns:
            BackupInfo with backup details.
        """
        timestamp = datetime.now()
        ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
        backup_table_name = f"{self._config.backup_prefix}_{table_info.table_name}_{ts_str}"
        full_backup = f"{table_info.database}.{backup_table_name}"
        full_original = table_info.full_name

        logger.info("Creating zero-copy backup: %s", full_backup)

        self._spark.sql(f"CREATE EXTERNAL TABLE {full_backup} LIKE {full_original} LOCATION '{table_info.location}'")

        partition_locations: dict[str, str] = {}

        if table_info.is_partitioned:
            for partition in table_info.partitions:
                if partition.needs_compaction:
                    spec_sql = partition.partition_sql_spec
                    self._spark.sql(
                        f"ALTER TABLE {full_backup} ADD PARTITION({spec_sql}) LOCATION '{partition.location}'"
                    )
                    partition_locations[partition.partition_spec_str] = partition.location

        backup_info = BackupInfo(
            original_table=full_original,
            backup_table=full_backup,
            original_location=table_info.location,
            timestamp=timestamp,
            partition_locations=partition_locations,
        )

        logger.info("Backup created: %s -> %s", full_original, full_backup)
        return backup_info

    def find_latest_backup(self, database: str, table_name: str) -> BackupInfo | None:
        """Find the most recent backup for a table.

        Args:
            database: Database name.
            table_name: Table name.

        Returns:
            BackupInfo if found, None otherwise.
        """
        prefix = f"{self._config.backup_prefix}_{table_name}_"
        tables = self._spark.sql(f"SHOW TABLES IN {database}").collect()

        backup_tables = []
        for row in tables:
            tbl = row["tableName"]
            if tbl.startswith(prefix):
                backup_tables.append(tbl)

        if not backup_tables:
            logger.warning("No backup found for %s.%s", database, table_name)
            return None

        backup_tables.sort(reverse=True)
        latest = backup_tables[0]
        full_backup = f"{database}.{latest}"

        desc_rows = self._spark.sql(f"DESCRIBE FORMATTED {full_backup}").collect()
        desc_map = {row[0].strip(): (row[1] or "").strip() for row in desc_rows if row[0]}

        location = ""
        for key in ("Location", "Location:"):
            if key in desc_map and desc_map[key]:
                location = desc_map[key]
                break

        ts_str = latest.replace(prefix, "")
        try:
            timestamp = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        except ValueError:
            timestamp = datetime.now()

        partition_locations = self._get_backup_partition_locations(full_backup)

        return BackupInfo(
            original_table=f"{database}.{table_name}",
            backup_table=full_backup,
            original_location=location,
            timestamp=timestamp,
            partition_locations=partition_locations,
        )

    def _get_backup_partition_locations(self, backup_table: str) -> dict[str, str]:
        """Get partition locations from a backup table."""
        partition_locations: dict[str, str] = {}
        try:
            partitions = self._spark.sql(f"SHOW PARTITIONS {backup_table}").collect()
            for row in partitions:
                spec_str = row[0]
                spec_parts = spec_str.split("/")
                spec_sql = ", ".join(f"{p.split('=')[0]}='{p.split('=')[1]}'" for p in spec_parts)
                desc = self._spark.sql(f"DESCRIBE FORMATTED {backup_table} PARTITION({spec_sql})").collect()
                desc_map = {r[0].strip(): (r[1] or "").strip() for r in desc if r[0]}
                for key in ("Location", "Location:"):
                    if key in desc_map and desc_map[key]:
                        partition_locations[spec_str] = desc_map[key]
                        break
        except Exception:
            logger.debug("No partitions in backup table %s", backup_table)

        return partition_locations

    def list_backups(self, database: str, table_name: str) -> list[str]:
        """List all backup tables for a given table.

        Args:
            database: Database name.
            table_name: Table name.

        Returns:
            List of backup table names sorted newest first.
        """
        prefix = f"{self._config.backup_prefix}_{table_name}_"
        tables = self._spark.sql(f"SHOW TABLES IN {database}").collect()

        backup_tables = [row["tableName"] for row in tables if row["tableName"].startswith(prefix)]
        backup_tables.sort(reverse=True)
        return backup_tables

    def drop_backup(self, database: str, backup_table_name: str) -> None:
        """Drop a backup table (metadata only, no data deleted).

        Args:
            database: Database name.
            backup_table_name: Backup table name to drop.
        """
        full_name = f"{database}.{backup_table_name}"
        logger.info("Dropping backup table: %s", full_name)
        self._spark.sql(f"DROP TABLE IF EXISTS {full_name}")
