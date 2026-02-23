"""Compaction orchestration - the main workflow engine."""

from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING

from beekeeper.models import CompactionReport, CompactionStatus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from beekeeper.config import BeekeeperConfig
    from beekeeper.models import BackupInfo, PartitionInfo, TableInfo
    from beekeeper.utils.hdfs import HdfsClient

logger = logging.getLogger(__name__)


class Compactor:
    """Orchestrates the compaction workflow for Hive external tables."""

    def __init__(self, spark: SparkSession, hdfs_client: HdfsClient, config: BeekeeperConfig) -> None:
        """Initialize the compactor.

        Args:
            spark: Active SparkSession.
            hdfs_client: HDFS client for file operations.
            config: Beekeeper configuration.
        """
        self._spark = spark
        self._hdfs = hdfs_client
        self._config = config

    def compact_table(self, table_info: TableInfo, backup_info: BackupInfo) -> CompactionReport:
        """Compact a table (partitioned or non-partitioned).

        Args:
            table_info: Table information from analysis.
            backup_info: Backup information.

        Returns:
            CompactionReport with before/after metrics.
        """
        start_time = time.time()
        full_name = table_info.full_name
        format_str = table_info.file_format.value

        logger.info("Starting compaction for %s (format=%s)", full_name, format_str)

        report = CompactionReport(
            table_name=full_name,
            status=CompactionStatus.IN_PROGRESS,
            before_file_count=table_info.total_file_count,
            before_size_bytes=table_info.total_size_bytes,
            before_avg_file_size=table_info.avg_file_size_bytes,
        )

        try:
            if table_info.is_partitioned:
                self._compact_partitioned(table_info, format_str, report)
            else:
                self._compact_non_partitioned(table_info, format_str, report)

            report.status = CompactionStatus.COMPLETED
            logger.info("Compaction completed for %s", full_name)

        except Exception as e:
            report.status = CompactionStatus.FAILED
            report.error = str(e)
            logger.exception("Compaction failed for %s", full_name)
            self._rollback_on_failure(table_info, backup_info)

        report.duration_seconds = time.time() - start_time
        return report

    def _compact_non_partitioned(self, table_info: TableInfo, format_str: str, report: CompactionReport) -> None:
        """Compact a non-partitioned table."""
        full_name = table_info.full_name
        location = table_info.location
        target_files = max(1, math.ceil(table_info.total_size_bytes / self._config.block_size_bytes))
        ts = int(time.time())
        new_location = f"{location}__compacted_{ts}"

        logger.info("Reading data from %s", location)
        original_count = self._spark.read.format(format_str).load(location).count()
        report.row_count_before = original_count

        logger.info("Writing %d target files to %s", target_files, new_location)
        df = self._spark.read.format(format_str).load(location)
        df.coalesce(target_files).write.format(format_str).mode("overwrite").save(new_location)

        new_count = self._spark.read.format(format_str).load(new_location).count()
        report.row_count_after = new_count

        if new_count != original_count:
            self._hdfs.delete_path(new_location)
            msg = f"Row count mismatch for {full_name}: original={original_count}, new={new_count}"
            raise ValueError(msg)

        logger.info("Swapping location for %s to %s", full_name, new_location)
        self._spark.sql(f"ALTER TABLE {full_name} SET LOCATION '{new_location}'")

        new_file_info = self._hdfs.get_file_info(new_location)
        report.after_file_count = new_file_info.file_count
        report.after_size_bytes = new_file_info.total_size_bytes
        report.after_avg_file_size = new_file_info.avg_file_size_bytes

    def _compact_partitioned(self, table_info: TableInfo, format_str: str, report: CompactionReport) -> None:
        """Compact a partitioned table, partition by partition."""
        partitions_compacted = 0
        partitions_skipped = 0
        total_after_files = 0
        total_after_size = 0

        for partition in table_info.partitions:
            if not partition.needs_compaction:
                partitions_skipped += 1
                total_after_files += partition.file_count
                total_after_size += partition.total_size_bytes
                continue

            self._compact_single_partition(table_info, partition, format_str)
            partitions_compacted += 1

            try:
                new_file_info = self._hdfs.get_file_info(self._last_partition_new_location)
                total_after_files += new_file_info.file_count
                total_after_size += new_file_info.total_size_bytes
            except Exception:
                total_after_files += partition.target_files
                total_after_size += partition.total_size_bytes

        report.partitions_compacted = partitions_compacted
        report.partitions_skipped = partitions_skipped
        report.after_file_count = total_after_files
        report.after_size_bytes = total_after_size
        if report.after_file_count > 0:
            report.after_avg_file_size = report.after_size_bytes // report.after_file_count

    def _compact_single_partition(self, table_info: TableInfo, partition: PartitionInfo, format_str: str) -> None:
        """Compact a single partition."""
        full_name = table_info.full_name
        location = partition.location
        target_files = partition.target_files
        ts = int(time.time())
        new_location = f"{location}__compacted_{ts}"
        self._last_partition_new_location = new_location

        logger.info(
            "Compacting partition %s (%d -> %d files)",
            partition.partition_spec_str,
            partition.file_count,
            target_files,
        )

        original_count = self._spark.read.format(format_str).load(location).count()

        df = self._spark.read.format(format_str).load(location)
        df.coalesce(target_files).write.format(format_str).mode("overwrite").save(new_location)

        new_count = self._spark.read.format(format_str).load(new_location).count()

        if new_count != original_count:
            self._hdfs.delete_path(new_location)
            msg = (
                f"Row count mismatch for partition {partition.partition_spec_str}: "
                f"original={original_count}, new={new_count}"
            )
            raise ValueError(msg)

        spec_sql = partition.partition_sql_spec
        self._spark.sql(f"ALTER TABLE {full_name} PARTITION({spec_sql}) SET LOCATION '{new_location}'")

    def _rollback_on_failure(self, table_info: TableInfo, backup_info: BackupInfo) -> None:
        """Rollback to original state on compaction failure."""
        full_name = table_info.full_name
        logger.warning("Rolling back %s to original state", full_name)

        try:
            if table_info.is_partitioned:
                for spec_str, original_location in backup_info.partition_locations.items():
                    parts = spec_str.split("/")
                    spec_sql = ", ".join(f"{p.split('=')[0]}='{p.split('=')[1]}'" for p in parts)
                    self._spark.sql(f"ALTER TABLE {full_name} PARTITION({spec_sql}) SET LOCATION '{original_location}'")
            else:
                self._spark.sql(f"ALTER TABLE {full_name} SET LOCATION '{backup_info.original_location}'")
            logger.info("Rollback successful for %s", full_name)
        except Exception:
            logger.exception("Rollback FAILED for %s - manual intervention required", full_name)
