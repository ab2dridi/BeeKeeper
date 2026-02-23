"""Integration test for the full Beekeeper workflow (with mocked Spark/HDFS)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from beekeeper.config import BeekeeperConfig
from beekeeper.models import CompactionStatus
from beekeeper.utils.hdfs import HdfsFileInfo


def _make_row(col0, col1="", col2=""):
    """Create a mock Row."""
    row = MagicMock()
    row.__getitem__ = lambda self, i: [col0, col1, col2][i]
    return row


class TestFullWorkflow:
    """Test the complete analyze -> backup -> compact -> verify workflow."""

    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        spark._jvm = MagicMock()
        spark._jsc = MagicMock()
        spark.sparkContext.applicationId = "test-integration"
        return spark

    @pytest.fixture
    def config(self):
        return BeekeeperConfig(
            block_size_mb=128,
            compaction_ratio_threshold=10.0,
            log_level="WARNING",
        )

    def test_non_partitioned_workflow(self, mock_spark, config):
        """Full workflow for a non-partitioned table."""
        from beekeeper.engine.hive_external import HiveExternalEngine

        # Setup DESCRIBE FORMATTED mock
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events"),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat"),
            _make_row("Table Type", "EXTERNAL_TABLE"),
        ]

        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,  # analyze: DESCRIBE FORMATTED
            [],  # create_backup: CREATE EXTERNAL TABLE (no return needed)
            desc_rows,  # compact: read & write (sql calls during compact)
        ]

        # Mock HDFS
        with patch("beekeeper.engine.hive_external.HdfsClient") as mock_hdfs_cls:
            mock_hdfs = mock_hdfs_cls.return_value

            # Before compaction: many small files
            before_info = HdfsFileInfo(file_count=65000, total_size_bytes=3 * 1024 * 1024 * 1024)
            # After compaction: few large files
            after_info = HdfsFileInfo(file_count=24, total_size_bytes=3 * 1024 * 1024 * 1024)
            mock_hdfs.get_file_info.side_effect = [before_info, after_info]
            mock_hdfs.delete_path.return_value = True

            # Mock Spark read/write
            mock_df = MagicMock()
            mock_df.count.return_value = 1000000
            mock_df.coalesce.return_value = mock_df
            mock_df.write = MagicMock()
            mock_df.write.format.return_value = mock_df.write
            mock_df.write.mode.return_value = mock_df.write
            mock_spark.read.format.return_value.load.return_value = mock_df

            engine = HiveExternalEngine(mock_spark, config)

            # Step 1: Analyze
            table_info = engine.analyze("mydb", "events")
            assert table_info.needs_compaction is True
            assert table_info.total_file_count == 65000

            # Step 2: Backup
            backup_info = engine.create_backup(table_info)
            assert backup_info.original_table == "mydb.events"
            assert backup_info.backup_table.startswith("mydb.__bkp_events_")

            # Step 3: Compact
            report = engine.compact(table_info, backup_info)
            assert report.status == CompactionStatus.COMPLETED
            assert report.row_count_before == 1000000
            assert report.row_count_after == 1000000

    def test_partitioned_workflow(self, mock_spark, config):
        """Full workflow for a partitioned table."""
        from beekeeper.engine.hive_external import HiveExternalEngine

        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs"),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
            _make_row("# Partition Information", None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("year", "string", ""),
            _make_row("# Detailed Table Information", None),
        ]

        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024",): v[i]),
        ]

        partition_desc = [
            _make_row("Location", "hdfs:///data/mydb/logs/year=2024"),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
        ]

        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,  # analyze: DESCRIBE FORMATTED
            partition_rows,  # analyze: SHOW PARTITIONS
            partition_desc,  # analyze: DESCRIBE PARTITION
            [],  # backup: CREATE TABLE
            [],  # backup: ALTER TABLE ADD PARTITION
        ]

        with patch("beekeeper.engine.hive_external.HdfsClient") as mock_hdfs_cls:
            mock_hdfs = mock_hdfs_cls.return_value

            small_files = HdfsFileInfo(file_count=10000, total_size_bytes=1 * 1024 * 1024 * 1024)
            after_compact = HdfsFileInfo(file_count=8, total_size_bytes=1 * 1024 * 1024 * 1024)
            mock_hdfs.get_file_info.side_effect = [small_files, after_compact]
            mock_hdfs.delete_path.return_value = True

            mock_df = MagicMock()
            mock_df.count.return_value = 500000
            mock_df.coalesce.return_value = mock_df
            mock_df.write = MagicMock()
            mock_df.write.format.return_value = mock_df.write
            mock_df.write.mode.return_value = mock_df.write
            mock_spark.read.format.return_value.load.return_value = mock_df

            engine = HiveExternalEngine(mock_spark, config)

            # Step 1: Analyze
            table_info = engine.analyze("mydb", "logs")
            assert table_info.is_partitioned is True
            assert table_info.needs_compaction is True
            assert len(table_info.partitions) == 1

            # Step 2: Backup
            backup_info = engine.create_backup(table_info)
            assert len(backup_info.partition_locations) == 1

            # Step 3: Compact
            report = engine.compact(table_info, backup_info)
            assert report.status == CompactionStatus.COMPLETED
            assert report.partitions_compacted == 1

    def test_rollback_workflow(self, mock_spark, config):
        """Test rollback after compaction."""
        from beekeeper.engine.hive_external import HiveExternalEngine

        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101_120000" if k == "tableName" else None}),
        ]

        backup_desc = [
            _make_row("Location", "hdfs:///data/mydb/events"),
        ]

        mock_spark.sql.return_value.collect.side_effect = [
            table_rows,  # find_latest_backup: SHOW TABLES
            backup_desc,  # find_latest_backup: DESCRIBE FORMATTED backup
            Exception("no partitions"),  # SHOW PARTITIONS fails (non-partitioned)
        ]

        with patch("beekeeper.engine.hive_external.HdfsClient"):
            engine = HiveExternalEngine(mock_spark, config)
            engine.rollback("mydb", "events")

            # Verify location was restored
            calls = [str(c) for c in mock_spark.sql.call_args_list]
            alter_calls = [c for c in calls if "SET LOCATION" in c and "__bkp" not in c]
            assert len(alter_calls) >= 1
