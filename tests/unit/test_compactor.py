"""Tests for beekeeper.core.compactor module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from beekeeper.core.compactor import Compactor
from beekeeper.models import BackupInfo, CompactionStatus
from beekeeper.utils.hdfs import HdfsFileInfo


class TestCompactor:
    @pytest.fixture
    def compactor(self, mock_spark, mock_hdfs_client, config):
        return Compactor(mock_spark, mock_hdfs_client, config)

    @pytest.fixture
    def backup_info(self, sample_table_info):
        return BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )

    def test_compact_non_partitioned_success(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        # Mock read().count() to return same value both times
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write

        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=24,
            total_size_bytes=3 * 1024 * 1024 * 1024,
        )

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        assert report.before_file_count == 65000
        assert report.after_file_count == 24
        assert report.row_count_before == 1000
        assert report.row_count_after == 1000
        assert report.duration_seconds > 0

    def test_compact_non_partitioned_row_mismatch(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        call_count = [0]

        def count_side_effect():
            call_count[0] += 1
            return 1000 if call_count[0] <= 1 else 999

        mock_df = MagicMock()
        mock_df.count.side_effect = count_side_effect
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write

        mock_spark.read.format.return_value.load.return_value = mock_df

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        assert "Row count mismatch" in report.error
        # Should have tried rollback
        mock_spark.sql.assert_any_call(f"ALTER TABLE mydb.events SET LOCATION '{backup_info.original_location}'")

    def test_compact_partitioned_success(self, compactor, mock_spark, mock_hdfs_client, sample_partitioned_table_info):
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )

        mock_df = MagicMock()
        mock_df.count.return_value = 500
        mock_df.coalesce.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write

        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=8,
            total_size_bytes=1 * 1024 * 1024 * 1024,
        )

        report = compactor.compact_table(sample_partitioned_table_info, backup_info)

        assert report.status == CompactionStatus.COMPLETED
        assert report.partitions_compacted == 1
        assert report.partitions_skipped == 1

    def test_compact_exception_triggers_rollback(
        self,
        compactor,
        mock_spark,
        mock_hdfs_client,
        sample_table_info,
        backup_info,
    ):
        mock_spark.read.format.return_value.load.side_effect = RuntimeError("Spark error")

        report = compactor.compact_table(sample_table_info, backup_info)

        assert report.status == CompactionStatus.FAILED
        assert "Spark error" in report.error
