"""Tests for beekeeper.core.backup module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from beekeeper.core.backup import BackupManager


class TestBackupManager:
    @pytest.fixture
    def backup_mgr(self, mock_spark, config):
        return BackupManager(mock_spark, config)

    def test_create_backup_non_partitioned(self, backup_mgr, mock_spark, sample_table_info):
        backup_info = backup_mgr.create_backup(sample_table_info)

        assert backup_info.original_table == "mydb.events"
        assert backup_info.backup_table.startswith("mydb.__bkp_events_")
        assert backup_info.original_location == "hdfs:///data/mydb/events"
        assert backup_info.partition_locations == {}
        assert isinstance(backup_info.timestamp, datetime)

        # Verify CREATE EXTERNAL TABLE was called
        calls = mock_spark.sql.call_args_list
        create_call = [c for c in calls if "CREATE EXTERNAL TABLE" in str(c)]
        assert len(create_call) == 1

    def test_create_backup_partitioned(self, backup_mgr, mock_spark, sample_partitioned_table_info):
        backup_info = backup_mgr.create_backup(sample_partitioned_table_info)

        assert backup_info.original_table == "mydb.logs"
        assert backup_info.backup_table.startswith("mydb.__bkp_logs_")
        # Only the partition that needs compaction should be backed up
        assert len(backup_info.partition_locations) == 1
        assert "year=2024/month=01" in backup_info.partition_locations

        # Verify ALTER TABLE ADD PARTITION was called
        calls = [str(c) for c in mock_spark.sql.call_args_list]
        alter_calls = [c for c in calls if "ADD PARTITION" in c]
        assert len(alter_calls) == 1

    def test_find_latest_backup_found(self, backup_mgr, mock_spark):
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240102_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
        ]

        desc_rows = [
            MagicMock(__getitem__=lambda self, i, vals=("Location", "hdfs:///data/mydb/events", None): vals[i]),
        ]

        mock_spark.sql.return_value.collect.side_effect = [
            table_rows,  # SHOW TABLES
            desc_rows,  # DESCRIBE FORMATTED backup table
            Exception("not partitioned"),  # SHOW PARTITIONS will fail
        ]

        result = backup_mgr.find_latest_backup("mydb", "events")
        assert result is not None
        assert result.backup_table == "mydb.__bkp_events_20240102_120000"

    def test_find_latest_backup_not_found(self, backup_mgr, mock_spark):
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
        ]
        mock_spark.sql.return_value.collect.return_value = table_rows

        result = backup_mgr.find_latest_backup("mydb", "events")
        assert result is None

    def test_list_backups(self, backup_mgr, mock_spark):
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240102_120000" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
        ]
        mock_spark.sql.return_value.collect.return_value = table_rows

        result = backup_mgr.list_backups("mydb", "events")
        assert len(result) == 2
        assert result[0] == "__bkp_events_20240102_120000"  # newest first

    def test_drop_backup(self, backup_mgr, mock_spark):
        backup_mgr.drop_backup("mydb", "__bkp_events_20240101_120000")

        mock_spark.sql.assert_called_with("DROP TABLE IF EXISTS mydb.__bkp_events_20240101_120000")
