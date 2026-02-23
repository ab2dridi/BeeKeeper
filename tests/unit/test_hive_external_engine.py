"""Tests for beekeeper.engine.hive_external module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from beekeeper.engine.hive_external import HiveExternalEngine
from beekeeper.models import BackupInfo, CompactionReport, CompactionStatus, FileFormat, TableInfo


class TestHiveExternalEngine:
    @pytest.fixture
    def engine(self, mock_spark, config):
        with (
            patch("beekeeper.engine.hive_external.HdfsClient") as mock_hdfs_cls,
            patch("beekeeper.engine.hive_external.TableAnalyzer") as mock_analyzer_cls,
            patch("beekeeper.engine.hive_external.BackupManager") as mock_backup_cls,
            patch("beekeeper.engine.hive_external.Compactor") as mock_compactor_cls,
        ):
            engine = HiveExternalEngine(mock_spark, config)
            engine._mock_analyzer = mock_analyzer_cls.return_value
            engine._mock_backup_mgr = mock_backup_cls.return_value
            engine._mock_compactor = mock_compactor_cls.return_value
            engine._mock_hdfs = mock_hdfs_cls.return_value
            return engine

    def test_analyze(self, engine):
        expected = TableInfo(
            database="mydb",
            table_name="tbl",
            location="hdfs:///data",
            file_format=FileFormat.PARQUET,
        )
        engine._mock_analyzer.analyze_table.return_value = expected

        result = engine.analyze("mydb", "tbl")
        assert result == expected
        engine._mock_analyzer.analyze_table.assert_called_with("mydb", "tbl")

    def test_create_backup(self, engine, sample_table_info):
        expected = BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime.now(),
        )
        engine._mock_backup_mgr.create_backup.return_value = expected

        result = engine.create_backup(sample_table_info)
        assert result == expected

    def test_compact(self, engine, sample_table_info):
        backup_info = BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime.now(),
        )
        expected = CompactionReport(
            table_name="mydb.events",
            status=CompactionStatus.COMPLETED,
        )
        engine._mock_compactor.compact_table.return_value = expected

        result = engine.compact(sample_table_info, backup_info)
        assert result.status == CompactionStatus.COMPLETED

    def test_rollback_found(self, engine, mock_spark):
        backup_info = BackupInfo(
            original_table="mydb.events",
            backup_table="mydb.__bkp_events_20240101_120000",
            original_location="hdfs:///data/mydb/events",
            timestamp=datetime.now(),
        )
        engine._mock_backup_mgr.find_latest_backup.return_value = backup_info

        engine.rollback("mydb", "events")

        mock_spark.sql.assert_any_call("ALTER TABLE mydb.events SET LOCATION 'hdfs:///data/mydb/events'")

    def test_rollback_not_found(self, engine):
        engine._mock_backup_mgr.find_latest_backup.return_value = None

        with pytest.raises(ValueError, match="No backup found"):
            engine.rollback("mydb", "events")

    def test_rollback_partitioned(self, engine, mock_spark):
        backup_info = BackupInfo(
            original_table="mydb.logs",
            backup_table="mydb.__bkp_logs_20240101_120000",
            original_location="hdfs:///data/mydb/logs",
            timestamp=datetime.now(),
            partition_locations={
                "year=2024/month=01": "hdfs:///data/mydb/logs/year=2024/month=01",
            },
        )
        engine._mock_backup_mgr.find_latest_backup.return_value = backup_info

        engine.rollback("mydb", "logs")

        mock_spark.sql.assert_any_call(
            "ALTER TABLE mydb.logs PARTITION(year='2024', month='01') "
            "SET LOCATION 'hdfs:///data/mydb/logs/year=2024/month=01'"
        )

    def test_cleanup(self, engine):
        engine._mock_backup_mgr.list_backups.return_value = [
            "__bkp_events_20240101_120000",
            "__bkp_events_20240102_120000",
        ]

        cleaned = engine.cleanup("mydb", "events")
        assert cleaned == 2

    def test_cleanup_with_age_filter(self, engine, config):
        engine._mock_backup_mgr.list_backups.return_value = [
            "__bkp_events_20200101_120000",  # old
        ]

        cleaned = engine.cleanup("mydb", "events", older_than_days=7)
        assert cleaned == 1

    def test_list_tables(self, engine, mock_spark):
        table_rows = [
            MagicMock(**{"__getitem__": lambda s, k: "events" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "__bkp_events_20240101" if k == "tableName" else None}),
            MagicMock(**{"__getitem__": lambda s, k: "users" if k == "tableName" else None}),
        ]

        def _make_row(col0, col1="", col2=""):
            row = MagicMock()
            row.__getitem__ = lambda self, i: [col0, col1, col2][i]
            return row

        desc_external = [_make_row("Table Type", "EXTERNAL_TABLE")]
        desc_managed = [_make_row("Table Type", "MANAGED_TABLE")]

        # SHOW TABLES returns all, then DESCRIBE each non-backup
        mock_spark.sql.return_value.collect.side_effect = [
            table_rows,
            desc_external,  # events
            desc_managed,  # users (not external)
        ]

        result = engine.list_tables("mydb")
        assert "events" in result
        assert "users" not in result
        assert "__bkp_events_20240101" not in result
