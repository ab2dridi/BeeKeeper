"""Tests for beekeeper.models module."""

from __future__ import annotations

from datetime import datetime

from beekeeper.models import (
    BackupInfo,
    CompactionReport,
    CompactionState,
    CompactionStatus,
    FileFormat,
    PartitionInfo,
    TableInfo,
)


class TestFileFormat:
    def test_values(self):
        assert FileFormat.PARQUET.value == "parquet"
        assert FileFormat.ORC.value == "orc"


class TestCompactionStatus:
    def test_values(self):
        assert CompactionStatus.PENDING.value == "pending"
        assert CompactionStatus.COMPLETED.value == "completed"
        assert CompactionStatus.FAILED.value == "failed"


class TestPartitionInfo:
    def test_avg_file_size_bytes(self):
        p = PartitionInfo(
            spec={"year": "2024"},
            location="hdfs:///data/t/year=2024",
            file_count=100,
            total_size_bytes=1000000,
        )
        assert p.avg_file_size_bytes == 10000

    def test_avg_file_size_bytes_zero_files(self):
        p = PartitionInfo(spec={}, location="", file_count=0, total_size_bytes=0)
        assert p.avg_file_size_bytes == 0

    def test_partition_spec_str(self):
        p = PartitionInfo(
            spec={"year": "2024", "month": "01"},
            location="",
        )
        assert p.partition_spec_str == "year=2024/month=01"

    def test_partition_sql_spec(self):
        p = PartitionInfo(
            spec={"year": "2024", "month": "01"},
            location="",
        )
        assert p.partition_sql_spec == "year='2024', month='01'"


class TestTableInfo:
    def test_full_name(self):
        t = TableInfo(database="db", table_name="tbl", location="", file_format=FileFormat.PARQUET)
        assert t.full_name == "db.tbl"

    def test_avg_file_size_bytes(self):
        t = TableInfo(
            database="db",
            table_name="tbl",
            location="",
            file_format=FileFormat.PARQUET,
            total_file_count=100,
            total_size_bytes=1000000,
        )
        assert t.avg_file_size_bytes == 10000

    def test_avg_file_size_bytes_zero_files(self):
        t = TableInfo(
            database="db",
            table_name="tbl",
            location="",
            file_format=FileFormat.PARQUET,
            total_file_count=0,
            total_size_bytes=0,
        )
        assert t.avg_file_size_bytes == 0

    def test_target_files(self):
        t = TableInfo(
            database="db",
            table_name="tbl",
            location="",
            file_format=FileFormat.PARQUET,
            total_size_bytes=3 * 128 * 1024 * 1024,  # 3 blocks
        )
        assert t.target_files == 3

    def test_target_files_zero_size(self):
        t = TableInfo(
            database="db",
            table_name="tbl",
            location="",
            file_format=FileFormat.PARQUET,
            total_size_bytes=0,
        )
        assert t.target_files == 1

    def test_defaults(self):
        t = TableInfo(database="db", table_name="tbl", location="", file_format=FileFormat.ORC)
        assert t.is_partitioned is False
        assert t.partition_columns == []
        assert t.partitions == []
        assert t.needs_compaction is False
        assert t.row_count is None


class TestBackupInfo:
    def test_creation(self):
        b = BackupInfo(
            original_table="db.tbl",
            backup_table="db.__bkp_tbl_20240101_120000",
            original_location="hdfs:///data/tbl",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )
        assert b.partition_locations == {}
        assert b.row_count is None


class TestCompactionState:
    def test_defaults(self):
        t = TableInfo(database="db", table_name="tbl", location="", file_format=FileFormat.PARQUET)
        state = CompactionState(table_info=t)
        assert state.status == CompactionStatus.PENDING
        assert state.backup_info is None
        assert state.new_locations == {}


class TestCompactionReport:
    def test_creation(self):
        r = CompactionReport(
            table_name="db.tbl",
            status=CompactionStatus.COMPLETED,
            before_file_count=65000,
            after_file_count=24,
            before_size_bytes=3 * 1024 * 1024 * 1024,
            after_size_bytes=3 * 1024 * 1024 * 1024,
        )
        assert r.partitions_compacted == 0
        assert r.error is None
        assert r.duration_seconds == 0.0
