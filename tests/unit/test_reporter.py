"""Tests for beekeeper.core.reporter module."""

from __future__ import annotations

from beekeeper.core.reporter import format_bytes, print_analysis_report, print_compaction_report
from beekeeper.models import CompactionReport, CompactionStatus, FileFormat, TableInfo


class TestFormatBytes:
    def test_zero(self):
        assert format_bytes(0) == "0 B"

    def test_bytes(self):
        assert format_bytes(500) == "500.0 B"

    def test_kilobytes(self):
        assert format_bytes(1024) == "1.0 KB"

    def test_megabytes(self):
        assert format_bytes(1024 * 1024) == "1.0 MB"

    def test_gigabytes(self):
        assert format_bytes(3 * 1024 * 1024 * 1024) == "3.0 GB"

    def test_terabytes(self):
        assert format_bytes(2 * 1024 * 1024 * 1024 * 1024) == "2.0 TB"


class TestPrintAnalysisReport:
    def test_non_partitioned(self, sample_table_info):
        report = print_analysis_report(sample_table_info)
        assert "mydb.events" in report
        assert "65,000" in report
        assert "parquet" in report
        assert "Needs compaction: True" in report

    def test_partitioned(self, sample_partitioned_table_info):
        report = print_analysis_report(sample_partitioned_table_info)
        assert "mydb.logs" in report
        assert "Partitioned:     True" in report
        assert "1 need compaction" in report
        assert "year=2024/month=01" in report

    def test_no_compaction_needed(self):
        table = TableInfo(
            database="db",
            table_name="t",
            location="hdfs:///t",
            file_format=FileFormat.PARQUET,
            total_file_count=5,
            total_size_bytes=640 * 1024 * 1024,
            needs_compaction=False,
        )
        report = print_analysis_report(table)
        assert "Needs compaction: False" in report


class TestPrintCompactionReport:
    def test_completed_report(self):
        report = CompactionReport(
            table_name="db.tbl",
            status=CompactionStatus.COMPLETED,
            before_file_count=65000,
            after_file_count=24,
            before_size_bytes=3 * 1024 * 1024 * 1024,
            after_size_bytes=3 * 1024 * 1024 * 1024,
            before_avg_file_size=48000,
            after_avg_file_size=128 * 1024 * 1024,
            row_count_before=1000000,
            row_count_after=1000000,
            duration_seconds=120.5,
        )
        output = print_compaction_report(report)
        assert "completed" in output
        assert "65,000" in output
        assert "24" in output
        assert "120.5s" in output
        assert "Gain:" in output
        assert "65,000 -> 24" in output
        assert "-100.0%" in output
        assert "Avg size gain:" in output

    def test_gain_section_not_shown_on_failure(self):
        report = CompactionReport(
            table_name="db.tbl",
            status=CompactionStatus.FAILED,
            before_file_count=1000,
            after_file_count=0,
            error="Row count mismatch",
        )
        output = print_compaction_report(report)
        assert "Gain:" not in output

    def test_failed_report_with_error(self):
        report = CompactionReport(
            table_name="db.tbl",
            status=CompactionStatus.FAILED,
            error="Row count mismatch",
        )
        output = print_compaction_report(report)
        assert "failed" in output
        assert "Row count mismatch" in output

    def test_partitioned_report(self):
        report = CompactionReport(
            table_name="db.tbl",
            status=CompactionStatus.COMPLETED,
            partitions_compacted=5,
            partitions_skipped=10,
        )
        output = print_compaction_report(report)
        assert "Partitions compacted: 5" in output
        assert "Partitions skipped:   10" in output
