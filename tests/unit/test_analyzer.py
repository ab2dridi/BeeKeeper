"""Tests for lakekeeper.core.analyzer module."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakekeeper.core.analyzer import TableAnalyzer
from lakekeeper.models import FileFormat
from lakekeeper.utils.hdfs import HdfsFileInfo


def _make_row(col0, col1="", col2=""):
    """Create a mock Row for DESCRIBE FORMATTED output."""
    row = MagicMock()
    row.__getitem__ = lambda self, i: [col0, col1, col2][i]
    return row


class TestTableAnalyzer:
    @pytest.fixture
    def analyzer(self, mock_spark, mock_hdfs_client, config):
        return TableAnalyzer(mock_spark, mock_hdfs_client, config)

    def test_analyze_non_partitioned_parquet(self, analyzer, mock_spark, mock_hdfs_client):
        desc_rows = [
            _make_row("col1", "string", ""),
            _make_row("", None, None),
            _make_row("# Detailed Table Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=65000,
            total_size_bytes=3 * 1024 * 1024 * 1024,
        )

        result = analyzer.analyze_table("mydb", "events")
        assert result.database == "mydb"
        assert result.table_name == "events"
        assert result.file_format == FileFormat.PARQUET
        assert result.is_partitioned is False
        assert result.total_file_count == 65000
        assert result.needs_compaction is True

    def test_analyze_non_partitioned_orc(self, analyzer, mock_spark, mock_hdfs_client):
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/orc_table", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=100,
            total_size_bytes=50 * 1024 * 1024 * 1024,
        )

        result = analyzer.analyze_table("mydb", "orc_table")
        assert result.file_format == FileFormat.ORC
        assert result.needs_compaction is False  # large avg file size

    def test_analyze_partitioned_table(self, analyzer, mock_spark, mock_hdfs_client, config):
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("year", "string", ""),
            _make_row("# Another Section", None, None),
        ]

        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("year=2023",): v[i]),
        ]

        partition_desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/logs/year=2024", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]

        # First call: DESCRIBE FORMATTED, second: SHOW PARTITIONS, then per-partition DESCRIBE
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,
            partition_desc_rows,
            [
                _make_row("Location", "hdfs:///data/mydb/logs/year=2023", None),
                _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            ],
        ]

        small_partition = HdfsFileInfo(file_count=5000, total_size_bytes=500 * 1024 * 1024)
        large_partition = HdfsFileInfo(file_count=2, total_size_bytes=256 * 1024 * 1024)
        mock_hdfs_client.get_file_info.side_effect = [small_partition, large_partition]

        result = analyzer.analyze_table("mydb", "logs")
        assert result.is_partitioned is True
        assert result.partition_columns == ["year"]
        assert len(result.partitions) == 2
        assert result.partitions[0].needs_compaction is True
        assert result.partitions[1].needs_compaction is False
        assert result.needs_compaction is True

    def test_analyze_missing_location(self, analyzer, mock_spark):
        desc_rows = [
            _make_row("InputFormat", "parquet", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        with pytest.raises(ValueError, match="Could not determine table location"):
            analyzer.analyze_table("mydb", "bad_table")

    def test_analyze_unknown_format(self, analyzer, mock_spark):
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/t", None),
            _make_row("InputFormat", "com.unknown.Format", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        with pytest.raises(ValueError, match="Could not detect file format"):
            analyzer.analyze_table("mydb", "t")

    def test_analyze_managed_table_raises(self, analyzer, mock_spark):
        """MANAGED tables must be rejected — only EXTERNAL tables are supported."""
        from lakekeeper.models import SkipTableError

        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/managed", None),
            _make_row("Table Type", "MANAGED_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows

        with pytest.raises(SkipTableError, match="not an external table"):
            analyzer.analyze_table("mydb", "managed")

    def test_analyze_external_table_not_skipped(self, analyzer, mock_spark, mock_hdfs_client):
        """EXTERNAL_TABLE must not be skipped."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/ext", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "ext")
        assert result.database == "mydb"

    def test_analyze_unknown_type_not_skipped(self, analyzer, mock_spark, mock_hdfs_client):
        """When Table Type is absent, do not skip (be permissive about unknown clusters)."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/ext", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.return_value = desc_rows
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(file_count=10, total_size_bytes=1024)

        result = analyzer.analyze_table("mydb", "ext")
        assert result.database == "mydb"


    def test_two_level_partition_detected_from_describe(self, analyzer, mock_spark, mock_hdfs_client):
        """Two-level partition (date+ref) detected correctly from DESCRIBE FORMATTED."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_2p", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("date", "string", ""),
            _make_row("ref", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01/ref=A",): v[i]),
        ]
        partition_desc = [
            _make_row("Location", "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=A", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,   # SHOW PARTITIONS
            partition_desc,   # DESCRIBE FORMATTED ... PARTITION(...)
        ]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=100, total_size_bytes=100 * 1024 * 1024
        )
        result = analyzer.analyze_table("mydb", "events_2p")
        assert result.is_partitioned is True
        assert result.partition_columns == ["date", "ref"]
        assert len(result.partitions) == 1

    def test_partition_detection_fallback_via_show_partitions(self, analyzer, mock_spark, mock_hdfs_client):
        """When DESCRIBE FORMATTED omits partition info, SHOW PARTITIONS is used as fallback."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_2p", None),
            _make_row("Table Type", "EXTERNAL_TABLE", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            # Deliberately NO "# Partition Information" section
        ]
        partition_rows = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01/ref=A",): v[i]),
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01/ref=B",): v[i]),
        ]
        partition_desc_a = [
            _make_row("Location", "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=A", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        partition_desc_b = [
            _make_row("Location", "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=B", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
        ]
        # Call sequence:
        # 1. DESCRIBE FORMATTED           (main)
        # 2. SHOW PARTITIONS              (fallback detection)
        # 3. SHOW PARTITIONS              (_analyze_partitions)
        # 4. DESCRIBE FORMATTED PARTITION (ref=A location)
        # 5. DESCRIBE FORMATTED PARTITION (ref=B location)
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_rows,   # fallback
            partition_rows,   # _analyze_partitions
            partition_desc_a,
            partition_desc_b,
        ]
        mock_hdfs_client.get_file_info.side_effect = [
            HdfsFileInfo(file_count=50, total_size_bytes=50 * 1024 * 1024),
            HdfsFileInfo(file_count=50, total_size_bytes=50 * 1024 * 1024),
        ]
        result = analyzer.analyze_table("mydb", "events_2p")
        assert result.is_partitioned is True
        assert result.partition_columns == ["date", "ref"]
        assert len(result.partitions) == 2

    def test_partition_fallback_non_partitioned_table(self, analyzer, mock_spark, mock_hdfs_client):
        """Non-partitioned table stays non-partitioned when SHOW PARTITIONS raises."""
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/flat", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            # No partition section
        ]
        # DESCRIBE FORMATTED returns desc_rows, then SHOW PARTITIONS raises (non-partitioned)
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            Exception("SHOW PARTITIONS is not allowed on a table that is not partitioned"),
        ]
        # Actually, side_effect on collect; but the sql() call itself raises — need to mock differently
        desc_result = MagicMock()
        desc_result.collect.return_value = desc_rows
        show_partitions_result = MagicMock()
        show_partitions_result.collect.side_effect = Exception(
            "SHOW PARTITIONS is not allowed on a table that is not partitioned"
        )
        mock_spark.sql.side_effect = [desc_result, show_partitions_result]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=10, total_size_bytes=10 * 1024 * 1024
        )
        result = analyzer.analyze_table("mydb", "flat")
        assert result.is_partitioned is False

    def test_detect_partition_columns_fallback_extracts_columns(self, analyzer, mock_spark):
        """_detect_partition_columns_fallback correctly parses multi-level spec."""
        rows = [
            MagicMock(__getitem__=lambda self, i, v=("year=2024/month=01/day=15",): v[i]),
        ]
        mock_spark.sql.return_value.collect.return_value = rows
        result = analyzer._detect_partition_columns_fallback("mydb.t")
        assert result == ["year", "month", "day"]


    def test_get_partition_location_takes_first_occurrence(self, analyzer, mock_spark, mock_hdfs_client):
        """On Hive 3 / CDP, DESCRIBE FORMATTED PARTITION emits Location twice.
        The first occurrence is the partition path; the second is the table root.
        We must take the first one or all compaction renames target the table root.
        """
        desc_rows = [
            _make_row("Location", "hdfs:///data/mydb/events_2p", None),
            _make_row("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat", None),
            _make_row("# Partition Information", None, None),
            _make_row("# col_name", "data_type", "comment"),
            _make_row("date", "string", ""),
            _make_row("ref", "string", ""),
            _make_row("# Another Section", None, None),
        ]
        # DESCRIBE FORMATTED PARTITION output with two Location entries:
        # 1st = partition path (correct), 2nd = table root (wrong)
        partition_desc_rows = [
            _make_row("# Detailed Partition Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=A", None),
            _make_row("# Detailed Table Information", None, None),
            _make_row("Location", "hdfs:///data/mydb/events_2p", None),  # table root — must NOT win
        ]
        partition_list = [
            MagicMock(__getitem__=lambda self, i, v=("date=2024-01-01/ref=A",): v[i]),
        ]
        mock_spark.sql.return_value.collect.side_effect = [
            desc_rows,
            partition_list,          # SHOW PARTITIONS
            partition_desc_rows,     # DESCRIBE FORMATTED ... PARTITION(...)
        ]
        mock_hdfs_client.get_file_info.return_value = HdfsFileInfo(
            file_count=900, total_size_bytes=50 * 1024 * 1024
        )
        result = analyzer.analyze_table("mydb", "events_2p")
        assert result.is_partitioned is True
        assert len(result.partitions) == 1
        # partition.location must be the partition path, NOT the table root
        assert result.partitions[0].location == "hdfs:///data/mydb/events_2p/date=2024-01-01/ref=A"


class TestParsePartitionSpec:
    def test_single_partition(self):
        result = TableAnalyzer._parse_partition_spec("year=2024")
        assert result == {"year": "2024"}

    def test_multiple_partitions(self):
        result = TableAnalyzer._parse_partition_spec("year=2024/month=01")
        assert result == {"year": "2024", "month": "01"}

    def test_empty_value(self):
        result = TableAnalyzer._parse_partition_spec("key=")
        assert result == {"key": ""}
