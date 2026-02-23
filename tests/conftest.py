"""Shared fixtures for Beekeeper tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from beekeeper.config import BeekeeperConfig
from beekeeper.models import FileFormat, PartitionInfo, TableInfo


@pytest.fixture
def config():
    """Default Beekeeper configuration for tests."""
    return BeekeeperConfig(
        block_size_mb=128,
        compaction_ratio_threshold=10.0,
        backup_prefix="__bkp",
        dry_run=False,
        log_level="WARNING",
    )


@pytest.fixture
def mock_spark():
    """Mock SparkSession with Hive and JVM support."""
    spark = MagicMock()
    spark._jvm = MagicMock()
    spark._jsc = MagicMock()

    # Mock SparkContext
    spark.sparkContext = MagicMock()
    spark.sparkContext.applicationId = "local-test-app"

    # Mock SQL execution
    spark.sql.return_value = MagicMock()
    spark.sql.return_value.collect.return_value = []

    # Mock read
    spark.read = MagicMock()
    spark.read.format.return_value = spark.read
    spark.read.load.return_value = MagicMock()
    spark.read.load.return_value.count.return_value = 1000
    spark.read.load.return_value.coalesce.return_value = MagicMock()

    return spark


@pytest.fixture
def mock_hdfs_client():
    """Mock HDFS client."""
    from beekeeper.utils.hdfs import HdfsClient, HdfsFileInfo

    client = MagicMock(spec=HdfsClient)
    client.get_file_info.return_value = HdfsFileInfo(
        file_count=65000,
        total_size_bytes=3 * 1024 * 1024 * 1024,  # 3 GB
    )
    # Default: paths do not exist (safe to use as staging areas)
    client.path_exists.return_value = False
    client.delete_path.return_value = True
    client.mkdirs.return_value = True
    client.rename_path.return_value = True
    return client


@pytest.fixture
def mock_backup_mgr():
    """Mock BackupManager."""
    from beekeeper.core.backup import BackupManager

    mgr = MagicMock(spec=BackupManager)
    return mgr


@pytest.fixture
def sample_table_info():
    """Non-partitioned table with many small files."""
    return TableInfo(
        database="mydb",
        table_name="events",
        location="hdfs:///data/mydb/events",
        file_format=FileFormat.PARQUET,
        is_partitioned=False,
        total_file_count=65000,
        total_size_bytes=3 * 1024 * 1024 * 1024,
        needs_compaction=True,
    )


@pytest.fixture
def sample_partitioned_table_info():
    """Partitioned table with some partitions needing compaction."""
    return TableInfo(
        database="mydb",
        table_name="logs",
        location="hdfs:///data/mydb/logs",
        file_format=FileFormat.ORC,
        is_partitioned=True,
        partition_columns=["year", "month"],
        partitions=[
            PartitionInfo(
                spec={"year": "2024", "month": "01"},
                location="hdfs:///data/mydb/logs/year=2024/month=01",
                file_count=10000,
                total_size_bytes=1 * 1024 * 1024 * 1024,
                needs_compaction=True,
                target_files=8,
            ),
            PartitionInfo(
                spec={"year": "2024", "month": "02"},
                location="hdfs:///data/mydb/logs/year=2024/month=02",
                file_count=5,
                total_size_bytes=500 * 1024 * 1024,
                needs_compaction=False,
                target_files=4,
            ),
        ],
        total_file_count=10005,
        total_size_bytes=int(1.5 * 1024 * 1024 * 1024),
        needs_compaction=True,
    )


def make_describe_formatted_rows(
    location="hdfs:///data/mydb/events",
    input_format="org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat",
    table_type="EXTERNAL_TABLE",
    partitioned=False,
    partition_cols=None,
):
    """Create mock rows for DESCRIBE FORMATTED output."""
    rows = [
        MagicMock(__getitem__=lambda self, i, vals=("col1", "string", ""): vals[i]),
        MagicMock(__getitem__=lambda self, i, vals=("col2", "int", ""): vals[i]),
        MagicMock(__getitem__=lambda self, i, vals=("", None, None): vals[i]),
        MagicMock(__getitem__=lambda self, i, vals=("# Detailed Table Information", None, None): vals[i]),
        MagicMock(__getitem__=lambda self, i, vals=("Location", location, None): vals[i]),
        MagicMock(__getitem__=lambda self, i, vals=("InputFormat", input_format, None): vals[i]),
        MagicMock(__getitem__=lambda self, i, vals=("Table Type", table_type, None): vals[i]),
    ]

    if partitioned and partition_cols:
        rows.append(MagicMock(__getitem__=lambda self, i, vals=("# Partition Information", None, None): vals[i]))
        rows.append(MagicMock(__getitem__=lambda self, i, vals=("# col_name", "data_type", "comment"): vals[i]))
        for col in partition_cols:
            vals = (col, "string", "")
            rows.append(MagicMock(__getitem__=lambda self, i, vals=vals: vals[i]))
        rows.append(MagicMock(__getitem__=lambda self, i, vals=("# Another Section", None, None): vals[i]))

    return rows
