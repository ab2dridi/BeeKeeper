"""Tests for beekeeper.utils.hdfs module."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from beekeeper.utils.hdfs import HdfsClient, HdfsFileInfo


class TestHdfsFileInfo:
    def test_avg_file_size(self):
        info = HdfsFileInfo(file_count=100, total_size_bytes=1000000)
        assert info.avg_file_size_bytes == 10000

    def test_avg_file_size_zero_files(self):
        info = HdfsFileInfo(file_count=0, total_size_bytes=0)
        assert info.avg_file_size_bytes == 0


class TestHdfsClient:
    @pytest.fixture
    def hdfs_client(self, mock_spark):
        return HdfsClient(mock_spark)

    def test_get_file_info(self, hdfs_client, mock_spark):
        # Create mock file statuses
        file1 = MagicMock()
        file1.getPath.return_value.getName.return_value = "part-00000.parquet"
        file1.getLen.return_value = 1000

        file2 = MagicMock()
        file2.getPath.return_value.getName.return_value = "part-00001.parquet"
        file2.getLen.return_value = 2000

        hidden_file = MagicMock()
        hidden_file.getPath.return_value.getName.return_value = "_SUCCESS"
        hidden_file.getLen.return_value = 0

        # Mock iterator
        iterator = MagicMock()
        has_next_values = [True, True, True, False]
        next_values = [file1, file2, hidden_file]
        iterator.hasNext.side_effect = has_next_values
        iterator.next.side_effect = next_values

        # Mock filesystem
        mock_fs = MagicMock()
        mock_fs.listFiles.return_value = iterator

        mock_path = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
        mock_path.getFileSystem.return_value = mock_fs

        result = hdfs_client.get_file_info("hdfs:///data/test")
        assert result.file_count == 2  # hidden file excluded
        assert result.total_size_bytes == 3000

    def test_get_file_info_skips_dot_files(self, hdfs_client, mock_spark):
        dot_file = MagicMock()
        dot_file.getPath.return_value.getName.return_value = ".metadata"
        dot_file.getLen.return_value = 100

        iterator = MagicMock()
        iterator.hasNext.side_effect = [True, False]
        iterator.next.side_effect = [dot_file]

        mock_fs = MagicMock()
        mock_fs.listFiles.return_value = iterator

        mock_path = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
        mock_path.getFileSystem.return_value = mock_fs

        result = hdfs_client.get_file_info("hdfs:///data/test")
        assert result.file_count == 0
        assert result.total_size_bytes == 0

    def test_path_exists(self, hdfs_client, mock_spark):
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True

        mock_path = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
        mock_path.getFileSystem.return_value = mock_fs

        assert hdfs_client.path_exists("hdfs:///data/test") is True

    def test_delete_path(self, hdfs_client, mock_spark):
        mock_fs = MagicMock()
        mock_fs.delete.return_value = True

        mock_path = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
        mock_path.getFileSystem.return_value = mock_fs

        assert hdfs_client.delete_path("hdfs:///data/test") is True
        mock_fs.delete.assert_called_once()

    def test_mkdirs(self, hdfs_client, mock_spark):
        mock_fs = MagicMock()
        mock_fs.mkdirs.return_value = True

        mock_path = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
        mock_path.getFileSystem.return_value = mock_fs

        assert hdfs_client.mkdirs("hdfs:///data/test/new") is True

    def test_rename_path(self, hdfs_client, mock_spark):
        mock_fs = MagicMock()
        mock_fs.rename.return_value = True

        mock_path = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
        mock_path.getFileSystem.return_value = mock_fs

        assert hdfs_client.rename_path("hdfs:///data/src", "hdfs:///data/dst") is True
        mock_fs.rename.assert_called_once()

    def test_rename_path_failure_raises(self, hdfs_client, mock_spark):
        mock_fs = MagicMock()
        mock_fs.rename.return_value = False  # HDFS signals failure

        mock_path = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = mock_path
        mock_path.getFileSystem.return_value = mock_fs

        with pytest.raises(RuntimeError, match="HDFS rename failed"):
            hdfs_client.rename_path("hdfs:///data/src", "hdfs:///data/dst")
