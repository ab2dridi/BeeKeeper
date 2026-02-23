"""Tests for beekeeper.utils.spark module."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

from beekeeper.utils.spark import stop_spark_session


class TestGetOrCreateSparkSession:
    def _setup_mock_pyspark(self):
        """Create a mock pyspark module and register it in sys.modules."""
        mock_spark_session = MagicMock()
        mock_pyspark = MagicMock()
        mock_pyspark.sql.SparkSession = mock_spark_session
        return mock_pyspark, mock_spark_session

    def test_default_session(self):
        mock_pyspark, mock_spark_cls = self._setup_mock_pyspark()
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_session = MagicMock()
        mock_session.sparkContext.applicationId = "test-app"
        mock_builder.getOrCreate.return_value = mock_session

        with patch.dict(sys.modules, {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark.sql}):
            from beekeeper.utils.spark import get_or_create_spark_session

            result = get_or_create_spark_session()

        mock_builder.appName.assert_called_with("beekeeper")
        mock_builder.enableHiveSupport.assert_called_once()
        assert result == mock_session

    def test_custom_app_name_and_master(self):
        mock_pyspark, mock_spark_cls = self._setup_mock_pyspark()
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_session = MagicMock()
        mock_session.sparkContext.applicationId = "test-app"
        mock_builder.getOrCreate.return_value = mock_session

        with patch.dict(sys.modules, {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark.sql}):
            from beekeeper.utils.spark import get_or_create_spark_session

            get_or_create_spark_session(app_name="my_app", master="local[4]")

        mock_builder.appName.assert_called_with("my_app")
        mock_builder.master.assert_called_with("local[4]")

    def test_without_hive(self):
        mock_pyspark, mock_spark_cls = self._setup_mock_pyspark()
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_session = MagicMock()
        mock_session.sparkContext.applicationId = "test-app"
        mock_builder.getOrCreate.return_value = mock_session

        with patch.dict(sys.modules, {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark.sql}):
            from beekeeper.utils.spark import get_or_create_spark_session

            get_or_create_spark_session(enable_hive=False)

        mock_builder.enableHiveSupport.assert_not_called()


class TestStopSparkSession:
    def test_stop_success(self):
        mock_spark = MagicMock()
        stop_spark_session(mock_spark)
        mock_spark.stop.assert_called_once()

    def test_stop_with_exception(self):
        mock_spark = MagicMock()
        mock_spark.stop.side_effect = RuntimeError("stop failed")
        # Should not raise
        stop_spark_session(mock_spark)
