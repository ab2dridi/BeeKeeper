"""SparkSession helper utilities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_or_create_spark_session(
    app_name: str = "beekeeper",
    master: str | None = None,
    enable_hive: bool = True,
) -> SparkSession:
    """Get or create a SparkSession configured for Hive access.

    Args:
        app_name: Spark application name.
        master: Spark master URL. If None, uses existing config.
        enable_hive: Whether to enable Hive support.

    Returns:
        Configured SparkSession.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    logger.info("SparkSession initialized: %s", spark.sparkContext.applicationId)
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Stop a SparkSession gracefully.

    Args:
        spark: SparkSession to stop.
    """
    try:
        spark.stop()
        logger.info("SparkSession stopped.")
    except Exception:
        logger.exception("Error stopping SparkSession")
