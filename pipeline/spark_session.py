"""
pipeline/spark_session.py
─────────────────────────
Centralised SparkSession factory for the Nedbank DE pipeline.

Silver transactions are processed by DuckDB, so Spark now handles:
- Bronze ingestion
- Silver customers/accounts
- Delta registration
- Gold provisioning

These Spark workloads are small enough to fit comfortably under the
2 GB RAM / 512 MB /tmp scoring envelope.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

_DELTA_JARS_DIR = "/opt/delta-jars"
_DELTA_JAR_NAMES = [
    "delta-spark_2.12-3.1.0.jar",
    "delta-storage-3.1.0.jar",
]


def _delta_jars_path() -> str:
    """Return comma-separated local Delta JAR paths."""
    paths: list[str] = []

    for jar in _DELTA_JAR_NAMES:
        full = os.path.join(_DELTA_JARS_DIR, jar)
        if os.path.exists(full):
            paths.append(full)
        else:
            logger.warning("Delta JAR not found: %s", full)

    if not paths:
        logger.error("No Delta JARs found in %s — Delta writes will fail.", _DELTA_JARS_DIR)
    else:
        logger.info("Delta JARs located: %s", paths)

    return ",".join(paths)


def get_or_create(spark_cfg: dict[str, Any]) -> SparkSession:
    """Return an existing SparkSession or create one from *spark_cfg*."""
    master: str = spark_cfg.get("master", "local[2]")
    app_name: str = spark_cfg.get("app_name", "nedbank-de-pipeline")

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    logger.info("Initialising SparkSession: master=%s app=%s", master, app_name)

    delta_jars = _delta_jars_path()

    session = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.jars", delta_jars)
        .config("spark.sql.extensions", _DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", _DELTA_CATALOG)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "512m")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.local.dir", "/tmp")
        .config("spark.shuffle.compress", "false")
        .config("spark.shuffle.spill.compress", "false")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready.")
    return session