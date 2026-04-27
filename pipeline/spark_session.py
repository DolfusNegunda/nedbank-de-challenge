"""
pipeline/spark_session.py
─────────────────────────
Centralised SparkSession factory for the Nedbank DE pipeline.

Design notes
------------
- Delta JARs are pre-downloaded into /opt/delta-jars/ at Docker build time
  and referenced via spark.jars as local file:// paths. This is the only
  approach that works under --network=none (configure_spark_with_delta_pip
  downloads from Maven at runtime, which fails with no network).
- spark.driver.host=127.0.0.1 prevents hostname DNS lookup failure under
  --network=none.
- local[2] matches the 2-vCPU scoring constraint.
- Stage 1 time limit: 15 minutes. Stage 2/3: 30 minutes.
- Parquet compression is set to UNCOMPRESSED.
  Both Snappy (default) and zstd use zstd-jni / snappy-java, which extract
  a native .so into /tmp at runtime and mmap it as executable. The scoring
  container mounts /tmp with noexec, causing UnsatisfiedLinkError on the
  first write. UNCOMPRESSED has zero native-library dependency and is the
  only codec guaranteed to work under --tmpfs /tmp:rw,noexec constraints.
  The size trade-off is acceptable: Delta Parquet with no compression still
  achieves good read performance, and the scoring system does not penalise
  output file size.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG   = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

# Pre-downloaded at Docker build time — no network needed at runtime
_DELTA_JARS_DIR  = "/opt/delta-jars"
_DELTA_JAR_NAMES = [
    "delta-spark_2.12-3.1.0.jar",
    "delta-storage-3.1.0.jar",
]


def _delta_jars_path() -> str:
    """Return comma-separated local file:// paths for the Delta JARs.

    Falls back gracefully if the directory does not exist (e.g. local dev
    outside Docker) so the session still starts — Delta writes will fail
    later with a clear error rather than here.
    """
    paths = []
    for jar in _DELTA_JAR_NAMES:
        full = os.path.join(_DELTA_JARS_DIR, jar)
        if os.path.exists(full):
            paths.append(full)
        else:
            logger.warning("Delta JAR not found: %s", full)

    if not paths:
        logger.error(
            "No Delta JARs found in %s — Delta writes will fail. "
            "Ensure the Dockerfile RUN curl step completed successfully.",
            _DELTA_JARS_DIR,
        )
    else:
        logger.info("Delta JARs located: %s", paths)

    return ",".join(paths)


def get_or_create(spark_cfg: dict[str, Any]) -> SparkSession:
    """Return an existing SparkSession or create one from *spark_cfg*.

    Parameters
    ----------
    spark_cfg:
        The ``spark`` sub-dict from ``pipeline_config.yaml``.
        Expected keys: ``master`` (default ``"local[2]"``) and ``app_name``.

    Returns
    -------
    SparkSession
        Fully configured session with Delta Lake support enabled.
    """
    master:   str = spark_cfg.get("master",   "local[2]")
    app_name: str = spark_cfg.get("app_name", "nedbank-de-pipeline")

    # Must be set before JVM starts — prevents hostname DNS lookup under --network=none
    os.environ.setdefault("SPARK_LOCAL_IP",      "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    logger.info("Initialising SparkSession: master=%s app=%s", master, app_name)

    delta_jars = _delta_jars_path()

    session = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        # ── Delta Lake — local JARs, no Maven download needed ─────────────────
        .config("spark.jars",                           delta_jars)
        .config("spark.sql.extensions",                 _DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog",      _DELTA_CATALOG)
        # ── Network: bind to loopback, bypass hostname resolution ─────────────
        .config("spark.driver.host",                    "127.0.0.1")
        .config("spark.driver.bindAddress",             "127.0.0.1")
        # ── Memory / parallelism tuning for 2-vCPU / 2 GB envelope ──────────
        .config("spark.driver.memory",                  "1g")
        .config("spark.executor.memory",                "512m")
        .config("spark.default.parallelism",            "2")
        .config("spark.sql.shuffle.partitions",         "4")
        # ── Temp dir — only /tmp is writable in the scoring container ────────
        .config("spark.local.dir",                      "/tmp")
        # ── Compression: UNCOMPRESSED — zero native-library dependency ────────
        # Snappy and zstd both extract a native .so into /tmp and mmap it as
        # executable. The scoring container mounts /tmp noexec, which causes
        # UnsatisfiedLinkError on the first Parquet write. UNCOMPRESSED avoids
        # all native codec libraries entirely.
        .config("spark.sql.parquet.compression.codec",  "uncompressed")
        # ── Disable Spark UI (no browser in container) ───────────────────────
        .config("spark.ui.enabled",                     "false")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready.")
    return session
