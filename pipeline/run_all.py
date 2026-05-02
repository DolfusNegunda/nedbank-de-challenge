"""
pipeline/run_all.py
───────────────────
Pipeline entry point — orchestrates Bronze -> Silver -> Gold in sequence.

Runtime strategy
----------------
Phase 1 — Spark ON:
    Bronze ingestion + Silver customers/accounts

Phase 2 — DuckDB:
    Silver transactions (3M+ rows) processed outside Spark to avoid /tmp spill

Phase 3 — Spark ON:
    Register Silver transactions as Delta + Gold provisioning + dq_report.json
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import time
import traceback

_APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from pipeline.config_loader import load as load_config
from pipeline.spark_session import get_or_create as get_spark
from pipeline.ingest import run_ingestion
from pipeline.transform import (
    run_silver_small_tables,
    run_silver_transactions_duckdb,
    run_silver_transactions_delta_register,
)
from pipeline.provision import run_provisioning

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _clean_tmp() -> None:
    """Delete Spark temporary directories from /tmp between phases."""
    tmp = "/tmp"
    removed = 0

    for entry in os.listdir(tmp):
        if entry.startswith(("blockmgr-", "spark-", "hsperfdata_")):
            full_path = os.path.join(tmp, entry)
            try:
                if os.path.isdir(full_path):
                    shutil.rmtree(full_path, ignore_errors=True)
                else:
                    os.remove(full_path)
                removed += 1
            except Exception as exc:
                logger.warning("Could not remove %s: %s", full_path, exc)

    logger.info("Cleaned %d Spark temp entries from /tmp.", removed)


def main() -> None:
    """Run the complete medallion pipeline end-to-end."""
    pipeline_start = time.time()

    logger.info("=== Nedbank DE Pipeline starting ===")
    logger.info("Stage 2 time limit: 30 minutes")

    cfg = load_config()
    cfg["stage"] = "2"

    # ── Phase 1: Spark — Bronze + Silver small tables ─────────────────────────
    logger.info("--- Phase 1: Bronze ingestion + Silver customers/accounts ---")
    spark = get_spark(cfg.get("spark", {}))
    try:
        source_counts = run_ingestion(spark, cfg)
        if source_counts is None:
            raise RuntimeError(
                "run_ingestion() returned None — expected raw source counts dict."
            )

        small_table_dq_counts = run_silver_small_tables(spark, cfg)
    finally:
        spark.stop()
        logger.info("Spark stopped.")

    _clean_tmp()

    # ── Phase 2: DuckDB — Silver transactions ─────────────────────────────────
    logger.info("--- Phase 2: Silver transactions via DuckDB ---")
    transaction_dq_counts = run_silver_transactions_duckdb(cfg)

    dq_counts = {
        **small_table_dq_counts,
        **transaction_dq_counts,
    }

    # ── Phase 3: Spark — Delta registration + Gold provisioning ───────────────
    logger.info("--- Phase 3: Delta registration + Gold provisioning ---")
    spark = get_spark(cfg.get("spark", {}))
    try:
        run_silver_transactions_delta_register(spark, cfg)

        duration_seconds = int(time.time() - pipeline_start)

        run_provisioning(
            spark=spark,
            cfg=cfg,
            source_counts=source_counts,
            dq_counts=dq_counts,
            execution_duration_seconds=duration_seconds,
        )
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")

    logger.info("=== Pipeline completed successfully ===")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception:
        logger.critical(
            "Pipeline failed with unhandled exception:\n%s",
            traceback.format_exc(),
        )
        sys.exit(1)