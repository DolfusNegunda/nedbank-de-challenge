"""
pipeline/run_all.py
───────────────────
Pipeline entry point — orchestrates Bronze -> Silver -> Gold in sequence.

Invocation (by the scoring system):
    docker run ... python pipeline/run_all.py

Design notes
------------
- A single SparkSession is created here and passed to each stage.
- sys.path is patched at startup so that "from pipeline.X import ..." works
  when this file is invoked as "python pipeline/run_all.py" from /app.
  (Python adds /app/pipeline to sys.path in that case, not /app.)
- Stage 1 time limit: 15 minutes.  Stage 2/3: 30 minutes.
- sys.exit(0) is called explicitly on success.
"""

from __future__ import annotations

import logging
import os
import sys
import traceback

# ── PYTHONPATH fix ────────────────────────────────────────────────────────────
# When invoked as "python pipeline/run_all.py" from /app, Python inserts
# /app/pipeline into sys.path[0], not /app.  The package imports below
# require /app to be on the path.  We insert it explicitly if absent.
_APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from pipeline.config_loader import load as load_config
from pipeline.spark_session import get_or_create as get_spark
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Run the complete medallion pipeline end-to-end."""
    logger.info("=== Nedbank DE Pipeline starting ===")
    logger.info("Stage 1 time limit: 15 minutes")

    cfg   = load_config()
    spark = get_spark(cfg.get("spark", {}))

    try:
        logger.info("--- Stage 1/3: Bronze ingestion ---")
        run_ingestion(spark, cfg)

        logger.info("--- Stage 2/3: Silver transformation ---")
        run_transformation(spark, cfg)

        logger.info("--- Stage 3/3: Gold provisioning ---")
        run_provisioning(spark, cfg)

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
