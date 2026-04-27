"""
pipeline/stream_ingest.py
─────────────────────────
Stage 3 — Streaming extension: process micro-batch JSONL files from /data/stream/.

Stage 1 / Stage 2: this module is a required stub (challenge_rules.md §3.6).
It is not invoked by run_all.py until Stage 3.

Stage 3 contract
----------------
Input:
    /data/stream/   — micro-batch JSONL files, pre-staged at container start.
                      Naming: stream_YYYYMMDD_HHMMSS_NNNN.jsonl
                      Lexicographic sort = chronological order.

Output (Delta Parquet):
    /data/output/stream_gold/current_balances/      — 4 fields, upsert
    /data/output/stream_gold/recent_transactions/   — 7 fields, last-50/account

SLA: updated_at within 300 s of source event timestamp for full credit.

See stream_interface_spec.md and output_schema_spec.md §5-6 for full spec.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def run_stream_ingestion(spark: SparkSession, cfg: dict[str, Any]) -> None:
    """Poll /data/stream/ and process micro-batch JSONL files into stream_gold tables.

    Parameters
    ----------
    spark:
        Active SparkSession with Delta Lake extensions configured.
    cfg:
        Parsed pipeline_config.yaml as a plain dict.

    Notes
    -----
    Stage 1 / Stage 2: no-op stub.
    Stage 3: implement directory polling, Delta MERGE upsert for
    current_balances, and last-50 retention for recent_transactions.
    """
    logger.debug("run_stream_ingestion called (stub — no-op at Stage 1/2).")
