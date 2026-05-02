"""
pipeline/stream_ingest.py
─────────────────────────
Stage 3 — process pre-staged micro-batch JSONL files from /data/stream/
and produce stream_gold Delta tables:

    /data/output/stream_gold/current_balances/
    /data/output/stream_gold/recent_transactions/

Implementation strategy
-----------------------
- Use DuckDB to process stream files efficiently in a constrained environment.
- Process files in lexicographic order (chronological order by contract).
- Maintain state in local Parquet files first, then re-register as Delta.
- Avoid reprocessing by tracking processed files in /tmp/processed_stream_files.txt.
- Clean up working directories after Delta registration so only scored outputs remain.
"""

from __future__ import annotations

import glob
import logging
import os
from typing import Any

import duckdb
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_PROCESSED_STATE_FILE = "/tmp/processed_stream_files.txt"


def run_stream_ingestion(spark: SparkSession, cfg: dict[str, Any]) -> None:
    """Process all pre-staged stream files and publish Delta stream_gold tables."""
    streaming_cfg = cfg.get("streaming")
    if not streaming_cfg:
        logger.info("No streaming config present — skipping Stage 3 stream ingestion.")
        return

    stream_input_path = streaming_cfg["stream_input_path"]
    stream_gold_path = streaming_cfg["stream_gold_path"]

    current_balances_work = os.path.join(stream_gold_path, "_work_current_balances")
    recent_transactions_work = os.path.join(stream_gold_path, "_work_recent_transactions")

    os.makedirs(current_balances_work, exist_ok=True)
    os.makedirs(recent_transactions_work, exist_ok=True)

    stream_files = sorted(glob.glob(os.path.join(stream_input_path, "*.jsonl")))
    if not stream_files:
        logger.info("No stream files found in %s", stream_input_path)
        return

    processed = _load_processed_files()
    files_to_process = [f for f in stream_files if os.path.basename(f) not in processed]

    logger.info("Stage 3 stream ingestion starting. Files to process: %d", len(files_to_process))

    if not files_to_process:
        logger.info("No new stream files to process.")
        _register_stream_gold_as_delta(
            spark,
            stream_gold_path,
            current_balances_work,
            recent_transactions_work,
        )
        _cleanup_work_dirs(current_balances_work, recent_transactions_work)
        return

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("SET memory_limit='300MB'")
        con.execute("SET threads=2")
        con.execute("SET temp_directory='/tmp'")

        current_balances_parquet = _find_parquet_files(current_balances_work)
        recent_transactions_parquet = _find_parquet_files(recent_transactions_work)

        if current_balances_parquet:
            con.execute(
                "CREATE TABLE current_balances AS "
                "SELECT * FROM read_parquet('"
                + current_balances_work
                + "/**/*.parquet', hive_partitioning=false)"
            )
        else:
            con.execute(
                """
                CREATE TABLE current_balances (
                    account_id VARCHAR,
                    current_balance DECIMAL(18,2),
                    last_transaction_timestamp TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """
            )

        if recent_transactions_parquet:
            con.execute(
                "CREATE TABLE recent_transactions AS "
                "SELECT * FROM read_parquet('"
                + recent_transactions_work
                + "/**/*.parquet', hive_partitioning=false)"
            )
        else:
            con.execute(
                """
                CREATE TABLE recent_transactions (
                    account_id VARCHAR,
                    transaction_id VARCHAR,
                    transaction_timestamp TIMESTAMP,
                    amount DECIMAL(18,2),
                    transaction_type VARCHAR,
                    channel VARCHAR,
                    updated_at TIMESTAMP
                )
                """
            )

        for file_path in files_to_process:
            file_name = os.path.basename(file_path)
            logger.info("Processing stream file: %s", file_name)

            batch_updated_at = con.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]

            con.execute(
                "CREATE OR REPLACE TEMP VIEW stream_batch AS "
                "SELECT * FROM read_json_auto('"
                + file_path.replace("\\", "/")
                + "', format='newline_delimited')"
            )

            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW stream_batch_normalized AS
                SELECT
                    transaction_id,
                    account_id,
                    COALESCE(
                        TRY_STRPTIME(transaction_date || ' ' || transaction_time, '%Y-%m-%d %H:%M:%S'),
                        TRY_STRPTIME(transaction_date || ' ' || transaction_time, '%d/%m/%Y %H:%M:%S'),
                        TRY_CAST(TO_TIMESTAMP(TRY_CAST(transaction_date AS BIGINT)) AS TIMESTAMP)
                    ) AS transaction_timestamp,
                    TRY_CAST(amount AS DECIMAL(18,2)) AS amount,
                    transaction_type,
                    channel,
                    TIMESTAMP '"""
                + str(batch_updated_at)
                + """' AS updated_at
                FROM stream_batch
                """
            )

            con.execute(
                """
                DELETE FROM current_balances
                USING (
                    SELECT DISTINCT account_id
                    FROM stream_batch_normalized
                ) s
                WHERE current_balances.account_id = s.account_id
                """
            )

            con.execute(
                """
                INSERT INTO current_balances
                SELECT
                    account_id,
                    amount AS current_balance,
                    transaction_timestamp AS last_transaction_timestamp,
                    updated_at
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY account_id
                               ORDER BY transaction_timestamp DESC
                           ) AS rn
                    FROM stream_batch_normalized
                )
                WHERE rn = 1
                """
            )

            con.execute(
                """
                DELETE FROM recent_transactions
                USING stream_batch_normalized s
                WHERE recent_transactions.account_id = s.account_id
                  AND recent_transactions.transaction_id = s.transaction_id
                """
            )

            con.execute(
                """
                INSERT INTO recent_transactions
                SELECT
                    account_id,
                    transaction_id,
                    transaction_timestamp,
                    amount,
                    transaction_type,
                    channel,
                    updated_at
                FROM stream_batch_normalized
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE recent_transactions_trimmed AS
                SELECT
                    account_id,
                    transaction_id,
                    transaction_timestamp,
                    amount,
                    transaction_type,
                    channel,
                    updated_at
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY account_id
                               ORDER BY transaction_timestamp DESC
                           ) AS rn
                    FROM recent_transactions
                )
                WHERE rn <= 50
                """
            )

            con.execute("DELETE FROM recent_transactions")
            con.execute("INSERT INTO recent_transactions SELECT * FROM recent_transactions_trimmed")

            _mark_processed(file_name)
            logger.info("Completed stream file: %s", file_name)

        _reset_directory(current_balances_work)
        _reset_directory(recent_transactions_work)

        con.execute(
            "COPY current_balances TO '"
            + current_balances_work
            + "/part.parquet' "
            "(FORMAT PARQUET, COMPRESSION UNCOMPRESSED)"
        )
        con.execute(
            "COPY recent_transactions TO '"
            + recent_transactions_work
            + "/part.parquet' "
            "(FORMAT PARQUET, COMPRESSION UNCOMPRESSED)"
        )

    finally:
        con.close()

    _register_stream_gold_as_delta(
        spark,
        stream_gold_path,
        current_balances_work,
        recent_transactions_work,
    )

    _cleanup_work_dirs(current_balances_work, recent_transactions_work)

    logger.info("Stage 3 stream ingestion complete.")


def _register_stream_gold_as_delta(
    spark: SparkSession,
    stream_gold_path: str,
    current_balances_work: str,
    recent_transactions_work: str,
) -> None:
    """Register Parquet work tables as Delta output tables."""
    current_balances_delta = os.path.join(stream_gold_path, "current_balances")
    recent_transactions_delta = os.path.join(stream_gold_path, "recent_transactions")

    cb_files = _find_parquet_files(current_balances_work)
    rt_files = _find_parquet_files(recent_transactions_work)

    if cb_files:
        cb_df = spark.read.parquet(*cb_files)
        (
            cb_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(current_balances_delta)
        )

    if rt_files:
        rt_df = spark.read.parquet(*rt_files)
        (
            rt_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(recent_transactions_delta)
        )


def _load_processed_files() -> set[str]:
    if not os.path.exists(_PROCESSED_STATE_FILE):
        return set()
    with open(_PROCESSED_STATE_FILE, "r", encoding="utf-8") as fh:
        return {line.strip() for line in fh if line.strip()}


def _mark_processed(file_name: str) -> None:
    with open(_PROCESSED_STATE_FILE, "a", encoding="utf-8") as fh:
        fh.write(file_name + "\n")


def _find_parquet_files(path: str) -> list[str]:
    return glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)


def _reset_directory(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    for file_path in glob.glob(os.path.join(path, "**", "*"), recursive=True):
        if os.path.isfile(file_path):
            os.remove(file_path)


def _remove_directory_contents(path: str) -> None:
    if not os.path.exists(path):
        return

    for file_path in glob.glob(os.path.join(path, "**", "*"), recursive=True):
        if os.path.isfile(file_path):
            os.remove(file_path)


def _cleanup_work_dirs(current_balances_work: str, recent_transactions_work: str) -> None:
    _remove_directory_contents(current_balances_work)
    _remove_directory_contents(recent_transactions_work)

    try:
        os.rmdir(current_balances_work)
    except OSError:
        pass

    try:
        os.rmdir(recent_transactions_work)
    except OSError:
        pass