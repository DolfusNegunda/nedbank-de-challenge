"""
pipeline/ingest.py
──────────────────
Bronze layer: ingest raw source files into Delta Parquet tables.

Contract
--------
Inputs  (read-only mounts):
    /data/input/accounts.csv
    /data/input/transactions.jsonl
    /data/input/customers.csv

Outputs (created by this module):
    /data/output/bronze/accounts/
    /data/output/bronze/transactions/
    /data/output/bronze/customers/

Design decisions
----------------
1. As-arrived preservation — no type coercion, no filtering.
2. JSONL flattening — nested location/metadata structs are flattened to
   top-level columns so Silver can apply typed casts without re-parsing JSON.
3. Explicit JSONL schema — avoids a full-file scan for inference on 1 M / 3.15 M records.
4. mode=overwrite + overwriteSchema=True — idempotent re-runs.
5. Single ingestion_timestamp literal per run (not per-row) for watermarking.
6. Returns raw source counts for Stage 2 dq_report.json generation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("transaction_date", StringType(), nullable=False),
        StructField("transaction_time", StringType(), nullable=False),
        StructField("transaction_type", StringType(), nullable=False),
        StructField("merchant_category", StringType(), nullable=True),
        StructField("merchant_subcategory", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("channel", StringType(), nullable=False),
        StructField(
            "location",
            StructType(
                [
                    StructField("province", StringType(), nullable=True),
                    StructField("city", StringType(), nullable=True),
                    StructField("coordinates", StringType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "metadata",
            StructType(
                [
                    StructField("device_id", StringType(), nullable=True),
                    StructField("session_id", StringType(), nullable=True),
                    StructField("retry_flag", BooleanType(), nullable=False),
                ]
            ),
            nullable=True,
        ),
    ]
)


def run_ingestion(spark: SparkSession, cfg: dict[str, Any]) -> dict[str, int]:
    """Ingest all three source files into the Bronze Delta layer.

    Parameters
    ----------
    spark:
        Active SparkSession with Delta Lake extensions configured.
    cfg:
        Parsed pipeline_config.yaml as a plain dict.

    Returns
    -------
    dict[str, int]
        Raw source record counts keyed as:
        - accounts_raw
        - customers_raw
        - transactions_raw
    """
    ingestion_ts = datetime.now(tz=timezone.utc)
    logger.info("Bronze ingestion started at %s", ingestion_ts.isoformat())

    input_cfg: dict[str, str] = cfg["input"]
    bronze_root: str = cfg["output"]["bronze_path"]

    accounts_df = _read_accounts(
        spark=spark,
        src_path=input_cfg["accounts_path"],
        ingestion_ts=ingestion_ts,
    )
    customers_df = _read_customers(
        spark=spark,
        src_path=input_cfg["customers_path"],
        ingestion_ts=ingestion_ts,
    )
    transactions_df = _read_transactions(
        spark=spark,
        src_path=input_cfg["transactions_path"],
        ingestion_ts=ingestion_ts,
    )

    source_counts: dict[str, int] = {
        "accounts_raw": accounts_df.count(),
        "customers_raw": customers_df.count(),
        "transactions_raw": transactions_df.count(),
    }

    _write_delta(accounts_df, f"{bronze_root}/accounts")
    logger.info("Accounts ingested: %d rows", source_counts["accounts_raw"])

    _write_delta(customers_df, f"{bronze_root}/customers")
    logger.info("Customers ingested: %d rows", source_counts["customers_raw"])

    _write_delta(transactions_df, f"{bronze_root}/transactions")
    logger.info("Transactions ingested: %d rows", source_counts["transactions_raw"])

    logger.info("Bronze ingestion complete.")
    return source_counts


def _read_accounts(
    spark: SparkSession,
    src_path: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """Read accounts.csv into a Bronze dataframe."""
    logger.info("Ingesting accounts: %s -> Bronze dataframe", src_path)

    df: DataFrame = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(src_path)
    )

    return _add_ingestion_timestamp(df, ingestion_ts)


def _read_customers(
    spark: SparkSession,
    src_path: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """Read customers.csv into a Bronze dataframe."""
    logger.info("Ingesting customers: %s -> Bronze dataframe", src_path)

    df: DataFrame = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(src_path)
    )

    return _add_ingestion_timestamp(df, ingestion_ts)


def _read_transactions(
    spark: SparkSession,
    src_path: str,
    ingestion_ts: datetime,
) -> DataFrame:
    """Read transactions.jsonl into a Bronze dataframe.

    Nested location and metadata structs are flattened to top-level columns.
    """
    logger.info("Ingesting transactions: %s -> Bronze dataframe", src_path)

    df: DataFrame = (
        spark.read
        .schema(_TRANSACTION_SCHEMA)
        .json(src_path)
    )

    df = (
        df
        .withColumn("location_province", F.col("location.province"))
        .withColumn("location_city", F.col("location.city"))
        .withColumn("location_coordinates", F.col("location.coordinates"))
        .withColumn("metadata_device_id", F.col("metadata.device_id"))
        .withColumn("metadata_session_id", F.col("metadata.session_id"))
        .withColumn("metadata_retry_flag", F.col("metadata.retry_flag"))
        .drop("location", "metadata")
    )

    return _add_ingestion_timestamp(df, ingestion_ts)


def _add_ingestion_timestamp(df: DataFrame, ts: datetime) -> DataFrame:
    """Append a single ingestion_timestamp literal column to df."""
    return df.withColumn(
        "ingestion_timestamp",
        F.lit(ts.isoformat()).cast("timestamp"),
    )


def _write_delta(df: DataFrame, path: str) -> None:
    """Write df to path as a Delta table, overwriting any prior run."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    logger.debug("Delta table written to: %s", path)