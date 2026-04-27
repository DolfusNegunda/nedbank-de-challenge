"""
pipeline/transform.py
─────────────────────
Silver layer: clean, type-cast, deduplicate, and DQ-flag Bronze tables.

Contract
--------
Inputs  (Bronze Delta tables):
    /data/output/bronze/accounts/
    /data/output/bronze/transactions/
    /data/output/bronze/customers/

Outputs (Silver Delta tables):
    /data/output/silver/accounts/
    /data/output/silver/transactions/
    /data/output/silver/customers/

Design decisions
----------------
1. Type safety — all date/decimal/boolean casts happen here, not in Gold.
2. Deduplication — window-function based, keeping the first occurrence
   ordered by ingestion_timestamp.  Deterministic and idempotent.
3. DQ flagging — single dq_flag column on silver/transactions.
   Stage 1 data is clean so all flags will be NULL.
   Stage 2 DQ handling is already wired — no structural changes needed.
4. Currency normalisation — all known ZAR variants mapped to "ZAR" before
   the flag is applied, so Gold always sees "ZAR".
5. merchant_subcategory — absent in Stage 1 JSON; arrives as null column.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logger = logging.getLogger(__name__)

# ── DQ issue codes (output_schema_spec.md §8) ─────────────────────────────────
_DQ_ORPHANED_ACCOUNT  = "ORPHANED_ACCOUNT"
_DQ_DUPLICATE_DEDUPED = "DUPLICATE_DEDUPED"
_DQ_TYPE_MISMATCH     = "TYPE_MISMATCH"
_DQ_DATE_FORMAT       = "DATE_FORMAT"
_DQ_CURRENCY_VARIANT  = "CURRENCY_VARIANT"
_DQ_NULL_REQUIRED     = "NULL_REQUIRED"

# ── Currency normalisation map (data_dictionary.md §4) ────────────────────────
_CURRENCY_VARIANTS: dict[str, str] = {
    "ZAR":   "ZAR",
    "zar":   "ZAR",
    "R":     "ZAR",
    "rands": "ZAR",
    "710":   "ZAR",
}

# ── Date format patterns (Stage 1: ISO only; Stage 2: adds DD/MM/YYYY + epoch)
_DATE_FORMATS: list[str] = [
    "yyyy-MM-dd",
    "dd/MM/yyyy",
]


def run_transformation(spark: SparkSession, cfg: dict[str, Any]) -> None:
    """Transform Bronze tables into typed, deduplicated Silver tables.

    Parameters
    ----------
    spark:
        Active SparkSession with Delta Lake extensions configured.
    cfg:
        Parsed pipeline_config.yaml as a plain dict.
    """
    logger.info("Silver transformation started.")

    bronze_root: str = cfg["output"]["bronze_path"]
    silver_root: str = cfg["output"]["silver_path"]

    bronze_accounts     = spark.read.format("delta").load(f"{bronze_root}/accounts")
    bronze_customers    = spark.read.format("delta").load(f"{bronze_root}/customers")
    bronze_transactions = spark.read.format("delta").load(f"{bronze_root}/transactions")

    silver_customers    = _transform_customers(bronze_customers)
    silver_accounts     = _transform_accounts(bronze_accounts, silver_customers)
    silver_transactions = _transform_transactions(bronze_transactions, silver_accounts)

    _write_delta(silver_customers,    f"{silver_root}/customers")
    _write_delta(silver_accounts,     f"{silver_root}/accounts")
    _write_delta(silver_transactions, f"{silver_root}/transactions")

    logger.info("Silver transformation complete.")


def _transform_customers(df: DataFrame) -> DataFrame:
    """Type-cast and deduplicate the customers Bronze table."""
    logger.info("Transforming customers.")
    df = df.withColumn("dob", _parse_date_col(df["dob"]))
    df = _dedup(df, pk="customer_id", order_col="ingestion_timestamp")
    logger.info("Customers transformed: %d rows", df.count())
    return df


def _transform_accounts(
    df: DataFrame,
    silver_customers: DataFrame,
) -> DataFrame:
    """Type-cast, deduplicate, and validate referential integrity for accounts."""
    logger.info("Transforming accounts.")

    df = (
        df
        .withColumn("open_date",         _parse_date_col(df["open_date"]))
        .withColumn("last_activity_date", _parse_date_col(df["last_activity_date"]))
        .withColumn("credit_limit",       F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance",    F.col("current_balance").cast(DecimalType(18, 2)))
    )

    # Stage 2: drop rows where account_id is null (NULL_REQUIRED — primary key)
    null_pk_count = df.filter(F.col("account_id").isNull()).count()
    if null_pk_count > 0:
        logger.warning(
            "Dropping %d account rows with null account_id (NULL_REQUIRED).",
            null_pk_count,
        )
    df = df.filter(F.col("account_id").isNotNull())
    df = _dedup(df, pk="account_id", order_col="ingestion_timestamp")

    orphaned = (
        df
        .join(
            silver_customers.select(F.col("customer_id").alias("_valid_cid")),
            df["customer_ref"] == F.col("_valid_cid"),
            "left_anti",
        )
        .count()
    )
    if orphaned > 0:
        logger.warning(
            "%d account rows have no matching customer_id — excluded from Gold joins.",
            orphaned,
        )

    logger.info("Accounts transformed: %d rows", df.count())
    return df


def _transform_transactions(
    df: DataFrame,
    silver_accounts: DataFrame,
) -> DataFrame:
    """Type-cast, deduplicate, and DQ-flag the transactions Bronze table."""
    logger.info("Transforming transactions.")

    # 1. Deduplication — keep earliest transaction_time per transaction_id
    df = _dedup(df, pk="transaction_id", order_col="transaction_time")

    # 2. Currency normalisation + CURRENCY_VARIANT flag
    currency_map_expr = _build_currency_map_expr()
    df = (
        df
        .withColumn("_normalised_currency", currency_map_expr)
        .withColumn(
            "dq_flag",
            F.when(
                (F.col("currency") != "ZAR") & F.col("_normalised_currency").isNotNull(),
                F.lit(_DQ_CURRENCY_VARIANT),
            ).otherwise(F.lit(None).cast("string"))
        )
        .withColumn("currency", F.col("_normalised_currency"))
        .drop("_normalised_currency")
    )

    # 3. Amount type cast + TYPE_MISMATCH flag
    df = (
        df
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        .withColumn(
            "dq_flag",
            F.when(
                F.col("amount").isNull() & F.col("dq_flag").isNull(),
                F.lit(_DQ_TYPE_MISMATCH),
            ).otherwise(F.col("dq_flag"))
        )
    )

    # 4. Date parsing + DATE_FORMAT flag
    df = (
        df
        .withColumn("_parsed_date", _parse_date_col(df["transaction_date"]))
        .withColumn(
            "dq_flag",
            F.when(
                F.col("_parsed_date").isNull() & F.col("dq_flag").isNull(),
                F.lit(_DQ_DATE_FORMAT),
            ).otherwise(F.col("dq_flag"))
        )
        .withColumn("transaction_date", F.col("_parsed_date"))
        .drop("_parsed_date")
    )

    # 5. Combine date + time into transaction_timestamp
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(" ", F.col("transaction_date").cast("string"), F.col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    # 6. ORPHANED_ACCOUNT flag
    valid_account_ids = silver_accounts.select(
        F.col("account_id").alias("_valid_aid")
    )
    df = (
        df
        .join(valid_account_ids, df["account_id"] == F.col("_valid_aid"), "left")
        .withColumn(
            "dq_flag",
            F.when(
                F.col("_valid_aid").isNull() & F.col("dq_flag").isNull(),
                F.lit(_DQ_ORPHANED_ACCOUNT),
            ).otherwise(F.col("dq_flag"))
        )
        .drop("_valid_aid")
    )

    logger.info("Transactions transformed: %d rows", df.count())
    return df


def _dedup(df: DataFrame, pk: str, order_col: str) -> DataFrame:
    """Return df deduplicated on pk, keeping the row with the lowest order_col."""
    w = Window.partitionBy(pk).orderBy(F.col(order_col).asc())
    return (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def _parse_date_col(col: "Column") -> "Column":  # type: ignore[name-defined]
    """Try each known date format; return first successful parse or null."""
    parsed = F.coalesce(*[F.to_date(col, fmt) for fmt in _DATE_FORMATS])
    epoch_parsed = F.to_date(F.from_unixtime(col.cast("long")), "yyyy-MM-dd")
    return F.coalesce(parsed, epoch_parsed)


def _build_currency_map_expr() -> "Column":  # type: ignore[name-defined]
    """Build a CASE expression mapping all known ZAR variants to "ZAR"."""
    expr = F.when(F.lit(False), F.lit(None))
    for variant, canonical in _CURRENCY_VARIANTS.items():
        expr = expr.when(F.col("currency") == variant, F.lit(canonical))
    return expr.otherwise(F.lit(None).cast("string"))


def _write_delta(df: DataFrame, path: str) -> None:
    """Write df to path as a Delta table, overwriting any prior run."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    logger.debug("Silver Delta table written to: %s", path)
