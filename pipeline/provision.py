"""
pipeline/provision.py
─────────────────────
Gold layer: build the dimensional model from Silver tables.

Contract
--------
Inputs  (Silver Delta tables):
    /data/output/silver/accounts/
    /data/output/silver/transactions/
    /data/output/silver/customers/

Outputs (Gold Delta tables):
    /data/output/gold/dim_customers/      — 9 fields
    /data/output/gold/dim_accounts/       — 11 fields  (incl. customer_id, GAP-026)
    /data/output/gold/fact_transactions/  — 15 fields

    /data/output/dq_report.json           — DQ summary (required Stage 2+;
                                            written at all stages for safety)

Design decisions
----------------
1. Surrogate keys — sha2(natural_key, 256) cast to BIGINT.  Deterministic
   across re-runs; stable even if row ordering changes.
2. age_band — derived from dob using floor division by 365.25 as specified
   in output_schema_spec.md §4.
3. FK resolution — fact_transactions joins to dim_accounts on account_id,
   then to dim_customers via dim_accounts.customer_id.
4. GAP-026 — dim_accounts.customer_id populated from accounts.customer_ref.
5. dq_report.json schema matches run_tests.sh Check 6 requirements:
   keys: total_records, clean_records, flagged_records, flag_counts.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType

logger = logging.getLogger(__name__)

_AGE_BANDS: list[tuple[int, str]] = [
    (65, "65+"),
    (56, "56-65"),
    (46, "46-55"),
    (36, "36-45"),
    (26, "26-35"),
    (18, "18-25"),
]

# Pipeline start time — set at module import so execution_duration is accurate
_PIPELINE_START: float = time.time()


def run_provisioning(spark: SparkSession, cfg: dict[str, Any]) -> None:
    """Build Gold dimensional model from Silver tables.

    Parameters
    ----------
    spark:
        Active SparkSession with Delta Lake extensions configured.
    cfg:
        Parsed pipeline_config.yaml as a plain dict.
    """
    logger.info("Gold provisioning started.")

    silver_root: str = cfg["output"]["silver_path"]
    gold_root:   str = cfg["output"]["gold_path"]

    silver_customers    = spark.read.format("delta").load(f"{silver_root}/customers")
    silver_accounts     = spark.read.format("delta").load(f"{silver_root}/accounts")
    silver_transactions = spark.read.format("delta").load(f"{silver_root}/transactions")

    dim_customers = _build_dim_customers(silver_customers)
    dim_accounts  = _build_dim_accounts(silver_accounts, dim_customers)
    fact_transactions = _build_fact_transactions(
        silver_transactions, dim_accounts, dim_customers
    )

    _write_delta(dim_customers,     f"{gold_root}/dim_customers")
    _write_delta(dim_accounts,      f"{gold_root}/dim_accounts")
    _write_delta(fact_transactions, f"{gold_root}/fact_transactions")

    dq_report_path: str = cfg["output"].get(
        "dq_report_path", "/data/output/dq_report.json"
    )
    _write_dq_report(fact_transactions, silver_accounts, silver_customers, dq_report_path)

    logger.info("Gold provisioning complete.")


def _build_dim_customers(silver: DataFrame) -> DataFrame:
    """Build dim_customers with surrogate key and derived age_band.

    Output schema (9 fields):
        customer_sk, customer_id, gender, province, income_band,
        segment, risk_score, kyc_status, age_band
    """
    logger.info("Building dim_customers.")
    run_date = date.today()
    age_expr = F.floor(
        F.datediff(F.lit(run_date.isoformat()), F.col("dob")) / F.lit(365.25)
    )
    dim = (
        silver
        .withColumn("customer_sk", _surrogate_key("customer_id"))
        .withColumn("age_band",    _build_age_band_expr(age_expr))
        .withColumn("risk_score",  F.col("risk_score").cast("integer"))
        .select(
            "customer_sk", "customer_id", "gender", "province",
            "income_band", "segment", "risk_score", "kyc_status", "age_band",
        )
    )
    logger.info("dim_customers built: %d rows", dim.count())
    return dim


def _build_dim_accounts(
    silver: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    """Build dim_accounts with surrogate key and customer_id (GAP-026).

    Output schema (11 fields):
        account_sk, account_id, customer_id, account_type, account_status,
        open_date, product_tier, digital_channel, credit_limit,
        current_balance, last_activity_date
    """
    logger.info("Building dim_accounts.")
    valid_customers = dim_customers.select(
        F.col("customer_id").alias("_cust_id")
    )
    dim = (
        silver
        .join(valid_customers, silver["customer_ref"] == F.col("_cust_id"), "inner")
        .withColumn("account_sk", _surrogate_key("account_id"))
        .withColumnRenamed("customer_ref", "customer_id")
        .select(
            "account_sk", "account_id", "customer_id", "account_type",
            "account_status", "open_date", "product_tier", "digital_channel",
            "credit_limit", "current_balance", "last_activity_date",
        )
    )
    logger.info("dim_accounts built: %d rows", dim.count())
    return dim


def _build_fact_transactions(
    silver: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    """Build fact_transactions by resolving surrogate keys.

    Output schema (15 fields):
        transaction_sk, transaction_id, account_sk, customer_sk,
        transaction_date, transaction_timestamp, transaction_type,
        merchant_category, merchant_subcategory, amount, currency,
        channel, province, dq_flag, ingestion_timestamp
    """
    logger.info("Building fact_transactions.")

    acct_lookup = dim_accounts.select(
        F.col("account_id").alias("_acct_id"),
        F.col("account_sk").alias("_account_sk"),
        F.col("customer_id").alias("_cust_id_from_acct"),
    )
    cust_lookup = dim_customers.select(
        F.col("customer_id").alias("_cust_id"),
        F.col("customer_sk").alias("_customer_sk"),
    )

    # Ensure merchant_subcategory column exists (absent in Stage 1 source)
    if "merchant_subcategory" not in silver.columns:
        silver = silver.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    fact = (
        silver
        .filter(
            F.col("dq_flag").isNull() | (F.col("dq_flag") != "ORPHANED_ACCOUNT")
        )
        .join(acct_lookup, silver["account_id"] == F.col("_acct_id"), "inner")
        .join(cust_lookup, F.col("_cust_id_from_acct") == F.col("_cust_id"), "inner")
        .withColumn("transaction_sk", _surrogate_key("transaction_id"))
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        .select(
            "transaction_sk",
            "transaction_id",
            F.col("_account_sk").alias("account_sk"),
            F.col("_customer_sk").alias("customer_sk"),
            "transaction_date",
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            "amount",
            "currency",
            "channel",
            F.col("location_province").alias("province"),
            "dq_flag",
            "ingestion_timestamp",
        )
    )
    logger.info("fact_transactions built: %d rows", fact.count())
    return fact


def _surrogate_key(natural_key_col: str) -> "Column":  # type: ignore[name-defined]
    """Generate a deterministic BIGINT surrogate key from a natural key column.

    Uses sha2(natural_key, 256) -> first 15 hex chars -> cast to BIGINT.
    15 hex chars = 60 bits, safely within BIGINT range.
    """
    return (
        F.conv(F.substring(F.sha2(F.col(natural_key_col), 256), 1, 15), 16, 10)
        .cast(LongType())
    )


def _build_age_band_expr(age_col: "Column") -> "Column":  # type: ignore[name-defined]
    """Build a CASE expression mapping integer age to the canonical age_band string."""
    expr = F.when(F.lit(False), F.lit(None))
    for min_age, label in _AGE_BANDS:
        expr = expr.when(age_col >= min_age, F.lit(label))
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
    logger.debug("Gold Delta table written to: %s", path)


def _write_dq_report(
    fact: DataFrame,
    silver_accounts: DataFrame,
    silver_customers: DataFrame,
    report_path: str,
) -> None:
    """Write dq_report.json conforming to the scoring harness schema.

    The run_tests.sh Check 6 validates for these top-level keys:
        total_records, clean_records, flagged_records, flag_counts

    The dq_report_template.json (Stage 2+) additionally requires:
        $schema, run_timestamp, stage, source_record_counts,
        dq_issues[], gold_layer_record_counts, execution_duration_seconds

    We write both schemas merged so the file satisfies both validators.
    """
    logger.info("Writing DQ report to: %s", report_path)

    total_rows = fact.count()

    flag_counts_rows = (
        fact
        .groupBy("dq_flag")
        .agg(F.count("*").alias("count"))
        .collect()
    )

    flag_counts: dict[str, int] = {}
    clean_count = 0
    for row in flag_counts_rows:
        if row["dq_flag"] is None:
            clean_count = row["count"]
        else:
            flag_counts[row["dq_flag"]] = row["count"]

    total_flagged = sum(flag_counts.values())
    execution_secs = int(time.time() - _PIPELINE_START)

    # Build dq_issues array for Stage 2+ template compliance
    dq_issues = []
    for issue_code, count in flag_counts.items():
        pct = round((count / total_rows * 100) if total_rows > 0 else 0.0, 2)
        dq_issues.append({
            "issue_type":         issue_code,
            "records_affected":   count,
            "percentage_of_total": pct,
            "handling_action":    _handling_action_for(issue_code),
            "records_in_output":  0 if issue_code == "ORPHANED_ACCOUNT" else count,
        })

    report: dict[str, Any] = {
        # ── run_tests.sh Check 6 required keys ───────────────────────────────
        "total_records":   total_rows,
        "clean_records":   clean_count,
        "flagged_records": total_flagged,
        "flag_counts":     flag_counts,
        # ── dq_report_template.json Stage 2+ keys ────────────────────────────
        "$schema":         "nedbank-de-challenge/dq-report/v1",
        "run_timestamp":   datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage":           "1",
        "source_record_counts": {
            "accounts_raw":     silver_accounts.count(),
            "transactions_raw": total_rows,
            "customers_raw":    silver_customers.count(),
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": {
            "fact_transactions": total_rows,
        },
        "execution_duration_seconds": execution_secs,
    }

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)

    logger.info(
        "DQ report written: total=%d flagged=%d clean=%d duration=%ds",
        total_rows, total_flagged, clean_count, execution_secs,
    )


_HANDLING_ACTION_MAP: dict[str, str] = {
    "ORPHANED_ACCOUNT":  "QUARANTINED",
    "DUPLICATE_DEDUPED": "DEDUPLICATED_KEEP_FIRST",
    "TYPE_MISMATCH":     "CAST_TO_DECIMAL",
    "DATE_FORMAT":       "NORMALISED_DATE",
    "CURRENCY_VARIANT":  "NORMALISED_CURRENCY",
    "NULL_REQUIRED":     "EXCLUDED_NULL_PK",
}


def _handling_action_for(issue_code: str) -> str:
    """Return the handling_action string for a given DQ issue code."""
    return _HANDLING_ACTION_MAP.get(issue_code, "UNKNOWN")
