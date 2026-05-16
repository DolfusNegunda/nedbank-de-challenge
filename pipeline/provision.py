"""
pipeline/provision.py
─────────────────────
Gold layer: build the dimensional model from Silver tables and write dq_report.json.

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

    /data/output/dq_report.json           — DQ summary (required Stage 2+)

Design decisions
----------------
1. Surrogate keys — deterministic BIGINT derived from sha2(natural_key, 256).
2. age_band — derived from dob using floor division by 365.25.
3. FK resolution — fact_transactions joins dim_accounts on account_id, then
   joins dim_customers via dim_accounts.customer_id.
4. GAP-026 — dim_accounts.customer_id is populated from accounts.customer_ref.
5. dq_report.json is written from true raw source counts + explicit dq_counts
   passed in from earlier pipeline phases, not inferred from Gold only.
"""

from __future__ import annotations

import json
import logging
import os
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

_HANDLING_ACTION_MAP: dict[str, str] = {
    "DUPLICATE_DEDUPED": "DEDUPLICATED_KEEP_FIRST",
    "ORPHANED_ACCOUNT": "QUARANTINED",
    "TYPE_MISMATCH": "CAST_TO_DECIMAL",
    "DATE_FORMAT": "NORMALISED_DATE",
    "CURRENCY_VARIANT": "NORMALISED_CURRENCY",
    "NULL_REQUIRED": "EXCLUDED_NULL_PK",
}


def run_provisioning(
    spark: SparkSession,
    cfg: dict[str, Any],
    source_counts: dict[str, int],
    dq_counts: dict[str, int],
    execution_duration_seconds: int,
) -> None:
    """Build Gold dimensional model from Silver tables and write dq_report.json.

    Parameters
    ----------
    spark:
        Active SparkSession with Delta Lake extensions configured.
    cfg:
        Parsed pipeline_config.yaml as a plain dict.
    source_counts:
        Raw source counts returned by Bronze ingestion.
    dq_counts:
        Explicit DQ counts returned by Silver transformation phases.
    execution_duration_seconds:
        Wall-clock duration from pipeline start to report generation.
    """
    if source_counts is None:
        raise ValueError("source_counts must not be None")
    if dq_counts is None:
        raise ValueError("dq_counts must not be None")

    logger.info("Gold provisioning started.")

    silver_root: str = cfg["output"]["silver_path"]
    gold_root: str = cfg["output"]["gold_path"]

    silver_customers = spark.read.format("delta").load(f"{silver_root}/customers")
    silver_accounts = spark.read.format("delta").load(f"{silver_root}/accounts")
    silver_transactions = spark.read.format("delta").load(f"{silver_root}/transactions")

    dim_customers = _build_dim_customers(silver_customers)
    dim_accounts = _build_dim_accounts(silver_accounts, dim_customers)
    fact_transactions = _build_fact_transactions(
        silver_transactions,
        dim_accounts,
        dim_customers,
    )

    _write_delta(dim_customers, f"{gold_root}/dim_customers")
    _write_delta(dim_accounts, f"{gold_root}/dim_accounts")
    _write_delta(fact_transactions, f"{gold_root}/fact_transactions")

    gold_counts = {
        "fact_transactions": fact_transactions.count(),
        "dim_accounts": dim_accounts.count(),
        "dim_customers": dim_customers.count(),
    }

    dq_report_path: str = cfg["output"].get(
        "dq_report_path",
        "/data/output/dq_report.json",
    )

    _write_dq_report(
        report_path=dq_report_path,
        source_counts=source_counts,
        dq_counts=dq_counts,
        gold_counts=gold_counts,
        stage=cfg.get("stage", "2"),
        execution_duration_seconds=execution_duration_seconds,
    )

    logger.info("Gold provisioning complete.")


def _build_dim_customers(silver: DataFrame) -> DataFrame:
    """Build dim_customers with surrogate key and derived age_band.

    Output schema:
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
        .withColumn("age_band", _build_age_band_expr(age_expr))
        .withColumn("risk_score", F.col("risk_score").cast("integer"))
        .select(
            "customer_sk",
            "customer_id",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "age_band",
        )
    )

    logger.info("dim_customers built: %d rows", dim.count())
    return dim


def _build_dim_accounts(
    silver: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    """Build dim_accounts with surrogate key and customer_id (GAP-026).

    Output schema:
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
        .join(
            F.broadcast(valid_customers),
            silver["customer_ref"] == F.col("_cust_id"),
            "inner",
        )
        .withColumn("account_sk", _surrogate_key("account_id"))
        .withColumnRenamed("customer_ref", "customer_id")
        .select(
            "account_sk",
            "account_id",
            "customer_id",
            "account_type",
            "account_status",
            "open_date",
            "product_tier",
            "digital_channel",
            "credit_limit",
            "current_balance",
            "last_activity_date",
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

    Output schema:
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

    if "merchant_subcategory" not in silver.columns:
        silver = silver.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    fact = (
        silver
        .filter(
            F.col("dq_flag").isNull() | (F.col("dq_flag") != "ORPHANED_ACCOUNT")
        )
        .join(
            F.broadcast(acct_lookup),
            silver["account_id"] == F.col("_acct_id"),
            "inner",
        )
        .join(
            F.broadcast(cust_lookup),
            F.col("_cust_id_from_acct") == F.col("_cust_id"),
            "inner",
        )
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
    """Generate a deterministic BIGINT surrogate key from a natural key column."""
    return (
        F.conv(F.substring(F.sha2(F.col(natural_key_col), 256), 1, 15), 16, 10)
        .cast(LongType())
    )


def _build_age_band_expr(age_col: "Column") -> "Column":  # type: ignore[name-defined]
    """Build a CASE expression mapping integer age to the canonical age_band."""
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
    report_path: str,
    source_counts: dict[str, int],
    dq_counts: dict[str, int],
    gold_counts: dict[str, int],
    stage: str,
    execution_duration_seconds: int,
) -> None:
    """Write a Stage 2-compliant dq_report.json with correct reconciliation."""
    logger.info("Writing DQ report to: %s", report_path)

    total_records = source_counts["transactions_raw"]

    non_zero_flag_counts = {
        issue_code: count
        for issue_code, count in dq_counts.items()
        if count > 0
    }

    fact_transactions_count = gold_counts["fact_transactions"]

    # Only retained flagged cohorts contribute to final flagged_records.
    retained_flagged_records = (
        non_zero_flag_counts.get("TYPE_MISMATCH", 0)
        + non_zero_flag_counts.get("DATE_FORMAT", 0)
        + non_zero_flag_counts.get("CURRENCY_VARIANT", 0)
    )

    flagged_records = retained_flagged_records
    clean_records = fact_transactions_count - flagged_records

    if clean_records < 0:
        raise ValueError(
            "DQ reconciliation failed: clean_records became negative. "
            f"fact_transactions={fact_transactions_count}, "
            f"retained_flagged_records={retained_flagged_records}"
        )

    dq_issues: list[dict[str, Any]] = []
    for issue_code, count in non_zero_flag_counts.items():
        denominator = (
            source_counts["accounts_raw"]
            if issue_code == "NULL_REQUIRED"
            else total_records
        )

        records_in_output = (
            0
            if issue_code in {"ORPHANED_ACCOUNT", "NULL_REQUIRED", "DUPLICATE_DEDUPED"}
            else count
        )

        dq_issues.append(
            {
                "issue_type": issue_code,
                "records_affected": count,
                "percentage_of_total": round((count / denominator) * 100, 2),
                "handling_action": _HANDLING_ACTION_MAP[issue_code],
                "records_in_output": records_in_output,
            }
        )

    report: dict[str, Any] = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage": str(stage),
        "source_record_counts": {
            "accounts_raw": source_counts["accounts_raw"],
            "transactions_raw": source_counts["transactions_raw"],
            "customers_raw": source_counts["customers_raw"],
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": gold_counts,
        "execution_duration_seconds": execution_duration_seconds,
    }

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)

    logger.info(
        "DQ report written: total=%d fact=%d flagged=%d clean=%d duration=%ds",
        total_records,
        fact_transactions_count,
        flagged_records,
        clean_records,
        execution_duration_seconds,
    )