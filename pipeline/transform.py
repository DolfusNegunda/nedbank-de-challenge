"""
pipeline/transform.py
─────────────────────
Silver layer: clean, type-cast, deduplicate, and DQ-flag Bronze tables.

Architecture
------------
run_silver_small_tables():
    Spark-based transformation for customers and accounts.

run_silver_transactions_duckdb():
    DuckDB-based transformation for large Stage 2 transaction volume.

run_silver_transactions_delta_register():
    Spark re-registers DuckDB output as Delta.
"""

from __future__ import annotations

import glob
import logging
import os
import uuid
from typing import Any

import duckdb
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logger = logging.getLogger(__name__)

_DQ_ORPHANED_ACCOUNT = "ORPHANED_ACCOUNT"
_DQ_DUPLICATE_DEDUPED = "DUPLICATE_DEDUPED"
_DQ_TYPE_MISMATCH = "TYPE_MISMATCH"
_DQ_DATE_FORMAT = "DATE_FORMAT"
_DQ_CURRENCY_VARIANT = "CURRENCY_VARIANT"
_DQ_NULL_REQUIRED = "NULL_REQUIRED"

_CURRENCY_VARIANTS: dict[str, str] = {
    "ZAR": "ZAR",
    "zar": "ZAR",
    "R": "ZAR",
    "rands": "ZAR",
    "710": "ZAR",
}

_DATE_FORMATS: list[str] = [
    "yyyy-MM-dd",
    "yyyy-MM-dd HH:mm:ss",
    "dd/MM/yyyy",
    "dd/MM/yyyy HH:mm:ss",
]


def run_silver_small_tables(spark: SparkSession, cfg: dict[str, Any]) -> dict[str, int]:
    """Transform Bronze customers and accounts into Silver Delta tables."""
    logger.info("Silver transformation started (small tables).")

    bronze_root = cfg["output"]["bronze_path"]
    silver_root = cfg["output"]["silver_path"]

    bronze_customers = spark.read.format("delta").load(bronze_root + "/customers")
    bronze_accounts = spark.read.format("delta").load(bronze_root + "/accounts")

    silver_customers = _transform_customers(bronze_customers)
    silver_accounts, null_pk_count = _transform_accounts(bronze_accounts, silver_customers)

    _write_delta_spark(silver_customers.coalesce(2), silver_root + "/customers")
    logger.info("Silver customers written.")

    _write_delta_spark(silver_accounts.coalesce(2), silver_root + "/accounts")
    logger.info("Silver accounts written.")

    return {
        _DQ_NULL_REQUIRED: null_pk_count,
    }


def run_silver_transactions_duckdb(cfg: dict[str, Any]) -> dict[str, int]:
    """Process Silver transactions in DuckDB using a two-pass chunked strategy."""
    bronze_root = cfg["output"]["bronze_path"]
    silver_root = cfg["output"]["silver_path"]

    bronze_path = bronze_root + "/transactions"
    silver_accts = silver_root + "/accounts"
    silver_path = silver_root + "/transactions"

    logger.info("Transforming transactions via DuckDB (two-pass chunked).")

    bronze_files = sorted(_find_parquet_files(bronze_path))
    acct_parquet = _find_parquet_files(silver_accts)

    if not bronze_files:
        raise RuntimeError("No Parquet files in Bronze transactions: " + bronze_path)
    if not acct_parquet:
        raise RuntimeError("No Parquet files in Silver accounts: " + silver_accts)

    logger.info("Bronze transaction Parquet files: %d", len(bronze_files))
    os.makedirs(silver_path, exist_ok=True)

    run_id = uuid.uuid4().hex
    dedup_keys_path = f"/tmp/dedup_keys_{run_id}.parquet"

    acct_glob = silver_accts + "/**/*.parquet"
    bronze_glob = bronze_path + "/**/*.parquet"

    duplicate_count_total = 0
    orphan_count_total = 0
    type_mismatch_count_total = 0
    date_format_count_total = 0
    currency_variant_count_total = 0

    logger.info("Pass 1: building dedup keys (transaction_id, min_timestamp).")
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("SET memory_limit='300MB'")
        con.execute("SET threads=2")
        con.execute("SET temp_directory='/tmp'")

        con.execute(
            "CREATE VIEW all_txn AS "
            "SELECT transaction_id, "
            "       transaction_date, "
            "       transaction_time "
            "FROM read_parquet('" + bronze_glob + "', hive_partitioning=false)"
        )

        con.execute(
            "COPY ("
            "  SELECT transaction_id, "
            "         MIN("
            "             COALESCE("
            "                 TRY_STRPTIME(transaction_date || ' ' || transaction_time, '%Y-%m-%d %H:%M:%S'),"
            "                 TRY_STRPTIME(transaction_date || ' ' || transaction_time, '%d/%m/%Y %H:%M:%S'),"
            "                 TRY_STRPTIME(transaction_date || ' 00:00:00', '%Y-%m-%d %H:%M:%S'),"
            "                 TRY_STRPTIME(transaction_date || ' 00:00:00', '%d/%m/%Y %H:%M:%S')"
            "             )"
            "         ) AS min_ts "
            "  FROM all_txn "
            "  GROUP BY transaction_id"
            ") TO '" + dedup_keys_path + "' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)"
        )

        duplicate_row = con.execute(
            "SELECT "
            "  (SELECT COUNT(*) FROM all_txn) - "
            "  (SELECT COUNT(*) FROM read_parquet('" + dedup_keys_path + "'))"
        ).fetchone()

        duplicate_count_total = int(duplicate_row[0] or 0)

        logger.info(
            "Dedup keys written to: %s (global duplicate rows=%d)",
            dedup_keys_path,
            duplicate_count_total,
        )
    finally:
        con.close()

    currency_case_lines = []
    for variant, canonical in _CURRENCY_VARIANTS.items():
        currency_case_lines.append(
            "                WHEN currency = '" + variant + "' THEN '" + canonical + "'"
        )
    currency_cases_sql = "\n".join(currency_case_lines)

    logger.info("Pass 2: processing %d Bronze files individually.", len(bronze_files))

    for idx, bronze_file in enumerate(bronze_files):
        out_file = silver_path + "/chunk_" + str(idx).zfill(4) + ".parquet"

        con = duckdb.connect(database=":memory:")
        try:
            con.execute("SET memory_limit='300MB'")
            con.execute("SET threads=2")
            con.execute("SET temp_directory='/tmp'")

            con.execute(
                "CREATE VIEW dedup_keys AS "
                "SELECT * FROM read_parquet('" + dedup_keys_path + "')"
            )
            con.execute(
                "CREATE VIEW silver_acct AS "
                "SELECT account_id "
                "FROM read_parquet('" + acct_glob + "', hive_partitioning=false)"
            )
            con.execute(
                "CREATE VIEW chunk_txn AS "
                "SELECT * FROM read_parquet('" + bronze_file + "')"
            )

            sql = (
                "COPY (\n"
                "    WITH parsed AS (\n"
                "        SELECT *,\n"
                "            COALESCE(\n"
                "                TRY_STRPTIME(transaction_date || ' ' || transaction_time, '%Y-%m-%d %H:%M:%S'),\n"
                "                TRY_STRPTIME(transaction_date || ' ' || transaction_time, '%d/%m/%Y %H:%M:%S'),\n"
                "                TRY_STRPTIME(transaction_date || ' 00:00:00', '%Y-%m-%d %H:%M:%S'),\n"
                "                TRY_STRPTIME(transaction_date || ' 00:00:00', '%d/%m/%Y %H:%M:%S'),\n"
                "                TRY_CAST(TO_TIMESTAMP(TRY_CAST(transaction_date AS BIGINT)) AS TIMESTAMP)\n"
                "            ) AS _txn_ts,\n"
                "            COALESCE(\n"
                "                TRY_STRPTIME(transaction_date, '%Y-%m-%d')::DATE,\n"
                "                TRY_STRPTIME(transaction_date, '%Y-%m-%d %H:%M:%S')::DATE,\n"
                "                TRY_STRPTIME(transaction_date, '%d/%m/%Y')::DATE,\n"
                "                TRY_STRPTIME(transaction_date, '%d/%m/%Y %H:%M:%S')::DATE,\n"
                "                TRY_CAST(TO_TIMESTAMP(TRY_CAST(transaction_date AS BIGINT)) AS DATE)\n"
                "            ) AS _parsed_date,\n"
                "            CASE\n"
                "                WHEN TRY_STRPTIME(transaction_date, '%Y-%m-%d') IS NOT NULL THEN FALSE\n"
                "                WHEN TRY_STRPTIME(transaction_date, '%Y-%m-%d %H:%M:%S') IS NOT NULL THEN TRUE\n"
                "                WHEN TRY_STRPTIME(transaction_date, '%d/%m/%Y') IS NOT NULL THEN TRUE\n"
                "                WHEN TRY_STRPTIME(transaction_date, '%d/%m/%Y %H:%M:%S') IS NOT NULL THEN TRUE\n"
                "                WHEN TRY_CAST(transaction_date AS BIGINT) IS NOT NULL THEN TRUE\n"
                "                ELSE FALSE\n"
                "            END AS _date_was_non_iso\n"
                "        FROM chunk_txn\n"
                "    ),\n"
                "    deduped AS (\n"
                "        SELECT p.*\n"
                "        FROM parsed p\n"
                "        INNER JOIN dedup_keys d\n"
                "          ON p.transaction_id = d.transaction_id\n"
                "         AND p._txn_ts = d.min_ts\n"
                "    ),\n"
                "    curr AS (\n"
                "        SELECT *,\n"
                "            CASE\n"
                + currency_cases_sql + "\n"
                "                ELSE NULL\n"
                "            END AS _norm_currency\n"
                "        FROM deduped\n"
                "    ),\n"
                "    amounted AS (\n"
                "        SELECT *, TRY_CAST(amount AS DECIMAL(18,2)) AS _amount_cast\n"
                "        FROM curr\n"
                "    ),\n"
                "    orphaned AS (\n"
                "        SELECT t.*, (a.account_id IS NULL) AS _is_orphan\n"
                "        FROM amounted t\n"
                "        LEFT JOIN silver_acct a USING (account_id)\n"
                "    )\n"
                "    SELECT\n"
                "        transaction_id,\n"
                "        account_id,\n"
                "        _parsed_date AS transaction_date,\n"
                "        transaction_time,\n"
                "        transaction_type,\n"
                "        merchant_category,\n"
                "        merchant_subcategory,\n"
                "        _amount_cast AS amount,\n"
                "        COALESCE(_norm_currency, currency) AS currency,\n"
                "        channel,\n"
                "        location_province,\n"
                "        location_city,\n"
                "        location_coordinates,\n"
                "        metadata_device_id,\n"
                "        metadata_session_id,\n"
                "        metadata_retry_flag,\n"
                "        ingestion_timestamp,\n"
                "        CASE\n"
                "            WHEN currency != 'ZAR' AND _norm_currency IS NOT NULL THEN '" + _DQ_CURRENCY_VARIANT + "'\n"
                "            WHEN _amount_cast IS NULL THEN '" + _DQ_TYPE_MISMATCH + "'\n"
                "            WHEN _parsed_date IS NULL THEN '" + _DQ_DATE_FORMAT + "'\n"
                "            WHEN _date_was_non_iso THEN '" + _DQ_DATE_FORMAT + "'\n"
                "            WHEN _is_orphan THEN '" + _DQ_ORPHANED_ACCOUNT + "'\n"
                "            ELSE NULL\n"
                "        END AS dq_flag,\n"
                "        _txn_ts AS transaction_timestamp,\n"
                "        _date_was_non_iso\n"
                "    FROM orphaned\n"
                ")\n"
                "TO '" + out_file + "' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED)"
            )

            con.execute(sql)

            dq_counts = con.execute(
                "SELECT "
                "  SUM(CASE WHEN dq_flag = '" + _DQ_ORPHANED_ACCOUNT + "' THEN 1 ELSE 0 END), "
                "  SUM(CASE WHEN dq_flag = '" + _DQ_TYPE_MISMATCH + "' THEN 1 ELSE 0 END), "
                "  SUM(CASE WHEN _date_was_non_iso OR dq_flag = '" + _DQ_DATE_FORMAT + "' THEN 1 ELSE 0 END), "
                "  SUM(CASE WHEN dq_flag = '" + _DQ_CURRENCY_VARIANT + "' THEN 1 ELSE 0 END) "
                "FROM read_parquet('" + out_file + "')"
            ).fetchone()

            orphan_count_total += int(dq_counts[0] or 0)
            type_mismatch_count_total += int(dq_counts[1] or 0)
            date_format_count_total += int(dq_counts[2] or 0)
            currency_variant_count_total += int(dq_counts[3] or 0)

        finally:
            con.close()

        if (idx + 1) % 6 == 0 or (idx + 1) == len(bronze_files):
            logger.info("Processed %d / %d Bronze files.", idx + 1, len(bronze_files))

    try:
        os.remove(dedup_keys_path)
    except OSError:
        pass

    logger.info("DuckDB Silver transactions written to: %s", silver_path)

    return {
        _DQ_DUPLICATE_DEDUPED: duplicate_count_total,
        _DQ_ORPHANED_ACCOUNT: orphan_count_total,
        _DQ_TYPE_MISMATCH: type_mismatch_count_total,
        _DQ_DATE_FORMAT: date_format_count_total,
        _DQ_CURRENCY_VARIANT: currency_variant_count_total,
    }


def run_silver_transactions_delta_register(
    spark: SparkSession, cfg: dict[str, Any]
) -> None:
    """Register DuckDB-written Parquet files as a Delta table."""
    silver_path = cfg["output"]["silver_path"] + "/transactions"
    logger.info("Registering Silver transactions as Delta table.")

    parquet_files = _find_parquet_files(silver_path)
    parquet_files = [f for f in parquet_files if "_delta_log" not in f]

    df = spark.read.parquet(*parquet_files)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )
    logger.info("Silver transactions Delta registration complete.")


def _transform_customers(df: DataFrame) -> DataFrame:
    logger.info("Transforming customers.")
    df = df.withColumn("dob", _parse_date_col(df["dob"]))
    return _dedup_window(df, pk="customer_id", order_col="ingestion_timestamp")


def _transform_accounts(df: DataFrame, silver_customers: DataFrame) -> tuple[DataFrame, int]:
    logger.info("Transforming accounts.")
    df = (
        df
        .withColumn("open_date", _parse_date_col(df["open_date"]))
        .withColumn("last_activity_date", _parse_date_col(df["last_activity_date"]))
        .withColumn("credit_limit", F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(DecimalType(18, 2)))
    )

    null_pk_count = df.filter(F.col("account_id").isNull()).count()
    if null_pk_count > 0:
        logger.warning(
            "Dropping %d account rows with null account_id (NULL_REQUIRED).",
            null_pk_count,
        )

    df = df.filter(F.col("account_id").isNotNull())
    df = _dedup_window(df, pk="account_id", order_col="ingestion_timestamp")

    orphaned = (
        df.join(
            F.broadcast(
                silver_customers.select(F.col("customer_id").alias("_valid_cid"))
            ),
            df["customer_ref"] == F.col("_valid_cid"),
            "left_anti",
        ).count()
    )
    if orphaned > 0:
        logger.warning(
            "%d account rows have no matching customer_id — excluded from Gold joins.",
            orphaned,
        )

    logger.info("Accounts transformed: %d rows", df.count())
    return df, null_pk_count


def _dedup_window(df: DataFrame, pk: str, order_col: str) -> DataFrame:
    """Window-based dedup — safe for small tables only."""
    w = Window.partitionBy(pk).orderBy(F.col(order_col).asc())
    return (
        df.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def _parse_date_col(col):
    parsed = F.coalesce(*[F.to_date(col, fmt) for fmt in _DATE_FORMATS])
    epoch_parsed = F.to_date(F.from_unixtime(col.cast("long")), "yyyy-MM-dd")
    return F.coalesce(parsed, epoch_parsed)


def _write_delta_spark(df: DataFrame, path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def _find_parquet_files(delta_path: str) -> list[str]:
    return glob.glob(os.path.join(delta_path, "**", "*.parquet"), recursive=True)