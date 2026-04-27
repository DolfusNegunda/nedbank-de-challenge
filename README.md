# Nedbank DE Challenge — Medallion Pipeline

> **AI Disclosure:** This submission was developed with AI assistance (Claude).
> Every design decision, trade-off, and line of code has been reviewed,
> understood, and is fully defensible by the author.

---

## Architecture Overview

```
accounts.csv  ──┐
transactions.jsonl ──┤  Bronze (raw + ingestion_timestamp)
customers.csv ──┘       │
                         ▼
                    Silver (typed, deduplicated, DQ-flagged)
                         │
                         ▼
              Gold: dim_customers / dim_accounts / fact_transactions
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| Single SparkSession, passed by reference | Avoids JVM startup x3; all stages share config |
| `local[2]` + `shuffle.partitions=4` | Matches 2-vCPU constraint; avoids 200-partition overhead |
| Explicit JSONL schema (no inference) | Avoids full-file scan on 1M records; handles absent `merchant_subcategory` |
| `sha2(natural_key, 256)` surrogate keys | Deterministic across re-runs; stable even if row ordering changes |
| Window-function deduplication | Deterministic ordering; no `dropDuplicates()` non-determinism |
| Currency normalisation before DQ flag | Gold always sees `"ZAR"`; flag records the source variant |
| `mode=overwrite` + `overwriteSchema=True` | Idempotent re-runs; safe in scoring environment |
| All paths from `pipeline_config.yaml` | Zero hardcoded paths; scoring system can inject overrides |
| `PYTHONPATH=/app` in Dockerfile | Ensures `from pipeline.X import` works when invoked as `python pipeline/run_all.py` |
| `stream_ingest.py` stub at Stage 1 | Required by challenge_rules.md §3.6 from Stage 1 onward |

---

## Repository Structure

```
├── Dockerfile                   # Extends nedbank-de-challenge/base:1.0
├── requirements.txt             # Extra deps (empty — base image is sufficient)
├── pipeline/
│   ├── __init__.py
│   ├── run_all.py               # Entry point: ingest -> transform -> provision
│   ├── spark_session.py         # SparkSession factory (single instance)
│   ├── config_loader.py         # YAML config loader with env-var override
│   ├── ingest.py                # Bronze layer
│   ├── transform.py             # Silver layer
│   ├── provision.py             # Gold layer + dq_report.json
│   └── stream_ingest.py         # Stage 3 stub (required from Stage 1)
├── config/
│   ├── pipeline_config.yaml     # All paths and Spark settings
│   └── dq_rules.yaml            # DQ handling rules (Stage 2+ activated)
├── adr/
│   └── .gitkeep                 # Stage 3: add stage3_adr.md here
├── stream/
│   ├── .gitkeep
│   └── README.md
└── README.md
```

---

## Local Development

### Prerequisites
- Docker Desktop (or Docker Engine on Linux)
- Git

### Step 1 — Build the base image

```bash
# From the challenge pack root
docker build -t nedbank-de-challenge/base:1.0 \
  -f stage1/infrastructure/Dockerfile.base stage1/infrastructure/
```

### Step 2 — Prepare test data

```bash
mkdir -p /tmp/test-data/input /tmp/test-data/config /tmp/test-data/output

cp stage1/data/accounts.csv         /tmp/test-data/input/
cp stage1/data/transactions.jsonl   /tmp/test-data/input/
cp stage1/data/customers.csv        /tmp/test-data/input/
cp config/pipeline_config.yaml      /tmp/test-data/config/
```

### Step 3 — Build your submission image

```bash
docker build -t candidate-submission:latest .
```

### Step 4 — Run with scoring-equivalent constraints (Stage 1: 15 min limit)

```bash
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  candidate-submission:latest

echo "Exit code: $?"   # Must be 0
```

### Step 5 — Verify outputs

```bash
ls /tmp/test-data/output/bronze/
ls /tmp/test-data/output/silver/
ls /tmp/test-data/output/gold/
cat /tmp/test-data/output/dq_report.json
```

### Step 6 — Run the official test harness

```bash
bash stage1/infrastructure/run_tests.sh \
  --stage 1 \
  --data-dir /tmp/test-data \
  --image candidate-submission:latest
```

All 7 checks must pass before submitting.

### Step 7 — Run validation queries locally (DuckDB)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SET VARIABLE gold_path = '/tmp/test-data/output/gold';

-- Q1: 4 rows expected (CREDIT, DEBIT, FEE, REVERSAL)
SELECT transaction_type, COUNT(*) AS record_count
FROM delta_scan(getvariable('gold_path') || '/fact_transactions')
GROUP BY transaction_type ORDER BY transaction_type;

-- Q2: Must return 0
SELECT COUNT(*) AS unlinked_accounts
FROM delta_scan(getvariable('gold_path') || '/dim_accounts') a
LEFT JOIN delta_scan(getvariable('gold_path') || '/dim_customers') c
  ON a.customer_id = c.customer_id WHERE c.customer_id IS NULL;

-- Q3: 9 rows expected (one per SA province)
SELECT c.province, COUNT(DISTINCT a.account_id) AS account_count
FROM delta_scan(getvariable('gold_path') || '/dim_accounts') a
JOIN delta_scan(getvariable('gold_path') || '/dim_customers') c
  ON a.customer_id = c.customer_id
GROUP BY c.province ORDER BY c.province;
"
```

---

## Submission

```bash
git add -A
git commit -m "Stage 1 final submission"
git tag -a stage1-submission -m "Stage 1 submission"
git push origin main
git push origin stage1-submission

# Verify tag is visible to scorer
git ls-remote origin refs/tags/stage1-submission
```

---

## Resource Constraints

| Resource | Limit | How we stay within it |
|---|---|---|
| RAM | 2 GB | `local[2]`, driver=1 GB, executor=512 MB; no `.toPandas()` on large DFs |
| CPU | 2 vCPU | `local[2]`, `shuffle.partitions=4` |
| Time | **15 min (Stage 1)**, 30 min (Stage 2/3) | Single Spark session; each source file read once |
| Network | None | All deps installed at build time |
| Writable FS | `/data/output` + `/tmp` | All writes go to `/data/output`; Spark temp to `/tmp` |

---

## Stage Progression

| Stage | Tag | What changes |
|---|---|---|
| 1 | `stage1-submission` | Clean data, core pipeline |
| 2 | `stage2-submission` | DQ issues injected; `dq_rules.yaml` activated; `dq_report.json` scored |
| 3 | `stage3-submission` | Real-time stream; `stream_ingest.py` implemented; `stream_gold/` tables; `adr/stage3_adr.md` required |
