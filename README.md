# Core Banking Data Platform

An end-to-end data platform simulating a bank's data infrastructure — built to demonstrate production-grade data engineering practices for a banking/financial-services context: data lake, data warehouse, dimensional modeling, batch + streaming ETL/ELT, data quality enforcement, and a rule-based fraud detection engine.

This is a portfolio project, not a production system — but every component is built and tested the way a production system would be: incremental extraction with watermarks, PII masking, data quality gates with quarantine, orchestrated pipelines with automatic chaining, and a live streaming fraud pipeline running independently of the batch pipeline.

## Why this project

Most portfolio ETL projects stop at "scrape data → clean it → put it in a database." This one is built around the specific things a bank's data platform actually has to solve:

- Master/reference data (customers, accounts, branches, loans) changes slowly and is extracted in **batch**
- Transaction activity is high-volume and needs to be caught **in near-real-time**, not the next day
- Customer PII (national ID, name) can never leave the raw layer unmasked
- Bad data (broken foreign keys, invalid values) has to be caught and quarantined, not silently propagated into reports
- "How did the numbers change over time" needs an honest answer — not every metric can be reconstructed retroactively from a single current-state snapshot, and this project is explicit about which of its trend numbers are true historical reconstructions vs. which are same-day cohort views

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  BATCH SIDE — master/reference data (branches, customers, accounts,  │
│  loans), extracted hourly                                            │
└─────────────────────────────────────────────────────────────────────┘

  source_activity_simulation (Airflow, every 15 min)
       │  simulates a live core-banking system: new customers,
       │  account changes, loan disbursements/closures
       ▼
  source_db/core_banking.db  (SQLite — stands in for the bank's OLTP DB)
       │
       │  bronze_batch_extraction (Airflow, hourly)
       │  watermark-based incremental extraction
       │  (WHERE updated_at > last_watermark)
       ▼
  MinIO — bucket "bronze"          (Parquet, partitioned by extraction run)
       │
       │  silver_batch_transformation (Airflow, triggered by Bronze)
       │  clean → data-quality checks → quarantine bad rows →
       │  mask PII (NIK hashed, name partially masked) →
       │  register keys for downstream referential-integrity checks
       ▼
  MinIO — bucket "silver"          (Parquet, cleaned + masked)
       │
       │  gold_batch_transformation (Airflow, triggered by Silver)
       │  dimensional modeling + trend/cohort analysis
       ▼
  PostgreSQL — "banking_warehouse" (star schema + analytical tables)
       │
       ▼
  Power BI  (Risk/NPL, Customer Segmentation, Trends dashboards)


┌─────────────────────────────────────────────────────────────────────┐
│  STREAMING SIDE — transaction activity, processed continuously       │
└─────────────────────────────────────────────────────────────────────┘

  Kafka producer (Docker service, runs forever)
       │  generates live transactions referencing real accounts
       │  from source_db; periodically injects labeled synthetic
       │  fraud scenarios (velocity burst, impossible travel,
       │  fan-in/collector pattern, structuring)
       ▼
  Kafka topic "bank.transactions.raw"
       │
       │  Kafka consumer (Docker service, runs forever)
       │  rule-based fraud scoring (self-calibrating thresholds)
       │  + independent network-quality gate
       │  at-least-once processing (manual offset commit after
       │  a batch's writes succeed, not before)
       ▼
  MinIO "bronze/transactions/" (lake)  +  Postgres "fact_transaction_stream" (serving)
       │
       ▼
  Power BI  (Fraud Monitoring dashboard)
```

## Tech stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9 (Docker) |
| Data lake | MinIO (S3-compatible object storage) |
| Data warehouse | PostgreSQL |
| Streaming | Apache Kafka (KRaft mode, no Zookeeper) |
| Transformation | Python, pandas |
| Visualization | Power BI Desktop |
| Infra | Docker Compose |

## What each layer actually does

### Data generation
`data_generator/` — OOP generators (`BranchGenerator`, `CustomerGenerator`, `AccountGenerator`, `LoanGenerator`, `TransactionGenerator`), all extending a shared `BaseGenerator` (Template Method pattern). Produces 2 years of realistic historical seed data, plus feeds both the batch simulation and the live Kafka stream.

### Bronze (data lake, raw)
`extractors/` — `BaseExtractor` (abstract) + one concrete extractor per table. Incremental extraction via watermark (`watermark/watermark_store.py`, SQLite-backed): each run only pulls rows where `updated_at` is newer than the last successful extraction, not a full reload every time.

### Silver (data lake, cleaned)
`silver/` — `BaseSilverTransformer` (abstract) + one concrete transformer per table.
- **Data quality** (`silver/data_quality.py`): not-null, no-duplicates, referential integrity (validated against an accumulated `ReferenceRegistry`, not just the current batch), value-range checks. Rows that fail are **dropped from Silver and logged** to a JSON audit report — quarantine happens in-memory before anything is written, not after.
- **PII masking** (`silver/pii_masking.py`): NIK is SHA-256 hashed with a salt (deterministic, so joins still work); names are partially masked (`"Ika Prasasta" → "Ika P."`).

### Gold (data warehouse)
`gold/` — dimensional model: `dim_branch`, `dim_customer`, `dim_time`, `fact_loan`, `fact_account`, plus three analytical tables:
- `customer_monthly_trend` — **true historical** monthly active/new/churned customer counts, reconstructed exactly from each customer's `created_at`/`deleted_at` (not estimated).
- `loan_cohort_trend` — current NPL rate grouped by loan disbursement month (a cohort/vintage view — deliberately **not** claimed to be a historical trend, since loan status only ever reflects today's value).
- `portfolio_snapshot_history` — today's portfolio totals, upserted by date. This is what accumulates into a *true* day-over-day NPL trend over time, as the pipeline keeps running.

### Streaming + fraud detection
`streaming/`:
- `fraud_rules.py` — `FraudRuleEngine`, 6 explainable rules (large amount, failed+high-value, velocity burst, impossible travel, fan-in/collector pattern — the signature of judol/pinjol proceeds "mule" accounts, and structuring). Thresholds are **calibrated at startup** from a live sample, not hardcoded, so they never go stale when the transaction-amount distribution changes.
- `network_quality.py` — an independent gate (bad network ≠ fraud) using latency/bandwidth thresholds derived from the reference dataset's own percentiles.
- `scenario_injector.py` — generates labeled synthetic fraud bursts (tagged `is_synthetic_scenario=True`) using real account IDs from the pool, since organic one-off transactions rarely produce the repeated-sender patterns these rules need to demonstrate live.
- `producer.py` / `consumer.py` — run as their own Docker services (`restart: unless-stopped`), independent of Airflow, since unbounded streaming doesn't fit a batch scheduler's model.

## Honesty notes (things worth saying out loud in an interview)

- **`loan_cohort_trend` is not a historical trend.** `dpd`/`status` only ever reflect the current value; grouping by disbursement month tells you which cohorts are risky *right now*, not what NPL looked like in the past. True historical portfolio health needs periodic snapshots (`portfolio_snapshot_history`), which only becomes a meaningful trend after the pipeline has run for a while.
- **The reference dataset's own "Fraud Flag" column doesn't correlate with anything observable** (amount, status, device are all ~50/50 regardless of flag). The fraud rules here are independent, explainable heuristics — not an attempt to reproduce that column.
- **`source_activity_simulation` and the Kafka producer both generate synthetic volume continuously.** For a stable demo, pause these DAGs/services before taking screenshots, otherwise the numbers keep moving.

## Running it locally

Requires Docker Desktop (with WSL2 backend on Windows) and Python 3.12+.

```bash
git clone <this-repo>
cd banking-data-pipeline
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
pip install -r requirements-streaming.txt   # kafka-python, sqlalchemy, dll -- khusus buat streaming/

docker compose up -d
```

This starts Airflow (webserver on `localhost:8080`), MinIO (console on `localhost:9001`), PostgreSQL warehouse (`localhost:5433`), Kafka, and the `producer`/`consumer` streaming services.

In the Airflow UI, unpause `source_activity_simulation` and trigger `bronze_batch_extraction` — it chains automatically into Silver and Gold. Then run:

```bash
python -m gold.run_builders
python gold/postgres_loader.py
```

to load the Gold tables into Postgres, and open the Power BI file, pointing its PostgreSQL connection at `localhost:5433` / `banking_warehouse`.

## Dashboards

_(screenshots go here — Risk/NPL, Customer Segmentation, Trends, Fraud Monitoring)_

## What I'd build next

- Move `portfolio_snapshot_history` from a single daily row to a proper periodic-snapshot fact table with more granular metrics
- Add a lightweight alerting hook (Slack/email) when the consumer flags a `risk_score >= 2` transaction
- Replace the manually-run `run_builders.py` / `postgres_loader.py` step with a final Airflow task in the Gold DAG, so the whole batch side loads to the warehouse with zero manual steps
