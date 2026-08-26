"""
consumer.py

Subscribes to "bank.transactions.raw", scores every transaction through
FraudRuleEngine AND NetworkQualityGate (two independent concerns -- see
network_quality.py), and lands results in two places -- the same
batch/speed-layer split a real bank's architecture would use:

- MinIO (bucket "bronze", prefix "transactions/") as Parquet -- the lake,
  full raw+scored history, same pattern as Bronze customers/accounts/loans.
- Postgres ("fact_transaction_stream" table) -- the fast serving path a
  live dashboard queries, so a fraud analyst sees flagged transactions
  within seconds, not after the next hourly Airflow batch.

Fraud thresholds are CALIBRATED AT STARTUP, not hardcoded: this consumer
generates a small sample from the same TransactionGenerator the producer
uses (same account pool, same amount distribution) and derives
large_amount_threshold/median_amount from that sample's own percentiles.
Earlier versions hardcoded numbers computed from a one-off historical CSV
-- those went silently stale the moment the generator's amount scale
changed (exactly what caused ~41% of traffic to get flagged as
"suspicious": a threshold tuned for hundreds-of-dollars amounts, compared
against a generator now producing hundreds-of-thousands-of-Rupiah
amounts). Self-calibrating from the live generator's own output means
this can never drift out of sync with it again.

All external connections (Kafka, MinIO, Postgres) resolve their address
from environment variables, defaulting to host-mapped ports for running
manually from WSL. When running as the Dockerized "consumer" service,
docker-compose overrides these to internal Docker network addresses --
same host-vs-container distinction hit repeatedly elsewhere in this
project (MinioClient, WarehouseConfig).

Run from WSL:
    python -m streaming.consumer
"""

import json
import os
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine

from gold.postgres_loader import WarehouseConfig
from storage.minio_client import MinioClient
from streaming.account_pool import load_account_pool
from streaming.fraud_rules import FraudRuleEngine
from streaming.network_quality import NetworkQualityGate
from data_generator.transaction_generator import TransactionGenerator

TOPIC = "bank.transactions.raw"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
BRONZE_BUCKET = "bronze"
FLUSH_EVERY_N = 25
CALIBRATION_SAMPLE_SIZE = 500


def _calibrate_thresholds(account_pool: list) -> tuple:
    """Generates a same-distribution sample to derive fraud thresholds
    from, instead of hardcoding numbers that go stale whenever the
    generator's amount logic changes."""
    sample_gen = TransactionGenerator(account_ids=account_pool)
    sample_txns = sample_gen.generate_many(CALIBRATION_SAMPLE_SIZE)
    amounts = pd.Series([t["amount"] for t in sample_txns])
    large_amount_threshold = float(amounts.quantile(0.75))
    median_amount = float(amounts.median())
    return large_amount_threshold, median_amount


def main() -> None:
    # enable_auto_commit=False is the key production-grade fix here: with
    # Kafka's default auto-commit, an offset gets marked "done" the moment
    # it's read off the topic -- BEFORE this code has actually written it
    # anywhere. Committing manually, only after a batch's MinIO+Postgres
    # writes both succeed, means a crash mid-batch causes that batch to be
    # RE-READ on restart instead of silently skipped ("at-least-once"
    # processing).
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="fraud-scoring-consumer",
        enable_auto_commit=False,
    )

    account_pool = load_account_pool()
    large_amount_threshold, median_amount = _calibrate_thresholds(account_pool)
    print(f"[consumer] calibrated thresholds from {CALIBRATION_SAMPLE_SIZE} sample txns: "
          f"large_amount_threshold={large_amount_threshold:.2f} median_amount={median_amount:.2f}")

    fraud_engine = FraudRuleEngine(
        large_amount_threshold=large_amount_threshold, median_amount=median_amount
    )
    network_gate = NetworkQualityGate()
    minio = MinioClient()
    db_engine = create_engine(WarehouseConfig().dsn)

    buffer = []
    total_scored = 0
    total_flagged = 0
    total_slow_network = 0

    print("[consumer] listening on", TOPIC, "...")
    for message in consumer:
        txn = message.value
        txn["timestamp"] = pd.to_datetime(txn["timestamp"])

        fraud_result = fraud_engine.score(txn)
        txn["risk_score"] = fraud_result.risk_score
        txn["risk_reasons"] = ",".join(fraud_result.reasons) if fraud_result.reasons else None
        txn["is_suspicious"] = fraud_result.risk_score >= 2

        network_result = network_gate.evaluate(
            latency_ms=txn.get("latency_ms"), bandwidth_mbps=txn.get("slice_bandwidth_mbps")
        )
        txn["network_quality"] = network_result.quality
        txn["network_check_passed"] = network_result.check_passed
        txn["network_reasons"] = ",".join(network_result.reasons) if network_result.reasons else None

        txn["scored_at"] = datetime.now(timezone.utc)

        buffer.append(txn)
        total_scored += 1
        if txn["is_suspicious"]:
            total_flagged += 1
            print(f"[consumer] SUSPICIOUS score={fraud_result.risk_score} reasons={fraud_result.reasons} "
                  f"sender={txn['sender_account_id']} amount={txn['amount']}")
        if not network_result.check_passed:
            total_slow_network += 1
            print(f"[consumer] SLOW NETWORK detected quality={network_result.quality} "
                  f"reasons={network_result.reasons} transaction_id={txn['transaction_id']} "
                  f"(logged for analysis, transaction still recorded normally)")

        if len(buffer) >= FLUSH_EVERY_N:
            _flush(buffer, minio, db_engine)
            consumer.commit()
            print(f"[consumer] flushed {len(buffer)} txns "
                  f"(total scored={total_scored}, flagged={total_flagged}, "
                  f"slow_network={total_slow_network})")
            buffer = []


def _flush(buffer: list, minio: MinioClient, db_engine) -> None:
    df = pd.DataFrame(buffer)
    for col in ["transaction_id", "sender_account_id", "receiver_account_id"]:
        df[col] = df[col].astype(str)

    batch_key = f"transactions/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
    minio.write_parquet(df, bucket=BRONZE_BUCKET, key=batch_key)

    df_pg = df.copy()
    df_pg["timestamp"] = df_pg["timestamp"].astype(str)
    df_pg["scored_at"] = df_pg["scored_at"].astype(str)
    df_pg.to_sql("fact_transaction_stream", db_engine, if_exists="append", index=False)


if __name__ == "__main__":
    main()
