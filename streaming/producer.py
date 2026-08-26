"""
producer.py

Generates a live, UNBOUNDED stream of synthetic transactions -- not a
replay of a fixed historical file. Every transaction references a real
account_id currently in source_db (the same SQLite "core banking" source
extractors.base_extractor reads from), so this reads as "customers
already in the system transacting right now", consistent with the rest
of the pipeline.

This is the streaming counterpart to source_db/simulate_activity.py:
that module simulates low-frequency events (new accounts, loans opening/
closing) picked up by hourly Airflow batch; this simulates high-frequency
events (customers actually using their accounts) picked up by Kafka in
near-real-time instead.

The account pool is reloaded from source_db every ACCOUNT_POOL_REFRESH_SECONDS
(wall-clock time, not transaction count -- so the refresh cadence stays
correct regardless of TXN_INTERVAL_SECONDS), since simulate_activity.py
keeps adding new accounts over time and a pool frozen at startup would
silently go stale.

Every INJECT_EVERY_N_ROWS transactions, injects one labeled synthetic
fraud scenario (see scenario_injector.py) so the consumer's rule engine
has repeated-sender/receiver patterns to actually catch during a live
demo.

Both KAFKA_BOOTSTRAP_SERVERS and the two timing variables below resolve
from environment variables, so docker-compose can override them for the
Dockerized "producer" service without touching this file:
    TXN_INTERVAL_SECONDS         -- pause between each generated transaction
    ACCOUNT_POOL_REFRESH_SECONDS -- how often to re-read source_db for new accounts

Run from WSL:
    python -m streaming.producer
"""

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

from data_generator.transaction_generator import TransactionGenerator
from streaming.account_pool import load_account_pool
from streaming.scenario_injector import random_scenario

TOPIC = "bank.transactions.raw"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TXN_INTERVAL_SECONDS = float(os.environ.get("TXN_INTERVAL_SECONDS", "0.5"))
ACCOUNT_POOL_REFRESH_SECONDS = float(os.environ.get("ACCOUNT_POOL_REFRESH_SECONDS", "300"))
INJECT_EVERY_N_ROWS = 40


def _serialize(txn: dict) -> bytes:
    payload = dict(txn)
    payload.setdefault("timestamp", datetime.now(timezone.utc))
    payload.setdefault("is_synthetic_scenario", False)
    payload.setdefault("scenario_type", None)
    if isinstance(payload["timestamp"], (pd.Timestamp, datetime)):
        payload["timestamp"] = payload["timestamp"].isoformat()
    return json.dumps(payload).encode("utf-8")


def main() -> None:
    account_pool = load_account_pool()
    print(f"[producer] loaded {len(account_pool)} accounts from source_db")
    print(f"[producer] TXN_INTERVAL_SECONDS={TXN_INTERVAL_SECONDS} "
          f"ACCOUNT_POOL_REFRESH_SECONDS={ACCOUNT_POOL_REFRESH_SECONDS}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v,  # we pre-serialize to bytes ourselves
    )

    txn_gen = TransactionGenerator(account_ids=account_pool)
    sent = 0
    last_pool_refresh = time.monotonic()

    while True:
        if (time.monotonic() - last_pool_refresh) >= ACCOUNT_POOL_REFRESH_SECONDS:
            account_pool = load_account_pool()
            txn_gen = TransactionGenerator(account_ids=account_pool)
            last_pool_refresh = time.monotonic()
            print(f"[producer] refreshed account pool -- now {len(account_pool)} accounts")

        txn = txn_gen.generate_one(sent)
        producer.send(TOPIC, _serialize(txn))
        sent += 1

        if sent % INJECT_EVERY_N_ROWS == 0:
            scenario_txns = random_scenario(account_pool, datetime.now(timezone.utc))
            for s_txn in scenario_txns:
                producer.send(TOPIC, _serialize(s_txn))
            print(f"[producer] sent {sent} transactions, injected scenario "
                  f"'{scenario_txns[0]['scenario_type']}' ({len(scenario_txns)} txns)")

        time.sleep(TXN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
