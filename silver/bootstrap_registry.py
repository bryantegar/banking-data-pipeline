"""
bootstrap_registry.py

One-time backfill for the ReferenceRegistry, needed because Silver is being
introduced after Bronze has already been running for a while. Without this,
the registry starts empty and the very first Silver run would flag valid
foreign keys (customer_id, branch_id created on earlier days) as violations,
since referential_integrity() only checks against what's already registered.

Two backfill sources are supported:

1. Source OLTP DB (preferred, `--from-source-db`): reads DISTINCT primary
   keys straight from source_db/core_banking.db. Authoritative and doesn't
   depend on which Bronze partition files still happen to be on disk.
2. Bronze partition glob (fallback, default): scans every historical Bronze
   CSV per table. Works without direct DB access, but is only as complete
   as the retained partition history -- if older daily files have been
   rotated away, some legitimately valid IDs may still be missed.

Safe to re-run either way: registration is idempotent (INSERT OR IGNORE).

Usage:
    python -m silver.bootstrap_registry                  # from Bronze files
    python -m silver.bootstrap_registry --from-source-db  # from source DB
"""

import argparse
import glob
import os
import sqlite3
from typing import Optional

import pandas as pd

from silver.reference_registry import ReferenceRegistry

BRONZE_DIR = "output/bronze"
SOURCE_DB_PATH = "source_db/core_banking.db"

# Only parent tables that other tables have foreign keys into need
# backfilling -- accounts/loans reference customers and branches, but
# nothing references accounts or loans (yet).
TABLES_TO_BACKFILL = {
    "branches": "branch_id",
    "customers": "customer_id",
}


def _bootstrap_from_bronze(registry: ReferenceRegistry) -> None:
    for table, primary_key in TABLES_TO_BACKFILL.items():
        partition_files = sorted(glob.glob(os.path.join(BRONZE_DIR, table, "*.csv")))
        if not partition_files:
            print(f"[{table}] no Bronze partitions found, skipping")
            continue

        total_read = 0
        for path in partition_files:
            df = pd.read_csv(path, usecols=[primary_key])
            registry.register_ids(table, df[primary_key].tolist())
            total_read += len(df)

        print(
            f"[{table}] scanned {len(partition_files)} Bronze partition(s), "
            f"{total_read} rows read, {len(registry.known_ids(table))} distinct IDs registered"
        )


def _bootstrap_from_source_db(registry: ReferenceRegistry, source_db_path: str) -> None:
    if not os.path.exists(source_db_path):
        raise FileNotFoundError(
            f"Source DB not found at {source_db_path!r} -- fall back to "
            "the Bronze-glob mode instead (omit --from-source-db)."
        )
    conn = sqlite3.connect(source_db_path)
    try:
        for table, primary_key in TABLES_TO_BACKFILL.items():
            ids = pd.read_sql(f"SELECT DISTINCT {primary_key} FROM {table}", conn)[primary_key].tolist()
            registry.register_ids(table, ids)
            print(f"[{table}] read {len(ids)} distinct IDs directly from {source_db_path}")
    finally:
        conn.close()


def bootstrap(registry: Optional[ReferenceRegistry] = None, from_source_db: bool = False) -> None:
    registry = registry or ReferenceRegistry()
    if from_source_db:
        _bootstrap_from_source_db(registry, SOURCE_DB_PATH)
    else:
        _bootstrap_from_bronze(registry)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-source-db",
        action="store_true",
        help="Backfill from source_db/core_banking.db instead of scanning Bronze partition files.",
    )
    args = parser.parse_args()
    bootstrap(from_source_db=args.from_source_db)