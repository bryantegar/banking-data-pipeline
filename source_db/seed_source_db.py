"""
seed_source_db.py

Populates a SQLite database that plays the role of the bank's operational
(OLTP) database -- the thing an Airflow DAG would normally connect to via
a DB hook to pull data from.

Run this once for the initial dataset, then run it again with
`--simulate-daily-changes` to mimic a day passing: some existing rows get
updated (their updated_at moves forward) and a few new rows get inserted.
That's what gives the incremental extractor something real to detect.
"""

import argparse
import random
import sqlite3
from datetime import date, datetime

import pandas as pd

from data_generator.branch_generator import BranchGenerator
from data_generator.customer_generator import CustomerGenerator
from data_generator.account_generator import AccountGenerator
from data_generator.loan_generator import LoanGenerator

DB_PATH = "source_db/core_banking.db"


def _write_df(conn: sqlite3.Connection, df: pd.DataFrame, table: str) -> None:
    # sqlite3 chokes on pandas Timestamp / mixed date objects -> normalize
    # any datetime-like column to plain ISO strings before writing.
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].apply(
            lambda v: isinstance(v, (pd.Timestamp, datetime, date))
        ).any():
            df[col] = df[col].astype(str).replace({"None": None, "NaT": None, "nan": None})
    df.to_sql(table, conn, if_exists="replace", index=False)


def initial_load() -> None:
    conn = sqlite3.connect(DB_PATH)

    branch_gen = BranchGenerator(seed=42)
    branch_gen.generate_many(20)
    branches = branch_gen.to_dataframe()
    _write_df(conn, branches, "branches")

    customer_gen = CustomerGenerator(seed=42)
    customer_gen.generate_many(5000)
    customers = customer_gen.to_dataframe()
    _write_df(conn, customers, "customers")

    account_gen = AccountGenerator(
        customer_ids=customers["customer_id"].tolist(),
        branch_ids=branches["branch_id"].tolist(),
        seed=42,
    )
    account_gen.generate_many(7000)
    accounts = account_gen.to_dataframe()
    _write_df(conn, accounts, "accounts")

    loan_gen = LoanGenerator(
        customer_ids=customers["customer_id"].tolist(),
        branch_ids=branches["branch_id"].tolist(),
        seed=42,
    )
    loan_gen.generate_many(1500)
    loans = loan_gen.to_dataframe()
    _write_df(conn, loans, "loans")

    conn.close()
    print("Initial load complete -> source_db/core_banking.db")


def simulate_daily_changes(n_updates: int = 150, n_new_customers: int = 30) -> None:
    """Mutate a slice of existing rows (updated_at -> now) and insert a
    handful of brand-new customers, so the next DAG run has real deltas
    to pick up via the watermark."""
    conn = sqlite3.connect(DB_PATH)
    now = datetime.utcnow().isoformat()

    customers = pd.read_sql("SELECT * FROM customers", conn)
    update_ids = random.sample(list(customers["customer_id"]), k=min(n_updates, len(customers)))
    customers.loc[customers["customer_id"].isin(update_ids), "updated_at"] = now
    _write_df(conn, customers, "customers")

    next_id = int(customers["customer_id"].max()) + 1
    new_customer_gen = CustomerGenerator(seed=random.randint(0, 100_000))
    new_customers = new_customer_gen.generate_many(n_new_customers)
    new_df = pd.DataFrame(new_customers)
    new_df["customer_id"] = range(next_id, next_id + n_new_customers)
    new_df["created_at"] = now
    new_df["updated_at"] = now
    combined = pd.concat([customers, new_df], ignore_index=True)
    _write_df(conn, combined, "customers")

    conn.close()
    print(f"Simulated {n_updates} updates + {n_new_customers} new customers")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--simulate-daily-changes", action="store_true")
    args = parser.parse_args()

    if args.simulate_daily_changes:
        simulate_daily_changes()
    else:
        initial_load()
