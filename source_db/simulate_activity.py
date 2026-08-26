"""
simulate_activity.py

Simulates ongoing "bank activity" hitting the source (OLTP) system:
new customers signing up, existing customers updating their data, new
accounts being opened, existing accounts changing status, new loans being
disbursed, and existing loans being paid off.

Volumes here are deliberately small (tens per run, not thousands) --
this runs every 15 minutes via Airflow, so even modest per-run volumes
compound fast. The original defaults (1500 new customers per run) were
sized for an initial demo burst, but running continuously for days
badly skews any month-over-month trend toward whichever month the
pipeline happened to be left running in. These defaults are sized to
roughly match the historical seed's per-month scale (tens of new
customers/loans a month), so the current month stays comparable to
past months in monthly-trend charts instead of dwarfing them.
"""

import random
import sqlite3
from datetime import datetime, date

import pandas as pd

from data_generator.customer_generator import CustomerGenerator
from data_generator.account_generator import AccountGenerator
from data_generator.loan_generator import LoanGenerator

DB_PATH = "source_db/core_banking.db"


def _write_df(conn: sqlite3.Connection, df: pd.DataFrame, table: str) -> None:
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].apply(
            lambda v: isinstance(v, (pd.Timestamp, datetime, date))
        ).any():
            df[col] = df[col].astype(str).replace({"None": None, "NaT": None, "nan": None})
    df.to_sql(table, conn, if_exists="replace", index=False)


def _simulate_loan_activity(
    conn: sqlite3.Connection,
    customer_ids: list[int],
    branch_ids: list[int],
    now: str,
    n_new_loans: int = 2,
    n_loan_closures: int = 1,
) -> dict:
    """New loan disbursements, plus a portion of currently on-time loans
    getting paid off in full.

    Only `status == "current"` loans are eligible for closure -- a
    delinquent loan (watchlist/NPL) doesn't just quietly disappear; it
    stays open until resolved through collections, which this simulation
    doesn't model (yet).
    """
    loans = pd.read_sql("SELECT * FROM loans", conn)

    open_current = loans[loans["status"] == "current"]
    close_ids = random.sample(
        list(open_current["loan_id"]), k=min(n_loan_closures, len(open_current))
    )
    close_mask = loans["loan_id"].isin(close_ids)
    # Assigning to a column that doesn't exist yet on `loans` (older rows
    # predate `closed_date`) creates it automatically, NaN everywhere
    # outside this mask -- no manual schema migration needed.
    loans.loc[close_mask, "status"] = "lunas"
    loans.loc[close_mask, "closed_date"] = now
    loans.loc[close_mask, "updated_at"] = now

    next_loan_id = int(loans["loan_id"].max()) + 1
    loan_gen = LoanGenerator(
        customer_ids=customer_ids, branch_ids=branch_ids, seed=random.randint(0, 10_000_000)
    )
    new_loans = pd.DataFrame(loan_gen.generate_many(n_new_loans))
    new_loans["loan_id"] = range(next_loan_id, next_loan_id + n_new_loans)
    new_loans["created_at"] = now
    new_loans["updated_at"] = now
    # A loan disbursed in *this* run hasn't existed long enough to be
    # delinquent or repaid -- force a clean "current" state regardless of
    # what LoanGenerator's own dpd/closed-probability logic produced,
    # which is meant for backdated historical loans, not brand-new ones.
    new_loans["dpd"] = 0
    new_loans["status"] = "current"
    new_loans["closed_date"] = None

    loans = pd.concat([loans, new_loans], ignore_index=True)
    _write_df(conn, loans, "loans")

    return {"loans_closed": len(close_ids), "loans_new": n_new_loans}


def simulate_activity(
    n_new_customers: int = 15,
    n_customer_updates: int = 25,
    n_new_accounts: int = 8,
    n_account_updates: int = 12,
    n_new_loans: int = 2,
    n_loan_closures: int = 1,
) -> dict:
    conn = sqlite3.connect(DB_PATH)
    now = datetime.utcnow().isoformat()
    summary = {}

    customers = pd.read_sql("SELECT * FROM customers", conn)
    update_ids = random.sample(
        list(customers["customer_id"]), k=min(n_customer_updates, len(customers))
    )
    customers.loc[customers["customer_id"].isin(update_ids), "updated_at"] = now

    next_customer_id = int(customers["customer_id"].max()) + 1
    new_customer_gen = CustomerGenerator(seed=random.randint(0, 10_000_000))
    new_customers = pd.DataFrame(new_customer_gen.generate_many(n_new_customers))
    new_customers["customer_id"] = range(next_customer_id, next_customer_id + n_new_customers)
    new_customers["created_at"] = now
    new_customers["updated_at"] = now

    customers = pd.concat([customers, new_customers], ignore_index=True)
    _write_df(conn, customers, "customers")
    summary["customers_updated"] = len(update_ids)
    summary["customers_new"] = n_new_customers

    accounts = pd.read_sql("SELECT * FROM accounts", conn)
    branches = pd.read_sql("SELECT branch_id FROM branches", conn)

    update_acc_ids = random.sample(
        list(accounts["account_id"]), k=min(n_account_updates, len(accounts))
    )
    accounts.loc[accounts["account_id"].isin(update_acc_ids), "updated_at"] = now

    next_account_id = int(accounts["account_id"].max()) + 1
    account_gen = AccountGenerator(
        customer_ids=customers["customer_id"].tolist(),
        branch_ids=branches["branch_id"].tolist(),
        seed=random.randint(0, 10_000_000),
    )
    new_accounts = pd.DataFrame(account_gen.generate_many(n_new_accounts))
    new_accounts["account_id"] = range(next_account_id, next_account_id + n_new_accounts)
    new_accounts["created_at"] = now
    new_accounts["updated_at"] = now

    accounts = pd.concat([accounts, new_accounts], ignore_index=True)
    _write_df(conn, accounts, "accounts")
    summary["accounts_updated"] = len(update_acc_ids)
    summary["accounts_new"] = n_new_accounts

    loan_summary = _simulate_loan_activity(
        conn,
        customer_ids=customers["customer_id"].tolist(),
        branch_ids=branches["branch_id"].tolist(),
        now=now,
        n_new_loans=n_new_loans,
        n_loan_closures=n_loan_closures,
    )
    summary.update(loan_summary)

    conn.close()
    return summary


if __name__ == "__main__":
    print(simulate_activity())