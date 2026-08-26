"""
backfill_loans.py

One-off fix: the original `loans` table was seeded before LoanGenerator
supported historical "lunas" closures (closed_date spread across the
disbursement window). Every "lunas" loan since then came only from
simulate_activity.py's ongoing closures, which are correctly dated "now"
-- so all closures piled up in the current month, with 0 in every earlier
month. This regenerates ONLY the loans table with the current generator
(which does produce historically-spread closures), leaving customers/
accounts (and their days of accumulated growth) untouched.

Run once: python -m source_db.backfill_loans
"""

import sqlite3

import pandas as pd

from data_generator.loan_generator import LoanGenerator

DB_PATH = "source_db/core_banking.db"
N_LOANS = 1500


def main() -> None:
    conn = sqlite3.connect(DB_PATH)

    customer_ids = pd.read_sql("SELECT customer_id FROM customers", conn)["customer_id"].tolist()
    branch_ids = pd.read_sql("SELECT branch_id FROM branches", conn)["branch_id"].tolist()

    loan_gen = LoanGenerator(customer_ids=customer_ids, branch_ids=branch_ids, seed=42)
    loans = pd.DataFrame(loan_gen.generate_many(N_LOANS))

    # Match _write_df's datetime -> string normalization used elsewhere,
    # so this table stays consistent with how every other writer saves it.
    for col in loans.columns:
        if pd.api.types.is_datetime64_any_dtype(loans[col]) or loans[col].apply(
            lambda v: hasattr(v, "isoformat")
        ).any():
            loans[col] = loans[col].astype(str).replace({"None": None, "NaT": None, "nan": None})

    loans.to_sql("loans", conn, if_exists="replace", index=False)
    conn.close()

    n_lunas = (loans["status"] == "lunas").sum()
    print(f"Regenerated {len(loans)} loans -- {n_lunas} historically closed (status='lunas')")


if __name__ == "__main__":
    main()