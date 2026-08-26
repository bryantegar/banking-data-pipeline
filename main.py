"""
Orchestrator for the Bronze-layer synthetic data generation.

Run: python main.py

Produces CSVs under output/bronze/ that simulate the raw daily export
from a core banking system: branches, customers, accounts, transactions,
and loans -- complete with created_at / updated_at / deleted_at columns
so a downstream Airflow DAG can do incremental (watermark-based) extraction.
"""

import os

from data_generator.branch_generator import BranchGenerator
from data_generator.customer_generator import CustomerGenerator
from data_generator.account_generator import AccountGenerator
from data_generator.transaction_generator import TransactionGenerator
from data_generator.loan_generator import LoanGenerator

OUTPUT_DIR = "output/bronze"
SEED = 42  # fixed seed -> reproducible dataset across runs

VOLUMES = {
    "branches": 20,
    "customers": 5000,
    "accounts": 7000,
    "transactions": 100_000,
    "loans": 1500,
}


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    branch_gen = BranchGenerator(seed=SEED)
    branch_gen.generate_many(VOLUMES["branches"])
    branch_gen.save_csv(f"{OUTPUT_DIR}/branches.csv")
    branch_ids = [r["branch_id"] for r in branch_gen._records]

    customer_gen = CustomerGenerator(seed=SEED)
    customer_gen.generate_many(VOLUMES["customers"])
    customer_gen.save_csv(f"{OUTPUT_DIR}/customers.csv")
    customer_ids = [r["customer_id"] for r in customer_gen._records]

    account_gen = AccountGenerator(customer_ids=customer_ids, branch_ids=branch_ids, seed=SEED)
    account_gen.generate_many(VOLUMES["accounts"])
    account_gen.save_csv(f"{OUTPUT_DIR}/accounts.csv")
    account_ids = [r["account_id"] for r in account_gen._records]

    transaction_gen = TransactionGenerator(account_ids=account_ids, seed=SEED)
    transaction_gen.generate_many(VOLUMES["transactions"])
    transaction_gen.save_csv(f"{OUTPUT_DIR}/transactions.csv")

    loan_gen = LoanGenerator(customer_ids=customer_ids, branch_ids=branch_ids, seed=SEED)
    loan_gen.generate_many(VOLUMES["loans"])
    loan_gen.save_csv(f"{OUTPUT_DIR}/loans.csv")

    print("\nBronze layer data generation complete.")


if __name__ == "__main__":
    main()
