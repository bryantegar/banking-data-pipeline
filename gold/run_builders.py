"""
run_builders.py

Runs every Gold builder and writes each result to output/gold/<table>.csv.
This is the "build" step -- separate from postgres_loader.py's "load" step
(Single Responsibility: a bug in one can't corrupt the other).

Usage: python -m gold.run_builders
       (or: docker compose exec airflow-scheduler python -m gold.run_builders)
"""

from gold.concrete_builders import (
    DimBranchBuilder,
    DimCustomerBuilder,
    DimTimeBuilder,
    FactLoanBuilder,
    FactAccountBuilder,
    CustomerMonthlyTrendBuilder,
    LoanCohortTrendBuilder,
    PortfolioSnapshotBuilder,
)

BUILDERS = [
    DimBranchBuilder,
    DimCustomerBuilder,
    DimTimeBuilder,
    FactLoanBuilder,
    FactAccountBuilder,
    CustomerMonthlyTrendBuilder,
    LoanCohortTrendBuilder,
    PortfolioSnapshotBuilder,
]


def main() -> None:
    for builder_cls in BUILDERS:
        result = builder_cls().run()
        print(f"[{builder_cls.__name__}] {result}")


if __name__ == "__main__":
    main()