"""
gold_batch_transformation_dag.py

Builds the Gold layer: dimensional tables (dim_branch, dim_customer,
dim_time) and fact tables (fact_loan for NPL/risk analysis, fact_account
for customer segmentation) from the current Silver state.

Triggered directly by silver_batch_transformation's last task via
TriggerDagRunOperator -- same pattern as Bronze -> Silver, no schedule of
its own and no time-based coordination.

All 5 tasks run in parallel: each builder reads directly from Silver and
only needs foreign-key IDs (branch_id, customer_id), not values looked up
from another Gold table, so there's no ordering dependency between them.

Drop this file into your Airflow `dags/` folder, same as the other DAGs.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from gold.postgres_loader import PostgresLoader, WarehouseConfig

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

default_args = {
    "owner": "bryan",
    "retries": 2,
    "retry_delay": 300,
}


def _run_builder(builder_cls, **context) -> None:
    result = builder_cls().run()
    print(f"[{builder_cls.__name__}] {result}")
    context["ti"].xcom_push(key=f"{result['table']}_result", value=result)
    
def _load_to_warehouse(**context) -> None:
    # Inside the Airflow container, warehouse-db is reachable by its
    # Docker service name on the internal port -- not localhost:5433
    # (that's only for connecting from the host/WSL).
    config = WarehouseConfig(host="warehouse-db", port=5432)
    loader = PostgresLoader(config)
    loader.load_all_from_dir()


with DAG(
    dag_id="gold_batch_transformation",
    description="Build Gold dimensional/fact tables from current Silver state",
    default_args=default_args,
    schedule=None,  # triggered by silver_batch_transformation
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "banking", "batch"],
) as dag:

    build_dim_branch = PythonOperator(
        task_id="build_dim_branch",
        python_callable=_run_builder,
        op_kwargs={"builder_cls": DimBranchBuilder},
    )

    build_dim_customer = PythonOperator(
        task_id="build_dim_customer",
        python_callable=_run_builder,
        op_kwargs={"builder_cls": DimCustomerBuilder},
    )

    build_dim_time = PythonOperator(
        task_id="build_dim_time",
        python_callable=_run_builder,
        op_kwargs={"builder_cls": DimTimeBuilder},
    )

    build_fact_loan = PythonOperator(
        task_id="build_fact_loan",
        python_callable=_run_builder,
        op_kwargs={"builder_cls": FactLoanBuilder},
    )

    build_fact_account = PythonOperator(
        task_id="build_fact_account",
        python_callable=_run_builder,
        op_kwargs={"builder_cls": FactAccountBuilder},
    )
    
    load_to_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=_load_to_warehouse,
    )
    
    build_customer_monthly_trend = PythonOperator(
    task_id="build_customer_monthly_trend",
    python_callable=_run_builder,
    op_kwargs={"builder_cls": CustomerMonthlyTrendBuilder},
    )

    build_loan_cohort_trend = PythonOperator(
        task_id="build_loan_cohort_trend",
        python_callable=_run_builder,
        op_kwargs={"builder_cls": LoanCohortTrendBuilder},
    )

    build_portfolio_snapshot = PythonOperator(
        task_id="build_portfolio_snapshot",
        python_callable=_run_builder,
        op_kwargs={"builder_cls": PortfolioSnapshotBuilder},
    )
    
    build_tasks = [
        build_dim_branch,
        build_dim_customer,
        build_dim_time,
        build_fact_loan,
        build_fact_account,
        build_customer_monthly_trend,
        build_loan_cohort_trend,
        build_portfolio_snapshot,
    ]
    build_tasks >> load_to_warehouse
    