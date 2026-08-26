"""
silver_batch_transformation_dag.py

Transforms Bronze partitions into Silver: cleans, masks PII (NIK hashed,
name partially masked), and enforces data-quality rules (referential
integrity against the accumulated ReferenceRegistry, not-null, uniqueness,
value ranges). Rows that fail a DQ check are dropped from Silver and logged
to output/silver/_dq_reports/ rather than silently propagating bad data
downstream.

This DAG has NO schedule of its own (schedule=None) -- it's triggered
directly by bronze_batch_extraction's last task via TriggerDagRunOperator,
right after Bronze finishes. Bronze passes the partition_key (Airflow's
ts_nodash) it just wrote through TriggerDagRunOperator's conf, so Silver
reads exactly that slice from MinIO -- no time-based coordination needed.

Order matters inside this DAG: branches and customers must land in Silver
(and register their IDs in the ReferenceRegistry) before accounts and loans
run their referential-integrity checks.

Drop this file into your Airflow `dags/` folder, same as
bronze_batch_extraction_dag.py.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from silver.concrete_transform import (
    AccountTransformer,
    BranchTransformer,
    CustomerTransformer,
    LoanTransformer,
)

default_args = {
    "owner": "bryan",
    "retries": 2,
    "retry_delay": 300,  # seconds
}


def _run_transform(transformer_cls, **context) -> None:
    partition_key = context["dag_run"].conf.get("partition_key")
    result = transformer_cls().run(partition_key=partition_key)
    print(
        f"[{transformer_cls.__name__}] rows_in={result.rows_in} rows_out={result.rows_out} "
        f"dq_passed={result.dq_report.passed} skipped={result.skipped}"
    )
    context["ti"].xcom_push(
        key=f"{result.table}_result",
        value={
            "rows_in": result.rows_in,
            "rows_out": result.rows_out,
            "dq_passed": result.dq_report.passed,
            "skipped": result.skipped,
        },
    )


with DAG(
    dag_id="silver_batch_transformation",
    description="Clean, mask PII, and validate Bronze partitions into Silver",
    default_args=default_args,
    schedule=None,  # triggered by bronze_batch_extraction, not on its own clock
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "banking", "batch"],
) as dag:

    transform_branches = PythonOperator(
        task_id="transform_branches",
        python_callable=_run_transform,
        op_kwargs={"transformer_cls": BranchTransformer},
    )

    transform_customers = PythonOperator(
        task_id="transform_customers",
        python_callable=_run_transform,
        op_kwargs={"transformer_cls": CustomerTransformer},
    )

    transform_accounts = PythonOperator(
        task_id="transform_accounts",
        python_callable=_run_transform,
        op_kwargs={"transformer_cls": AccountTransformer},
    )

    transform_loans = PythonOperator(
        task_id="transform_loans",
        python_callable=_run_transform,
        op_kwargs={"transformer_cls": LoanTransformer},
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="gold_batch_transformation",
        wait_for_completion=False,
    )

    # accounts/loans validate FKs against customers/branches -> those must
    # register their IDs in the ReferenceRegistry first.
    [transform_branches, transform_customers] >> transform_accounts >> transform_loans >> trigger_gold