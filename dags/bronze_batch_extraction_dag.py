"""
bronze_batch_extraction_dag.py

Hourly batch extraction of master/reference data (branches, customers,
accounts, loans) from the core banking source system into the Bronze
layer, using watermark-based incremental extraction.

Transaction data is NOT here -- it's high-volume and event-driven, so it's
streamed continuously via Kafka instead (see kafka/producer.py and
kafka/consumer.py). This DAG runs hourly to sync everything that changed
since the last run (source_activity_simulation feeds it new data every 15
minutes in between).

On success, this DAG directly triggers silver_batch_transformation via
TriggerDagRunOperator -- no time-based coordination needed between the two
DAGs, Silver just runs whenever Bronze says it's done.

Drop this file into your Airflow `dags/` folder. Assumes the project root
(this repo) is on PYTHONPATH -- e.g. mounted into the Airflow container the
same way as the bootcamp's docker-compose setup.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from extractors.concrete_extractors import (
    BranchExtractor,
    CustomerExtractor,
    AccountExtractor,
    LoanExtractor,
)

default_args = {
    "owner": "bryan",
    "retries": 2,
    "retry_delay": 300,  # seconds
}


def _run_extractor(extractor_cls, **context) -> None:
    partition_key = context["ts_nodash"]  # e.g. "20260811T090000" -- shared by all tasks in this run
    extractor = extractor_cls()
    result = extractor.extract(partition_key=partition_key)
    print(f"[{extractor_cls.__name__}] {result}")
    # Push to XCom so downstream tasks (e.g. a data-quality check) can react
    context["ti"].xcom_push(key=f"{extractor.table_name}_result", value=result)


with DAG(
    dag_id="bronze_batch_extraction",
    description="Incremental extraction of branch/customer/account/loan master data into Bronze",
    default_args=default_args,
    schedule="0 * * * *",  # every hour on the hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "banking", "batch"],
) as dag:

    extract_branches = PythonOperator(
        task_id="extract_branches",
        python_callable=_run_extractor,
        op_kwargs={"extractor_cls": BranchExtractor},
    )

    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=_run_extractor,
        op_kwargs={"extractor_cls": CustomerExtractor},
    )

    extract_accounts = PythonOperator(
        task_id="extract_accounts",
        python_callable=_run_extractor,
        op_kwargs={"extractor_cls": AccountExtractor},
    )

    extract_loans = PythonOperator(
        task_id="extract_loans",
        python_callable=_run_extractor,
        op_kwargs={"extractor_cls": LoanExtractor},
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="silver_batch_transformation",
        conf={"partition_key": "{{ ts_nodash }}"},
        wait_for_completion=False,  # fire-and-forget; don't block this DAG's worker slot
    )

    # accounts/loans reference customer_id + branch_id, so pull those first
    [extract_branches, extract_customers] >> extract_accounts >> extract_loans >> trigger_silver