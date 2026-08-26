"""
source_activity_simulation_dag.py

Plays the role of "the bank being used" -- runs every 15 minutes and
injects thousands of new/updated rows into the source (OLTP) database.
Completely separate from bronze_batch_extraction_dag -- in the real
world, a bank's core banking system has no idea Airflow exists.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from source_db.simulate_activity import simulate_activity

default_args = {
    "owner": "bryan",
    "retries": 1,
    "retry_delay": 60,
}


def _run_simulation(**context) -> None:
    result = simulate_activity(
        n_new_customers=1500,
        n_customer_updates=2500,
        n_new_accounts=800,
        n_account_updates=1200,
    )
    print(f"[source_activity_simulation] {result}")
    context["ti"].xcom_push(key="simulation_result", value=result)


with DAG(
    dag_id="source_activity_simulation",
    description="Simulates ongoing core-banking activity every 15 minutes",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["source", "simulation", "demo"],
) as dag:

    simulate_activity_task = PythonOperator(
        task_id="simulate_activity",
        python_callable=_run_simulation,
    )