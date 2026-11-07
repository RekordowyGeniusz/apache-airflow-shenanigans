"""Lesson 01: a small pipeline written with classic operators.

Compare with pipeline_taskflow.py, which builds the same pipeline with @dag/@task.
Here every task is an operator object, and data between tasks moves through XCom
explicitly: one task returns a value, the next pulls it with ti.xcom_pull().
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.types import RuntimeTaskInstanceProtocol


def extract() -> list[int]:
    # A PythonOperator's return value is stored as an XCom under the key "return_value".
    return [3, 1, 4, 1, 5, 9, 2, 6]


def transform(ti: RuntimeTaskInstanceProtocol) -> int:
    # Airflow passes context values (ti = the task instance) to arguments with matching names.
    numbers = ti.xcom_pull(task_ids="extract")
    return sum(numbers)


def load(ti: RuntimeTaskInstanceProtocol) -> None:
    total = ti.xcom_pull(task_ids="transform")
    print(f"Loaded total: {total}")


with DAG(
    dag_id="l01_pipeline_classic",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lesson-01"],
) as dag:
    # Operators created inside the `with DAG(...)` block belong to that DAG.
    start = BashOperator(
        task_id="start",
        # bash_command is a Jinja template: {{ run_id }} is filled in when the task runs.
        bash_command='echo "Starting run {{ run_id }}"',
    )
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    # Dependencies are declared separately from the tasks, with the bitshift operator.
    start >> extract_task >> transform_task >> load_task
