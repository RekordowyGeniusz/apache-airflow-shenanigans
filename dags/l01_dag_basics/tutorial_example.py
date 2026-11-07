"""Lesson 01: your first DAG from the old local setup (project1/), ported to this repository.

A classic DAG with one PythonOperator that calls a function from another module.
The dags/ folder is on Python's import path, so modules import relative to it.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from l01_dag_basics.callables.example_task import funny_func1

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Other per-task defaults you can set here:
    # "execution_timeout": timedelta(seconds=300),
    # "on_failure_callback": some_function,  # lesson 08
    # "trigger_rule": "all_success",  # lesson 04
}

with DAG(
    dag_id="l01_tutorial_example",
    default_args=default_args,
    description="A simple example of a DAG",
    schedule=timedelta(days=1),  # Lesson 02 explains what "daily" really means.
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lesson-01"],
) as dag:
    PythonOperator(
        task_id="example_python_task1",
        python_callable=funny_func1,
    )
