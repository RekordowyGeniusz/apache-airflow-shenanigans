"""Lesson 00: the smallest useful DAG, to prove the Docker Compose stack works.

Two tasks run one after the other. The first returns a value, which Airflow stores
as an XCom; the second receives it as an argument. Lesson 01 explains all of this.
"""

import platform
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="l00_hello_airflow",
    schedule=None,  # Only runs when triggered by hand (UI, CLI or API).
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lesson-00"],
)
def hello_airflow() -> None:
    @task
    def say_hello() -> str:
        # print() output ends up in the task log, visible in the UI.
        print(f"Hello from Airflow, running on host {platform.node()}")
        return "Airflow"

    @task
    def greet(name: str) -> None:
        print(f"Hello, {name}! The stack works.")

    greet(say_hello())


hello_airflow()
