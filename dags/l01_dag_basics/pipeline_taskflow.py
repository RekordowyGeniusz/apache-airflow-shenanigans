"""Lesson 01: the same pipeline as pipeline_classic.py, written with TaskFlow.

@task turns a plain function into a task. Calling one task with another's result
both passes the data (as an XCom, behind the scenes) and creates the dependency,
so there is no xcom_pull() and no `>>` for the data flow.
"""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="l01_pipeline_taskflow",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lesson-01"],
)
def pipeline_taskflow() -> None:
    # TaskFlow and classic operators mix freely: @task.bash runs the returned command.
    @task.bash
    def start() -> str:
        return 'echo "Starting run $AIRFLOW_CTX_DAG_RUN_ID"'

    @task
    def extract() -> list[int]:
        return [3, 1, 4, 1, 5, 9, 2, 6]

    @task
    def transform(numbers: list[int]) -> int:
        return sum(numbers)

    @task
    def load(total: int) -> None:
        print(f"Loaded total: {total}")

    # The data flow extract -> transform -> load creates those dependencies.
    # start passes no data, so its dependency is declared with >>.
    numbers = extract()
    start() >> numbers
    load(transform(numbers))


# Calling the @dag function is what registers the DAG; without this line it is ignored.
pipeline_taskflow()
