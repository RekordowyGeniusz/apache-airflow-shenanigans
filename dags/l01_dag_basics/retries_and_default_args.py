"""Lesson 01: default_args and retries.

default_args sets the defaults for every task in the DAG; a task can override them.
flaky_task fails on its first try on purpose, so you can watch Airflow retry it.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

default_args = {
    "owner": "learner",
    "retries": 2,  # Up to 2 retries after the first failure, so 3 tries in total.
    "retry_delay": timedelta(seconds=10),  # Kept short so you can watch it happen.
}


@dag(
    dag_id="l01_retries_and_default_args",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["lesson-01"],
)
def retries_and_default_args() -> None:
    @task
    def flaky_task() -> str:
        # get_current_context() gives a TaskFlow function the same context classic operators get.
        try_number = get_current_context()["ti"].try_number
        print(f"This is try number {try_number}")
        if try_number < 2:
            raise RuntimeError(
                "Simulated failure on the first try; Airflow will retry."
            )
        return "succeeded on a retry"

    # Overrides default_args for this one task: no retries, a different owner.
    @task(retries=0, owner="someone_else")
    def report(result: str) -> None:
        print(f"flaky_task {result}")

    report(flaky_task())


retries_and_default_args()
