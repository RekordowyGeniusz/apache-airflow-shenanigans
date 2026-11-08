"""The one task every lesson 02 DAG runs: print the times Airflow gave this run."""

from typing import Any

from airflow.sdk import task


@task
def show_times(**context: Any) -> None:
    # A run with no logical date, like one triggered from the CLI, has no data interval
    # either, so these values may be absent: .get() prints None instead of failing.
    print(f"run_type            = {context['dag_run'].run_type}")
    print(f"logical_date        = {context.get('logical_date')}")
    print(f"data_interval_start = {context.get('data_interval_start')}")
    print(f"data_interval_end   = {context.get('data_interval_end')}")
    print(f"run_after           = {context['dag_run'].run_after}")
