"""Lesson 02: catchup=True creates a run for every schedule point since start_date.

end_date closes the window, so the DAG produces the same runs whenever you unpause it.
Without it, a fixed start_date would mean one more run for every day this file ages.
"""

from datetime import datetime

from airflow.sdk import DAG
from l02_scheduling.callables.show_times import show_times

with DAG(
    dag_id="l02_catchup",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    end_date=datetime(2026, 11, 5),
    catchup=True,
    tags=["lesson-02"],
) as dag:
    show_times()
