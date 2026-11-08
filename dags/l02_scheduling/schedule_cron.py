"""Lesson 02: a cron schedule. Runs are aligned to the clock: :00, :05, :10, ..."""

from datetime import datetime

from airflow.sdk import DAG
from l02_scheduling.callables.show_times import show_times

with DAG(
    dag_id="l02_schedule_cron",
    # minute hour day-of-month month day-of-week
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lesson-02"],
) as dag:
    show_times()
