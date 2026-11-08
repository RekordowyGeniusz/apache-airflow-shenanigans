"""Lesson 02: a timedelta schedule. Runs are spaced by a duration, not aligned to the clock."""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from l02_scheduling.callables.show_times import show_times

with DAG(
    dag_id="l02_schedule_timedelta",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lesson-02"],
) as dag:
    show_times()
