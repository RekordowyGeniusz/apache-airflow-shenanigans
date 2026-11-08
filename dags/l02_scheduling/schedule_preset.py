"""Lesson 02: a preset. "@daily" is a readable name for the cron "0 0 * * *"."""

from datetime import datetime

from airflow.sdk import DAG
from l02_scheduling.callables.show_times import show_times

with DAG(
    dag_id="l02_schedule_preset",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lesson-02"],
) as dag:
    show_times()
