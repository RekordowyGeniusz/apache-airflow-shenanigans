"""Lesson 02: the target for a backfill. catchup=False, so old runs exist only if you ask."""

from datetime import datetime

from airflow.sdk import DAG
from l02_scheduling.callables.show_times import show_times

with DAG(
    dag_id="l02_backfill",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["lesson-02"],
) as dag:
    show_times()
