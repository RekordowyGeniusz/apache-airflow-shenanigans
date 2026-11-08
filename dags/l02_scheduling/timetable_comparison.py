"""Lesson 02: the same "daily" written with the four built-in timetables.

The four DAGs share a window that starts at 06:00, off the midnight the cron lines ask for, and
use catchup so every run in the window is created at once. Compare the show_times logs: which
days get a run, and what logical_date and the data interval are for each. A schedule string or
timedelta is shorthand for one of these classes; see the guide for which one.
"""

from datetime import datetime, timedelta

from airflow.sdk import (
    DAG,
    CronDataIntervalTimetable,
    CronTriggerTimetable,
    DeltaDataIntervalTimetable,
    DeltaTriggerTimetable,
)
from l02_scheduling.callables.show_times import show_times

START = datetime(2026, 11, 1, 6)
END = datetime(2026, 11, 4, 6)
TAGS = ["lesson-02", "timetable-comparison"]

with DAG(
    dag_id="l02_timetable_cron_trigger",
    schedule=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
    start_date=START,
    end_date=END,
    catchup=True,
    tags=TAGS,
):
    show_times()

with DAG(
    dag_id="l02_timetable_cron_interval",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=START,
    end_date=END,
    catchup=True,
    tags=TAGS,
):
    show_times()

with DAG(
    dag_id="l02_timetable_delta_trigger",
    schedule=DeltaTriggerTimetable(timedelta(days=1)),
    start_date=START,
    end_date=END,
    catchup=True,
    tags=TAGS,
):
    show_times()

with DAG(
    dag_id="l02_timetable_delta_interval",
    schedule=DeltaDataIntervalTimetable(timedelta(days=1)),
    start_date=START,
    end_date=END,
    catchup=True,
    tags=TAGS,
):
    show_times()
