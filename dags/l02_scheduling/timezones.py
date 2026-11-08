"""Lesson 02: a cron schedule in local time, across a daylight-saving change.

Europe/Warsaw left summer time on 2026-10-25. The DAG runs at 09:00 Warsaw time every day,
so its time in UTC, which is what the logs show, moves by one hour on that date.
"""

import pendulum
from airflow.sdk import DAG
from l02_scheduling.callables.show_times import show_times

WARSAW = "Europe/Warsaw"

with DAG(
    dag_id="l02_timezones",
    # A timezone-aware start_date is what makes the cron expression below local time.
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 10, 23, tz=WARSAW),
    end_date=pendulum.datetime(2026, 10, 27, 23, 59, tz=WARSAW),
    catchup=True,
    tags=["lesson-02"],
) as dag:
    show_times()
