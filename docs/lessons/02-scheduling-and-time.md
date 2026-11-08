# Lesson 02: Scheduling and time

## Goal

Predict exactly when Airflow will run a DAG and which dates that run will carry: read a schedule,
tell a logical date from a data interval, and recover past runs with catchup and backfill.

## Concepts

### A schedule, a timetable

You give a DAG a `schedule`. Airflow turns it into a **timetable**, the object that decides when
runs happen:

| You write | Meaning | Timetable in Airflow 3 |
|---|---|---|
| `"*/5 * * * *"` | cron: aligned to the clock | `CronTriggerTimetable` |
| `"@daily"` | preset: a name for the cron `0 0 * * *` | `CronTriggerTimetable` |
| `timedelta(minutes=5)` | a fixed duration between runs | `DeltaTriggerTimetable` |
| `None` | never scheduled, only triggered | none |

Cron fields are `minute hour day-of-month month day-of-week`. Other presets include `@hourly`,
`@weekly` and `@monthly`. You can also pass a timetable object as `schedule` yourself, which
`timetable_comparison.py` does.

`start_date` is the earliest moment the schedule applies. `end_date` is the last.

### The times of a run

`show_times`, the task in every lesson DAG, prints what Airflow gave the run:

| Value | What it is |
|---|---|
| `run_type` | Why the run exists: `scheduled`, `manual`, `backfill` (or `asset_triggered`, lesson 07) |
| `logical_date` | The moment the run is "for". It identifies the run; it is not when it ran |
| `data_interval_start`, `data_interval_end` | The period of data the run should process |
| `run_after` | The earliest moment the run may start |

The scheduler picks up a run at or after `run_after`, so real start times are slightly later. A
run triggered from the CLI has no logical date and no data interval: Airflow 3 lets a run be
"for" nothing in particular. `run_after` always exists.

### Trigger timetables and data-interval timetables

The two families answer a different question.

**Trigger timetables** (the Airflow 3 default) say *when to fire*. The run fires at the schedule
point, and that point is its logical date. The data interval has zero length:
`data_interval_start == data_interval_end == logical_date`.

**Data-interval timetables** (the Airflow 2 default) say *which period to process*. A run covers a
whole period and fires when that period is complete.

### Why a daily DAG runs at the end of its interval

With a data-interval timetable, the run that processes 2 November cannot start until 2 November is
over. It waits for the end of the interval:

```
         interval of the run                the run starts here
   ┌─────────────────────────────┐              ↓
 Nov 2 00:00                Nov 3 00:00
   ↑
   logical_date = Nov 2 00:00, the START of the interval
```

This is the classic surprise: the run labelled 2 November starts on 3 November. A trigger timetable
has no such gap, but its zero-length interval also says nothing about which data to read. To
process "yesterday" with one, give it an interval (`CronTriggerTimetable(..., interval=timedelta(days=1))`,
exercise 2), or compute the period from `logical_date` yourself.

### Cron and delta differ on alignment

Cron is anchored to the clock; a delta is anchored to `start_date`. `timetable_comparison.py` writes
"daily" four ways, all from `2026-11-01 06:00` to `2026-11-04 06:00` UTC. Here is what you get:

| DAG (`l02_timetable_…`) | Logical dates | Data interval of the first run | Runs at |
|---|---|---|---|
| `cron_trigger` | Nov 2, 3, 4 at 00:00 | Nov 2 00:00, zero length | the logical date |
| `cron_interval` | Nov 2, 3, 4 at 00:00 | Nov 2 00:00 to Nov 3 00:00 | the end of the interval |
| `delta_trigger` | Nov 1, 2, 3, 4 at 06:00 | Nov 1 06:00, zero length | the logical date |
| `delta_interval` | Nov 1, 2, 3, 4 at 06:00 | Nov 1 06:00 to Nov 2 06:00 | the end of the interval |

Cron has no run on 1 November: midnight of that day is before `start_date`. Note also that `end_date`
limits the *logical date*: the last `cron_interval` run starts at Nov 5 00:00, after `end_date`.

### Catchup and backfill

Both create runs for the past, but they are not the same thing.

| | Catchup | Backfill |
|---|---|---|
| Who starts it | The scheduler, on its own | You, from the CLI or UI |
| Setting | `catchup=True` on the DAG | none; works with `catchup=False` too |
| Which runs | Every schedule point since `start_date` | The range you choose, both ends included |
| `run_type` | `scheduled` | `backfill` |

With `catchup=False`, unpausing creates only the **latest** schedule point, however old `start_date`
is. A backfill by default adds only the **missing** runs in its range. Only one backfill can run per
DAG at a time; a second one fails with `AlreadyRunningBackfill` until the first ends.

Choose `--reprocess-behavior` to change which dates are included: `none` (missing runs, the default),
`failed` (also runs that failed), or `completed` (every run, redoing the ones that succeeded).

Airflow logs "Created backfill Dag run" even for a date it skips, so preview with `--dry-run`:
it lists the runs that would really happen.

### Timezones

Airflow stores and displays times in UTC (`default_timezone = utc` here). A schedule string is read
in the timezone of the DAG's `start_date`, so a timezone-aware `start_date` makes cron local:

```python
schedule="0 9 * * *",
start_date=pendulum.datetime(2026, 10, 23, tz="Europe/Warsaw"),
```

`l02_timezones` runs at 09:00 Warsaw time across the end of summer time on 2026-10-25. In UTC, the
runs move by an hour:

| Logical date (UTC) | Warsaw time |
|---|---|
| Oct 23 and 24, 07:00 | 09:00 (summer time, UTC+2) |
| Oct 25, 26 and 27, 08:00 | 09:00 (winter time, UTC+1) |

A timedelta does not follow the wall clock like this, because a delta is a fixed duration, so use cron
when "09:00 local" matters. A timetable object takes its own `timezone=` argument.

### What changed in Airflow 3

| Setting | Airflow 3 default | Effect |
|---|---|---|
| `[scheduler] catchup_by_default` | `False` | No surprise history when a DAG is first unpaused |
| `[scheduler] create_cron_data_intervals` | `False` | Cron strings use `CronTriggerTimetable` |
| `[scheduler] create_delta_data_intervals` | `False` | Timedeltas use `DeltaTriggerTimetable` |

Set a `create_*` option to `True` to get the data-interval timetable for every plain schedule of that
kind, as in Airflow 2. Also new: runs without a logical date, and backfills created by
`airflow backfill create` and run by the scheduler.

`l01_tutorial_example` from lesson 01 is `timedelta(days=1)` with `catchup=False`. Its first
scheduled run is therefore a `DeltaTriggerTimetable` run dated the moment you unpaused it, and the
next comes a day later.

## Run it

Start the stack from this checkout (`docker compose up -d`) and load the new DAGs:

```bash
docker compose exec airflow-scheduler airflow dags reserialize
docker compose exec airflow-scheduler airflow dags list-import-errors   # must be empty
```

You should now see eleven `l02_` DAGs (filter by the `lesson-02` tag). To read a run, open it in the
**Grid** view, click `show_times` and open **Logs**. All times below are UTC.

### 1. Four ways to schedule

Unpause `l02_schedule_cron`, `l02_schedule_timedelta` and `l02_schedule_preset`, then wait a few
minutes and compare.

- `l02_schedule_cron` first creates the latest 5-minute mark before you unpaused it, then one run at
  every following mark.
- `l02_schedule_timedelta` first creates a run dated when you unpaused it, then one every 5 minutes
  from there. Its minutes are not round.
- `l02_schedule_preset` creates one run for today at 00:00 and nothing more until tomorrow.

Unpause `l02_schedule_manual`, then trigger it with the CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger l02_schedule_manual
```

No run appears until you trigger it. Its log shows `run_type = DagRunType.MANUAL` and
`logical_date = None`.

Pause the two 5-minute DAGs when you are done, or they keep running.

### 2. Compare the timetables

Filter by the `timetable-comparison` tag and unpause all four. Each DAG creates its runs at once (they
are in the past and catchup is on). Check them against the table in **Concepts**: `cron_interval` and
`cron_trigger` have 3 runs, the delta ones have 4.

### 3. Catchup

Unpause `l02_catchup`. It creates 5 runs of `run_type = scheduled`, for 1 to 5 November at 00:00, and
stops because of `end_date`.

### 4. Backfill

Unpause `l02_backfill`. With `catchup=False` it creates a single run, for today at 00:00, despite its
start on 1 November. Now fill in part of the past:

```bash
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id l02_backfill --from-date 2026-11-01 --to-date 2026-11-03 --max-active-runs 1
```

Three runs appear with `run_type = backfill`, one at a time. In the UI you can do the same from
**Trigger** on the DAG's page by choosing **Backfill**.

### 5. Timezones

Unpause `l02_timezones` and check the logical dates against the table in **Concepts**.

## Try this

1. **Move the window.** In `catchup.py`, change `end_date` to `datetime(2026, 11, 7)` and reserialize.
   Two more runs appear, for 6 and 7 November: catchup continues from the last run.
2. **Widen a trigger interval.** In `timetable_comparison.py`, change the first DAG's schedule to
   `CronTriggerTimetable("0 0 * * *", timezone="UTC", interval=timedelta(days=1))`. Delete its history
   and unpause it again, so it starts over:
   ```bash
   docker compose exec airflow-scheduler airflow dags delete -y l02_timetable_cron_trigger
   docker compose exec airflow-scheduler airflow dags reserialize
   docker compose exec airflow-scheduler airflow dags unpause -y l02_timetable_cron_trigger
   ```
   The runs still start at midnight on Nov 2, 3 and 4, but each now covers the day *before* it, and
   its `logical_date` moves back to the start of that day. It now looks just like `cron_interval`.
3. **Drop the timezone.** In `timezones.py`, set `WARSAW = "UTC"`, then delete, reserialize and
   unpause `l02_timezones` the same way. All five runs are at 09:00 UTC and the shift disappears.
4. **Backfill again.** Preview a wider range:
   ```bash
   docker compose exec airflow-scheduler airflow backfill create \
     --dag-id l02_backfill --from-date 2026-11-01 --to-date 2026-11-05 --dry-run
   ```
   Only 4 and 5 November are listed, because the first three already exist. Add
   `--reprocess-behavior completed` and drop `--dry-run` to redo them all. The first three runs go
   back to `queued` with the same run ids, and run again.
5. **Trigger for a date.** Give a manual run a logical date:
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger l02_schedule_manual \
     --logical-date 2026-11-02T12:00:00+00:00
   ```
   `logical_date` is what you passed, but the data interval is the moment you triggered it: with
   `schedule=None`, there is no timetable to derive one from.

## Recap

- A schedule becomes a timetable. In Airflow 3, cron strings and timedeltas default to trigger
  timetables, which fire at the schedule point with a zero-length data interval.
- `logical_date` names a run; `run_after` says when it may start; a data interval says which period it
  processes. A manual run can have no logical date at all.
- With a data-interval timetable, a run starts when its interval ends.
- Cron is aligned to the clock; a timedelta is anchored to `start_date`.
- Catchup is the scheduler filling the past since `start_date` on its own; a backfill is a range you
  ask for, by default only the missing runs.
- A timezone-aware `start_date` makes cron local across daylight-saving changes.
