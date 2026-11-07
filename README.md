# Airflow Learning

A hands-on course for learning Apache Airflow 3 by running and breaking real DAGs.
Everything runs in Docker Compose (Postgres metadata database, separate scheduler,
DAG processor, API server and triggerer), close to a production setup.

## Lessons

Each lesson has its DAGs in `dags/lNN_<topic>/` and a guide in `docs/lessons/NN-<topic>.md`
explaining what to run, what to watch in the UI, and exercises to try.

| # | Lesson | Topics |
|---|---|---|
| 00 | Docker Compose setup and architecture | Compose stack, Airflow components, `airflow dags test` |
| 01 | DAG basics | `@dag`/`@task` vs. classic operators, dependencies, `default_args`, retries |
| 02 | Scheduling and time | cron/timedelta/presets, `start_date`, catchup, backfill, data intervals, timetables |
| 03 | Passing data | XComs, Jinja templating and macros, Params, triggering with config |
| 04 | Branching and control flow | branching, trigger rules, short-circuit, task groups, setup/teardown |
| 05 | Dynamic DAGs | dynamic task mapping, `.partial()`/`.expand()`, generated DAGs |
| 06 | Connections, Variables and Hooks | connections, variables, hooks, HTTP and Postgres, secrets via env |
| 07 | Event-driven pipelines | Assets, sensors, deferrable operators, the triggerer |
| 08 | Reliability | idempotency, timeouts, callbacks, pools, concurrency, priority |
| 09 | Isolated task environments | `@task.docker`, `@task.virtualenv` |
| 10 | Testing and quality | DAG integrity tests, unit tests, `dag.test()`, ruff |
| 11 | Capstone ETL pipeline | API → validate → Postgres → daily report, end to end |

## Running

Requires Docker with Compose v2 (4GB+ memory for Docker).

```bash
cp .env.example .env        # then set AIRFLOW_UID to the output of `id -u`
docker compose up -d        # UI at http://localhost:8080, login airflow / airflow
docker compose down         # stop (add -v to also wipe the database)
```

Details, the architecture and the first DAG: [lesson 00](docs/lessons/00-setup-and-architecture.md).
