# AGENTS.md

A learning repository for Apache Airflow 3. The reader is learning Airflow, so clarity beats cleverness.

## Layout

- `docker-compose.yaml`, `.env.example`: the Airflow stack (set up in lesson 00).
- `config/` (generated `airflow.cfg`, ignored), `plugins/`, `logs/` (ignored): mounted into the containers.
- `dags/lNN_<topic>/`: DAGs for lesson NN. One DAG teaches one idea. DAG ids start with `lNN_`.
- `dags/lNN_<topic>/callables/` or plain modules: task logic kept out of DAG files when it grows.
- `docs/lessons/NN-<topic>.md`: the lesson guide.
- `tests/`: pytest tests (from lesson 10 on; DAG integrity tests cover every lesson).

## Lesson conventions

- Every lesson guide has: **Goal**, **Concepts** (short explanations), **Run it** (exact commands and
  where to look in the UI), **Try this** (exercises that change or break the DAGs), and **Recap**.
- DAGs use the Airflow 3 Task SDK (`from airflow.sdk import dag, task, ...`) unless the lesson is
  about classic operators. Use `airflow.providers.standard` for standard operators and sensors.
- Comment DAG code where the comment teaches something; keep DAGs small and runnable on their own.
- Set `catchup=False` and a fixed past `start_date` unless the lesson is about them.
- Tag each DAG with its lesson (`tags=["lesson-NN"]`).

## Verification

- Verify APIs against the Airflow version pinned in `docker-compose.yaml` (Context7 / official docs);
  never from memory.
- A lesson is done when its DAGs parse (`airflow dags list-import-errors` is empty) and each DAG
  runs successfully, or fails on purpose where the lesson says so, in the Compose stack.
