# Lesson 01: DAG basics

## Goal

Write DAGs with both styles Airflow offers (classic operators and TaskFlow), wire task
dependencies in every common shape, and control retries with `default_args`. Along the way,
learn to read a run in the UI.

## Concepts

### DAG, DAG run, task, task instance

| Term | What it is | Example |
|---|---|---|
| **DAG** | A Directed Acyclic Graph: tasks plus the dependencies between them, defined in a Python file. A *definition*, it does nothing by itself. | `l01_pipeline_classic` |
| **Task** | One node of the DAG: one unit of work. | `transform` |
| **Operator** | The class a task is made from; it decides *what kind* of work it is. | `PythonOperator`, `BashOperator`, `EmptyOperator` |
| **DAG run** | One execution of the whole DAG, for example "the run triggered at 17:33". | `manual__2026-11-07T17:33:37…` |
| **Task instance** | One task in one DAG run. This is what has a state, logs and tries. | `transform` in that run |

"Acyclic" means no loops: Airflow refuses a DAG where a task depends, directly or indirectly,
on itself.

### Task instance states

A task instance moves through states, shown as colours in the Grid view:

```
(no status) → scheduled → queued → running → success
                                          ↘ failed
                                          ↘ up_for_retry → (waits retry_delay) → scheduled → …
```

- **scheduled**: its upstream tasks are done; the scheduler decided it should run.
- **queued**: handed to the executor, waiting for a free slot.
- **up_for_retry**: it failed but has retries left.
- **upstream_failed** / **skipped**: covered in lesson 04.

### Two ways to write a DAG

**Classic** (`pipeline_classic.py`): a `with DAG(...)` block, one operator object per task,
dependencies declared with `>>`, and data passed explicitly with `ti.xcom_pull()`.

**TaskFlow** (`pipeline_taskflow.py`): `@dag` and `@task` decorators on plain functions.
Calling a task with another task's result passes the data and creates the dependency at once.

```python
numbers = extract()
load(transform(numbers))  # extract >> transform >> load, with data
```

Both produce the same kind of DAG, and you can mix them in one DAG. TaskFlow is the usual
choice for Python logic; classic operators are how you use ready-made integrations (Bash,
SQL, HTTP, cloud services). Data passed between tasks is stored as an **XCom** in the metadata
database, so keep it small; lesson 03 goes deeper.

The calls inside a `@dag` function do not run your task code. They only *build the graph* when
the DAG processor parses the file. Your function bodies run later, one task at a time, when the
scheduler runs them. That is also why `pipeline_taskflow()` must be called at the bottom of the
file: calling it is what creates the DAG.

### Dependencies

`dependency_shapes.py` shows each shape:

| Shape | Code |
|---|---|
| Linear | `a >> b >> c` (same as `c << b << a`) |
| Fan-out / fan-in | `start >> [x, y, z] >> join` |
| Sequence | `chain(a, b, c, d)`; two equal-length lists in a row link pairwise |
| All-to-all | `cross_downstream([s1, s2], [t1, t2, t3])` |

`[x, y] >> [p, q]` is a Python error: you cannot `>>` one list into another. Use
`cross_downstream` for all-to-all, or `chain` for pairwise.

By default a task runs when **all** its upstream tasks succeeded. Lesson 04 changes that with
trigger rules.

### default_args

`default_args` is a dict of operator arguments applied to every task in the DAG. A task can
override any of them (`retries_and_default_args.py`):

```python
default_args = {"owner": "learner", "retries": 2, "retry_delay": timedelta(seconds=10)}

@task(retries=0, owner="someone_else")  # this task ignores the DAG defaults
def report(result: str) -> None: ...
```

`retries=2` means up to 2 *extra* tries after the first failure. Each try has its own log,
listed as attempt 1, 2, …

### Keeping code out of the DAG file

`tutorial_example.py` (your first DAG from the old `project1/` setup) imports its function from
`l01_dag_basics/callables/example_task.py`. The `dags/` folder is on Python's import path,
so imports start from there: `from l01_dag_basics.callables.example_task import funny_func1`.
Plain functions in their own modules are easier to reuse and to unit-test (lesson 10).

## Run it

Start the stack as in lesson 00 (`docker compose up -d`) and open <http://localhost:8080>.

New DAG files appear within about 5 minutes, when the DAG processor rescans the folder. To see
them right away:

```bash
docker compose exec airflow-scheduler airflow dags reserialize
```

You should now see five `l01_` DAGs (filter by the `lesson-01` tag).

### 1. Classic vs. TaskFlow

1. Unpause and trigger `l01_pipeline_classic` and `l01_pipeline_taskflow`.
2. Open each DAG's **Graph** view: the same four tasks, `start → extract → transform → load`.
3. In each run, click `load` and open **Logs**: both print `Loaded total: 31`.
4. Click `extract` and open the **XCom** tab: the list `[3, 1, 4, …]` is stored there in both
   styles, only the code that reads it differs.
5. Open the **Code** tab of each DAG and compare the two files.

### 2. Dependency shapes

Trigger `l01_dependency_shapes` and look at the **Graph** view. Match each group of boxes to the
code. In the **Grid** view, notice that the first task of every group ran at the same time: the
groups are independent.

### 3. Retries

Trigger `l01_retries_and_default_args` and watch the **Grid** view. `flaky_task` turns
`up_for_retry` (yellow), waits 10 seconds, then succeeds. Click it: the **Logs** tab has a
picker for attempt 1 (with the `RuntimeError`) and attempt 2. Open **Details** to see its
`owner` and `retries`, then compare with `report`.

### 4. Your old DAG

`l01_tutorial_example` has a daily schedule. When you unpause it, the scheduler starts a
*scheduled* run on its own, besides any you trigger. Lesson 02 explains which run that is.

The CLI works too:

```bash
docker compose exec airflow-scheduler airflow dags trigger l01_pipeline_taskflow
docker compose exec airflow-scheduler airflow dags list-runs l01_pipeline_taskflow
docker compose exec airflow-scheduler airflow tasks list l01_dependency_shapes
docker compose exec airflow-scheduler airflow dags test l01_retries_and_default_args
```

## Try this

1. **Run out of retries.** In `flaky_task`, change `try_number < 2` to `try_number < 4`. With
   `retries=2` it now fails three times: the task ends `failed` and `report` becomes
   `upstream_failed`. Then raise `retries` in `default_args` to `3` and run again.
2. **Make a cycle.** In `dependency_shapes.py`, add `c >> a`. What does the UI show? Read the
   error with `airflow dags list-import-errors`, then remove the line.
3. **Fan-in that waits.** In `pipeline_classic.py`, add a second extract task that runs in
   parallel to the first, and make `transform` wait for both:
   `start >> [extract_task, extract_task_2] >> transform_task`. Watch the Graph view.
4. **Convert a DAG.** Rewrite `tutorial_example.py` with `@dag` and `@task`, calling
   `funny_func1()` inside the task. Give it a new `dag_id` so both versions show up.
5. **Forget the call.** Note the `last_parsed_time` in
   `airflow dags details l01_pipeline_taskflow`. Comment out `pipeline_taskflow()` at the bottom
   of `pipeline_taskflow.py` and reserialize. `list-import-errors` stays empty, but
   `last_parsed_time` no longer changes: the file parses fine, it just no longer creates a DAG,
   so Airflow keeps the last version it saw. Restore the line and reserialize.

## Recap

- A DAG is a definition; each execution is a DAG run, and each task in a run is a task instance
  with its own state, logs and tries.
- Classic operators: explicit `>>` and `xcom_pull()`. TaskFlow: function calls create both the
  data flow and the dependencies. Mix them freely.
- Wire dependencies with `>>`, lists, `chain()` and `cross_downstream()`.
- `default_args` sets defaults for all tasks; per-task arguments override them.
- Keep task logic in plain modules under `dags/` and import it into the DAG file.
