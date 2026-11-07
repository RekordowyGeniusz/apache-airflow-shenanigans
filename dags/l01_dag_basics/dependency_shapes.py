"""Lesson 01: the common ways to wire task dependencies.

All tasks are EmptyOperators, which do nothing; look at the Graph view to see the shapes.
"""

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, chain, cross_downstream

with DAG(
    dag_id="l01_dependency_shapes",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lesson-01"],
) as dag:
    # 1. Linear: a runs, then b, then c.
    a, b, c = (EmptyOperator(task_id=f"linear_{name}") for name in "abc")
    a >> b >> c
    # `c << b` would mean the same as `b >> c`; b.set_downstream(c) too.

    # 2. Fan-out / fan-in: a list on either side of >> means "each of these".
    fan_start = EmptyOperator(task_id="fan_start")
    branches = [EmptyOperator(task_id=f"fan_branch_{i}") for i in range(3)]
    fan_join = EmptyOperator(task_id="fan_join")
    fan_start >> branches >> fan_join
    # Note: `[x, y] >> [p, q]` is not allowed; Python cannot `>>` two lists. Use cross_downstream.

    # 3. chain(): a long sequence without repeating >>. Two lists of the same length
    #    are linked pairwise: step_1a >> step_2a and step_1b >> step_2b.
    chain(
        EmptyOperator(task_id="chain_start"),
        [
            EmptyOperator(task_id="chain_step_1a"),
            EmptyOperator(task_id="chain_step_1b"),
        ],
        [
            EmptyOperator(task_id="chain_step_2a"),
            EmptyOperator(task_id="chain_step_2b"),
        ],
        EmptyOperator(task_id="chain_end"),
    )

    # 4. cross_downstream(): every task in the first list before every task in the second.
    sources = [EmptyOperator(task_id=f"cross_source_{i}") for i in range(2)]
    targets = [EmptyOperator(task_id=f"cross_target_{i}") for i in range(3)]
    cross_downstream(sources, targets)

    # The four groups are not connected to each other, so they all start at once.
