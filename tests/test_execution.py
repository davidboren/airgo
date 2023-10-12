from airgo import DAG
from airgo.exceptions import AirgoException
import pytest


def test_bad_dag_id(default_args):
    with pytest.raises(AirgoException):
        DAG(
            dag_id="BAD_NAME_WITH_UNDERSCORES",
            description="test dag break",
            schedule_interval=None,
            default_args=default_args,
        )
    DAG(
        dag_id="GOOD-NAME-WITH-HYPHENS",
        description="test dag break",
        schedule_interval=None,
        default_args=default_args,
    )


def test_duplicate_ids(task_registration_op, example_dag, default_args):
    with pytest.raises(AirgoException):
        DAG(
            dag_id=example_dag.dag_id,
            description="test dag break",
            schedule_interval=None,
            default_args=default_args,
        )
    task_registration_op(task_id="task-1", dag=example_dag)
    with pytest.raises(AirgoException):
        task_registration_op(task_id="task-1", dag=example_dag)


def test_cycle_break(task_registration_op, example_dag):
    task_1 = task_registration_op(task_id="task-1", dag=example_dag)
    task_2 = task_registration_op(task_id="task-2", dag=example_dag)
    task_3 = task_registration_op(task_id="task-3", dag=example_dag)
    task_1 >> task_2
    task_2 >> task_3
    with pytest.raises(AirgoException):
        task_3 >> task_1
    with pytest.raises(AirgoException):
        task_1 << task_3
    with pytest.raises(AirgoException):
        task_1 >> task_1
    assert set(["task-1"]) == set([t.task_id for t in task_2.upstream_tasks])
    assert set(["task-2"]) == set([t.task_id for t in task_1.downstream_tasks])
    assert set(["task-2"]) == set([t.task_id for t in task_3.upstream_tasks])
    assert set(["task-3"]) == set([t.task_id for t in task_2.downstream_tasks])


def test_execution(task_registration_op, example_dag):
    initial_tasks = [
        task_registration_op(task_id=f"task-{i}", dag=example_dag) for i in range(50)
    ]
    downstream_tasks = [
        task_registration_op(task_id=f"task-{i}", dag=example_dag)
        for i in range(50, 100)
    ]
    upstream_tasks = [
        task_registration_op(task_id=f"task-{i}", dag=example_dag)
        for i in range(100, 150)
    ]
    for task in initial_tasks:
        task.set_upstream(upstream_tasks)
    for task in initial_tasks:
        task.set_downstream(downstream_tasks)
    final_task = task_registration_op(task_id="task-150", dag=example_dag)
    final_task.set_upstream(downstream_tasks)
    example_dag.execute()
    expected_order = (
        [i for i in range(100, 150)]
        + [i for i in range(50)]
        + [i for i in range(50, 100)]
        + [150]
    )
    assert example_dag.EXECUTED_TASKS == [f"task-{j}" for j in expected_order]
