from airgo.operators import ShortCircuitPythonOperator


def short_circuit():
    return True


def test_short_circuit(mock_get_project_config, task_registration_op, example_dag):
    task_1 = task_registration_op(task_id="task-1", dag=example_dag)
    task_2 = ShortCircuitPythonOperator(
        task_id="task-2", dag=example_dag, python_callable=short_circuit
    )
    task_3 = task_registration_op(task_id="task-3", dag=example_dag)
    task_4 = task_registration_op(task_id="task-4", dag=example_dag)
    final_task = task_registration_op(task_id="task-5", dag=example_dag)

    task_1.set_downstream(task_3)
    task_2.set_downstream(task_3)
    task_1.set_upstream(task_4)
    task_2.set_upstream(task_4)
    final_task.set_upstream(task_3)

    example_dag.execute()
    expected_order = (4, 1)
    assert example_dag.EXECUTED_TASKS == [f"task-{j}" for j in expected_order]
