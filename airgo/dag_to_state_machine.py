from typing import List, Dict, Any

from airgo.operators.base_operator import BaseOperator
from airgo.utils.decorators import local_artifact_directory
from airgo.dag import DAG
from airgo.utils.decorators import (
    apply_defaults,
)

RESERVED_TAGS = ["__DEFINE_DEFAULTS", "__APPLY_DEFAULTS", "__FINISH"]


class DefineDefaultsSMOperator(BaseOperator):
    @apply_defaults
    def __init__(self, default_dict: Dict[str, Any], **kwargs):
        self.default_dict = default_dict
        super().__init__(task_id="__DEFINE_DEFAULTS", **kwargs)

    def to_sf_dict(self):
        return {
            "Type": "Pass",
            "ResultPath": "$.inputDefaults",
            "Parameters": {
                **self.default_dict,
            },
            **self.df_next_or_end,
        }


class ApplyDefaultsSMOperator(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(task_id="__APPLY_DEFAULTS", **kwargs)

    def to_sf_dict(self):
        return {
            "Type": "Pass",
            "ResultPath": "$.dagInputs",
            "OutputPath": "$.dagInputs",
            "Parameters": {
                "args.$": "States.JsonMerge($.inputDefaults, $$.Execution.Input, false)",
            },
            **self.df_next_or_end,
        }


class FinishSMOperator(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(task_id="__FINISH", **kwargs)

    def to_sf_dict(self):
        return {"Type": "Succeed"}


class ChoiceSMOperator(BaseOperator):
    def df_next_or_end(self) -> Dict[str, Any]:
        return super().df_next_or_end

    def to_sf_dict(self):
        return {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.artifacts.short-circuit",
                    "NumericEquals": 0,
                    **self.df_next_or_end,
                },
                {
                    "Variable": "$.artifacts.short-circuit",
                    "NumericEquals": 1,
                    "Next": "__FINISH",
                },
            ],
            "Default": "SFTP_TO_S3",
        }


class ParallelSMOperator(BaseOperator):
    def __init__(self, tasks: List[BaseOperator], **kwargs):
        self.tasks = tasks
        super().__init__(**kwargs)

    def to_sf_dict(self):
        return {
            "Type": "Parallel",
            "Branches": [
                {
                    "StartAt": task.task_id,
                    "States": {task.task_id: task.to_sf_dict()},
                    "End": True,
                }
                for task in self.tasks
            ],
        }

    @property
    def has_short_circuit(self):
        return any(t.has_short_circuit for t in self.tasks)


class ParallelShortCircuitMergeSMOperator(BaseOperator):
    def to_sf_dict(self) -> Dict[str, Any]:
        return {
            "Type": "Pass",
            "Parameters": {
                f"short_circuit.$": f"States.JsonMerge($[{i}].artifacts.short_circuit)"
                for i in range(len(self.upstream_operators[0].tasks) - 1)
            },
            "ResultPath": "$.artifacts",
            "End": True,
        }


def convert_dag_to_state_machine(dag: DAG):
    initial_tasks = [t for t in dag.tasks if not t.upstream_tasks]
    final_tasks = [t for t in dag.tasks if not t.downstream_tasks]
    default_op = DefineDefaultsSMOperator(
        dag=dag, default_dict=dag.state_machine_default_inputs
    )
    apply_defaults_op = ApplyDefaultsSMOperator(dag=dag)
    default_op.set_downstream(apply_defaults_op)
    apply_defaults_op.set_downstream(initial_tasks)
    finish_op = FinishSMOperator(dag=dag)
    for t in final_tasks:
        t.set_downstream(finish_op)

    tasks_by_tree_depth = dag.task_by_tree_depth
    for task in dag.tasks:
        task.clear_relatives()
    i = 0
    last_node = None
    while len(tasks_by_tree_depth[i]) > 0:
        if len(tasks_by_tree_depth[i]) == 1:
            n = tasks_by_tree_depth[i][0]
        else:
            n = ParallelSMOperator(
                dag=dag, task_id=f"__PARALLEL_{i}", tasks=tasks_by_tree_depth[i]
            )
        if last_node:
            n.set_upstream(last_node)
        last_node = n
        if n.has_short_circuit:
            if len(tasks_by_tree_depth[i]) > 1:
                n = ParallelShortCircuitMergeSMOperator(
                    dag=dag, task_id=f"__PARALLEL_SHORT_CIRCUIT_MERGE_{i}"
                )
                last_node = n
            choice_op = ChoiceSMOperator(dag=dag, task_id=f"__CHOICE_{i}")
            choice_op.set_upstream(n)
            last_node = choice_op
        i += 1
    return dag
