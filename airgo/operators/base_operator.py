import logging
import json
import hashlib
import os
import yaml

from typing import Dict, List, Optional, Set, Any, Union, Iterable, Sequence
from cached_property import cached_property  # type: ignore
from logging import Logger
from datetime import datetime

from airgo.utils.traversal import (
    get_configuration_template,
)
from airgo.exceptions import AirgoInstantiationException
from airgo.utils.decorators import (
    apply_defaults,
    ARTIFACT_PROPERTIES,
    local_artifact_directory,
)
from airgo.utils.k8 import k8_str_test, k8_str_filter


class BaseOperator:
    """
    Base class for all Airgo operators.

    Our operators require one abstract method to be created `execute`, which will be called
    within the container when the operator is invoked.
    Any operators passed in as upstream_operators will be set as the upstream of this operator.
    Likewise for downstream operators.

    :param task_id: Unique identifier for the task.
    :type task_id: str
    :param requests_cpu: Kubernetes compatible cpu requests string.
    :type requests_cpu: str
    :param requests_memory: Kubernetes compatible memory requests string.
    :type requests_memory: str
    :param limits_cpu: Kubernetes or State-Machine compatible cpu limits string.
    :type limits_cpu: str
    :param limits_memory: Kubernetes or State-Machine compatible memory limits string.
    :type limits_memory: str
    :param template: Name of template to use.  References the names of the yamls placed in your `airgo/templates` directory
    :type template: str
    :type upstream_operators: list of airflow operators
    :param upstream_operators: list of airflow operators that must run prior to this operator.
    :type upstream_operators: list of airflow operators
    :param downstream_operators: list of airflow operators that must run prior to this operator.
    :type downstream_operators: list of airflow operators
    :param hard_disk_path: Creates an emptydir volumemount at a particular directory for this task to ensure that hardisk is accessible there.
    :type hard_disk_path: string
    """

    @classmethod
    def get_artifact_dir(cls) -> str:
        return os.getenv("ARGO_ARTIFACT_DIRECTORY", "/.argo-artifacts")

    @classmethod
    def qualname_to_property(cls, qualname: str) -> str:
        return qualname.split(".")[1]

    @classmethod
    def artifact_property_to_fullname(cls, property_name: str) -> str:
        return f"{cls.__name__}.{property_name}"

    def artifact_property_to_param(self, property_name):
        return self.task_id + "-" + property_name

    @classmethod
    def artifact_property_to_path(cls, property_name: str) -> str:
        return os.path.join(
            cls.get_artifact_dir(), cls.artifact_property_to_fullname(property_name)
        )

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        requests_cpu: str,
        requests_memory: str,
        dag: Any,
        limits_cpu: Optional[str] = None,
        limits_memory: Optional[str] = None,
        template: str = "default",
        retries: int = 0,
        parameters: Union[Dict[str, str], None] = None,
        upstream_operators: None = None,
        downstream_operators: None = None,
        hard_disk_path: Union[None, str] = None,
    ) -> None:
        self.dag = dag
        self.requests_cpu = requests_cpu
        self.requests_memory = requests_memory
        self.limits_cpu = limits_cpu
        self.limits_memory = limits_memory
        self.retries = retries
        self.task_id = task_id
        self.template = k8_str_filter(template, dag.project_type)
        self.upstream_tasks: List[BaseOperator] = []
        self.downstream_tasks: List[BaseOperator] = []
        self.hard_disk_path = hard_disk_path
        if upstream_operators:
            self.set_upstream(upstream_operators)
        if downstream_operators:
            self.set_downstream(downstream_operators)
        self.dag.add_task(self)
        self.parameters = (
            {}
            if parameters is None
            else {k8_str_filter(p, dag.project_type): v for p, v in parameters.items()}
        )

    @property
    def artifact_properties(self) -> List[str]:
        return [
            self.qualname_to_property(p)
            for p in ARTIFACT_PROPERTIES
            if self.is_qualname(p)
        ]

    def __repr__(self):
        return f"{self.task_id}: {self.__class__.__name__}"

    def __eq__(self, other: "BaseOperator") -> bool:
        return self.task_id == other.task_id

    def __rshift__(self, other: "BaseOperator") -> "BaseOperator":
        """
        Implements Self >> Other == self.set_downstream(other)
        """
        self.set_downstream(other)
        return other

    def __lshift__(self, other: "BaseOperator") -> "BaseOperator":
        """
        Implements Self << Other == self.set_upstream(other)
        """
        self.set_upstream(other)
        return other

    @cached_property
    def logger(self) -> Logger:
        self._log = logging.getLogger(self.task_id)
        self._log.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        self._log.addHandler(ch)
        return self._log

    @property
    def parameters(self) -> Dict[str, Any]:
        return self._parameters

    @parameters.setter
    def parameters(self, parameters: Dict[str, Any]) -> None:
        for k in parameters.keys():
            k8_str_test(k, "parameters", self.dag.project_type)
        self._parameters = parameters

    @property
    def parameter_names(self) -> List[str]:
        return sorted(self.parameters.keys())

    @property
    def template(self) -> str:
        return self._template

    @template.setter
    def template(self, template: str) -> None:
        k8_str_test(template, "template", self.dag.project_type)
        self._template = template

    @property
    def task_id(self) -> str:
        return self._task_id

    @task_id.setter
    def task_id(self, task_id: str):
        if not isinstance(task_id, str):
            raise AirgoInstantiationException(
                f"Your task_id must be a string, not {type(task_id)}"
            )

        k8_str_test(task_id, "task_id", self.dag.project_type)
        self._task_id = task_id

    @property
    def dag(self) -> Any:
        return self._dag

    @dag.setter
    def dag(self, dag: Any) -> None:
        if not dag.__class__.__name__ == "DAG":
            raise AirgoInstantiationException(
                f"Your dag must be an instance of Airgo's DAG class, not {dag.__class__.__name__}"
            )

        self._dag = dag

    def has_dag(self) -> bool:
        return hasattr(self, "_dag")

    def set_upstream(
        self, task_or_task_list: Union[Sequence["BaseOperator"], "BaseOperator"]
    ) -> None:
        self._set_relatives(task_or_task_list, upstream=True)

    def set_downstream(
        self, task_or_task_list: Union[Sequence["BaseOperator"], "BaseOperator"]
    ) -> None:
        self._set_relatives(task_or_task_list, upstream=False)

    def add_only_new(self, item_set: Set[str], item: str) -> None:
        if item in item_set:
            raise AirgoInstantiationException(
                "Dependency {self}, {item} already registered" "".format(**locals())
            )

        else:
            item_set.add(item)

    def get_relative_task_ids(self, upstream: bool = False) -> List[str]:
        return list(
            set(
                [
                    task_id
                    for task in (
                        self.upstream_tasks if upstream else self.downstream_tasks
                    )
                    for task_id in (
                        [task.task_id] + task.get_relative_task_ids(upstream)
                    )
                ]
            )
        )

    def _set_relatives(
        self,
        task_or_task_list: Union["BaseOperator", Iterable["BaseOperator"]],
        upstream: bool = False,
    ) -> None:
        if isinstance(task_or_task_list, BaseOperator):
            task_list = [task_or_task_list]
        else:
            task_list = list(task_or_task_list)
        for t in task_list:
            if not isinstance(t, BaseOperator):
                raise AirgoInstantiationException(
                    "Relationships can only be set between "
                    "Operators; received {}".format(t.__class__.__name__)
                )

        relative_task_ids = self.get_relative_task_ids(not upstream)
        cyclical_tasks = list(
            set(
                [
                    task.task_id
                    for task in task_list
                    if task.task_id in relative_task_ids
                ]
            )
        )
        if self.task_id in [task.task_id for task in task_list]:
            raise AirgoInstantiationException(
                f"Tried to set an {'upstream' if upstream else 'downstream'} "
                f"relationship with self on task {self.task_id}"
            )

        if len(cyclical_tasks) > 0:
            raise AirgoInstantiationException(
                f"Tried to set an {'upstream' if upstream else 'downstream'} "
                f"relationship on task {self.task_id}, which already has tasks "
                f"{cyclical_tasks} set as {'downstream' if upstream else 'upstream'} tasks."
            )

        # relationships can only be set if the tasks share a single DAG. Tasks
        # without a DAG are assigned to that DAG.
        dags = {t.dag.dag_id: t.dag for t in [self] + task_list if t.has_dag()}
        if len(dags) > 1:
            raise AirgoInstantiationException(
                "Tried to set relationships between tasks in "
                "more than one DAG: {}".format(dags.values())
            )

        elif len(dags) == 1:
            dag = dags.popitem()[1]
        else:
            raise AirgoInstantiationException(
                "Tried to create relationships between tasks that don't have "
                "DAGs yet. Set the DAG for at least one "
                "task  and try again: {}".format([self] + task_list)
            )

        if dag and not self.has_dag:
            self.dag = dag
        for task in task_list:
            if dag and not task.has_dag():
                task.dag = dag
            if upstream:
                if not any(t.task_id == task.task_id for t in self.upstream_tasks):
                    self.upstream_tasks.append(task)
                    task.set_downstream(self)
            else:
                if not any(t.task_id == task.task_id for t in self.downstream_tasks):
                    self.downstream_tasks.append(task)
                    task.set_upstream(self)

    def clear_relatives(self):
        for task in self.downstream_tasks:
            task.upstream_tasks.remove(self)
        for task in self.upstream_tasks:
            task.downstream_tasks.remove(self)
        self.downstream_tasks = []
        self.upstream_tasks = []

    def get_all_relative_ids(self, upstream: bool = True) -> Set[str]:
        deps = self.upstream_tasks if upstream else self.downstream_tasks
        return set(
            [t for r in deps for t in r.get_all_relative_ids(upstream)]
            + [t.task_id for t in deps]
        )

    @property
    def all_upstream_task_ids(self):
        return self.get_all_relative_ids(upstream=True)

    @property
    def all_downstream_task_ids(self):
        return self.get_all_relative_ids(upstream=False)

    @property
    def hashable_attribute_names(self) -> List[str]:
        return [
            "requests_cpu",
            "requests_memory",
            "limits_cpu",
            "limits_memory",
            "retries",
            "artifact_properties",
            "hard_disk_paths",
        ]

    @property
    def hashable_attributes(self) -> Dict[str, str]:
        return {
            k: getattr(self, k) if getattr(self, k) is not None else "N/A"
            for k in self.hashable_attribute_names
        }

    @property
    def hash_suffix(self) -> str:
        return hashlib.sha256(
            json.dumps(self.hashable_attributes, sort_keys=True).encode("utf8")
        ).hexdigest()[:10]

    @property
    def default_parameters(self) -> List[str]:
        return ["task-id", "dag-id"]

    @property
    def filtered_default_parameters(self) -> List[str]:
        return [
            k8_str_filter(el, self.dag.project_type) for el in self.default_parameters
        ]

    @property
    def has_short_circuit(self):
        return "short_circuit" in self.artifact_properties

    @cached_property
    def upstream_short_circuits(self) -> Sequence["BaseOperator"]:
        return [
            t
            for t in self.dag.tasks
            if t.task_id in self.get_all_relative_ids(upstream=True)
            and t.has_short_circuit
        ]

    @cached_property
    def final_template_name(self) -> str:
        return self.template + "-" + self.hash_suffix

    def execute(self, context: Dict[str, Any]) -> None:
        pass

    @property
    def final_parameters(self) -> List[str]:
        return sorted(set(self.filtered_default_parameters + self.parameter_names))

    def is_qualname(self, qualname: str) -> bool:
        return qualname.split(".")[0] in [C.__name__ for C in self.__class__.mro()]

    @property
    def hard_disk_paths(self) -> Dict[str, str]:
        return (
            {}
            if self.hard_disk_path is None
            else {self.task_id + "-empty-dir": self.hard_disk_path}
        )

    @property
    def volumes(self) -> Dict[str, str]:
        return {
            **self.hard_disk_paths,
            **(
                {"argo-artifacts": self.get_artifact_dir()}
                if self.artifact_properties
                else {}
            ),
        }

    def gen_template(self, template: Dict[str, Any]) -> Dict[str, Any]:
        final_template_name = self.final_template_name
        template["inputs"]["parameters"] = [
            {"name": parameter} for parameter in sorted(self.final_parameters)
        ]
        if "labels" not in template["metadata"]:
            template["metadata"]["labels"] = {}
        template["metadata"]["labels"]["app"] = "-".join(
            [self.dag.project_name, self.dag.dag_id, final_template_name]
        )
        if "name" not in template:
            template["name"] = final_template_name
        limits = {}
        if self.limits_cpu is not None:
            limits["cpu"] = self.limits_cpu
        if self.limits_memory is not None:
            limits["memory"] = self.limits_memory
        template["container"]["resources"] = {
            "requests": {"cpu": self.requests_cpu, "memory": self.requests_memory},
            **({"limits": limits} if limits else {}),
        }
        if self.volumes:
            template["container"]["volumeMounts"] = template["container"].get(
                "volumeMounts", []
            ) + [
                {"mountPath": self.volumes[k], "name": k} for k in sorted(self.volumes)
            ]
        if self.retries > 0:
            template["retryStrategy"] = {"limit": self.retries, "retryPolicy": "Always"}
        if self.artifact_properties:
            template["outputs"] = {
                "parameters": [
                    {
                        "name": artifact_name,
                        "valueFrom": {
                            "path": self.artifact_property_to_path(artifact_name)
                        },
                    }
                    for artifact_name in sorted(self.artifact_properties)
                ]
            }
        return template

    def to_argo_dict(self) -> Dict[str, Any]:
        sc = self.upstream_short_circuits
        return {
            "name": self.task_id,
            "dependencies": [
                t.task_id for t in sorted(self.upstream_tasks, key=lambda t: t.task_id)
            ],
            "templateRef": {
                "name": self.dag.project_name,
                "template": self.final_template_name,
            },
            "arguments": {
                "parameters": [
                    {"name": "task-id", "value": self.task_id},
                    {"name": "dag-id", "value": self.dag.dag_id},
                ]
                + [{"name": k, "value": v} for k, v in sorted(self.parameters.items())]
            },
            **(
                {
                    "when": " && ".join(
                        [
                            (
                                "!{{tasks."
                                + t.task_id
                                + ".outputs.parameters."
                                + "short_circuit"
                                + "}}"
                            )
                            for t in sc
                        ]
                    )
                }
                if sc
                else {}
            ),
        }

    @property
    def df_next_or_end(self) -> Dict[str, Any]:
        if len(self.downstream_tasks) == 0:
            return {"End": True}
        elif len(self.downstream_tasks) == 1:
            return {"Next": self.downstream_tasks[0].task_id}
        else:
            raise AirgoInstantiationException(
                f"Task {self.task_id} has more than one downstream task"
            )

    def to_sf_dict(self) -> Dict[str, Any]:
        template = get_configuration_template(
            "step_functions_task_template.yaml.j2"
        ).render(
            CLUSTER_ARN=self.dag.aws_ecs_cluster_arn,
            PROJECT_NAME=self.dag.project_name,
            AWS_REGION=self.dag.aws_region,
            AWS_ID=self.dag.aws_id,
            SUBNET_ID=self.dag.aws_subnet_id,
            SECURITY_GROUP=self.dag.aws_security_group,
            TEMPLATE_NAME=self.template,
        )
        template = yaml.load(
            template,
            Loader=yaml.FullLoader,
        )
        if self.dag.state_machine_default_inputs:
            template["Parameters"]["Overrides"]["Cpu"] = self.limits_cpu
            template["Parameters"]["Overrides"]["Memory"] = self.limits_memory
            container_override = template["Parameters"]["Overrides"][
                "ContainerOverrides"
            ][0]
            container_override["Environment"].extend(
                [
                    {"Name": k, "Value.$": f"$.dagInputs.{k}"}
                    for k in self.dag.state_machine_default_inputs.keys()
                ]
                + [
                    {"Name": "DAG_ID", "Value": self.dag.dag_id},
                    {"Name": "TASK_ID", "Value": self.task_id},
                ]
            )
        if self.retries > 0:
            template["Retry"] = [
                {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 3,
                    "MaxAttempts": self.retries,
                    "BackoffRate": 2,
                }
            ]
        if self.artifact_properties:
            template["ResultPath"] = "$.artifacts"
        else:
            template["ResultPath"] = None
        template.update(**self.df_next_or_end)
        return template

    def should_execute(self, execution_set: Dict[str, Set[str]]) -> bool:
        upstream_shorted = self.get_all_relative_ids(upstream=True).intersection(
            execution_set["short_circuited"]
        )
        return (
            self.task_id not in execution_set["executed"] and len(upstream_shorted) == 0
        )

    def execute_syncronously(
        self, execution_set: Dict[str, Set[str]], context: Dict[str, datetime]
    ) -> None:
        for dep in self.upstream_tasks:
            dep.execute_syncronously(execution_set, context)
        if self.should_execute(execution_set):
            self.logger.info(f"Executing task {self.task_id}")
            self.base_execute(context)
            execution_set["executed"].add(self.task_id)

    @local_artifact_directory
    def execute_all(self) -> None:
        self.execute_syncronously({}, self.dag.default_context)

    def save_artifacts(self) -> None:
        res = {}
        for property_name in self.artifact_properties:
            res[property_name] = getattr(self, property_name)
        if self.dag.project_type == "step-functions":
            import boto3

            client = boto3.client("stepfunctions")
            client.send_task_success(
                taskToken=os.environ["TASK_TOKEN"],
                output=json.dumps(res),
            )

    def ensure_hd_dir(self) -> None:
        if self.hard_disk_path is not None:
            if not os.path.exists(self.hard_disk_path):
                os.makedirs(self.hard_disk_path)

    def base_execute(self, context: Dict[str, Any]) -> None:
        self.ensure_hd_dir()
        self.execute(context)
        self.save_artifacts()
