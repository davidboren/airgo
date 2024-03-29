from typing import Optional, List, Dict, Any, Union, Tuple

from collections import deque, defaultdict
from cached_property import cached_property  # type: ignore
from copy import deepcopy
import json
import datetime as dt
import yaml
import os

from airgo.operators.base_operator import BaseOperator
from airgo.utils.decorators import local_artifact_directory
from airgo.exceptions import AirgoInstantiationException
from airgo.utils.traversal import (
    get_project_config,
    get_argo_container_templates,
    get_configuration_template,
)
from airgo.utils.k8 import k8_str_test
from datetime import datetime


script_dir = os.path.dirname(__file__)


class literal_str(str):
    pass


def literal_presenter(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(literal_str, literal_presenter)


class DAG:
    """
    Class for defining an Airgo dag (Argo cronworkflow/workflow).

    :param dag_id: Unique identifier for the dag.
    :type dag_id: str
    :param schedule_interval: Cron-notation string used for scheduling dag (if not given,
                              no cronworkflow will be given but a workflow template will be rendered).
    :type schedule_interval: str
    :param default_args: Dictionary of default keyword arguments to be passed into every task in the
                         dag (if not overridden by the task itself).  Usually this is cpu and memory requests.
    :type default_args: dict
    :param concurrency_policy: String defining what to do when a new cron dag run starts while an old one is still
                               running.  Can be 'Allow', 'Replace', or 'Forbid'
    :type concurrency_policy: str
    :param start_date: Date object defining the first run for the dag (useful ONLY for backfilling)
    :type start_date: str
    :type max_active_runs: None
    :param max_active_runs: Deprecated argument.  Use 'concurrency_policy' instead.
    """

    REGISTERED_DAGS: List[str] = []
    DATE_FORMATS = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S %z UTC",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    EMPTY_DIR_VOLUME_NAME = "empty-dir-volume"

    def __init__(
        self,
        dag_id: str,
        description: str,
        schedule_interval: Optional[Union[str, List[str]]] = None,
        default_args: Dict[str, Any] = None,
        concurrency_policy: str = "Forbid",
        start_date: Union[str, dt.datetime, None] = None,
        max_active_runs: Optional[int] = None,
        state_machine_default_inputs: Optional[Dict[str, Any]] = None,
        state_machine_email_notification_on_failure: Optional[str] = None,
    ) -> None:
        self.dag_id = dag_id
        if self.dag_id in DAG.REGISTERED_DAGS:
            raise AirgoInstantiationException(
                f"Dag with id `{self.dag_id}` already registered"
            )
        if max_active_runs is not None:
            raise AirgoInstantiationException(
                "Max active runs is deprecated. Set concurrency_policy instead to 'Allow', 'Replace', or 'Forbid'"
            )
        self.start_date = (
            self.format_date(start_date, "start_date")
            if isinstance(start_date, str)
            else start_date
        )
        DAG.REGISTERED_DAGS.append(self.dag_id)
        self.description = description
        self.concurrency_policy = concurrency_policy
        self.schedule_interval = schedule_interval
        self.default_args = {} if default_args is None else default_args
        self.tasks: List[BaseOperator] = []
        self.max_active_runs = max_active_runs
        self.state_machine_default_inputs = state_machine_default_inputs or {}
        self.state_machine_email_notification_on_failure = (
            state_machine_email_notification_on_failure
        )
        state_machine_default_inputs = state_machine_default_inputs or {}
        for k, v in state_machine_default_inputs.items():
            if not isinstance(v, str):
                raise AirgoInstantiationException(
                    f"State machine default input values must be strings, not {type(v)}"
                )
            if not isinstance(k, str):
                raise AirgoInstantiationException(
                    f"State machine default input keys must be strings, not {type(k)}"
                )

    @property
    def concurrency_policy(self) -> str:
        return self._concurrency_policy

    @concurrency_policy.setter
    def concurrency_policy(self, concurrency_policy):
        if concurrency_policy not in ["Allow", "Replace", "Forbid"]:
            raise AirgoInstantiationException(
                "Set concurrency_policy instead to 'Allow', 'Replace', or 'Forbid'"
            )
        self._concurrency_policy = concurrency_policy

    @property
    def dag_id(self) -> str:
        return self._dag_id

    @dag_id.setter
    def dag_id(self, dag_id):
        if not isinstance(dag_id, str):
            raise AirgoInstantiationException(
                f"Your dag_id must be a string, not {type(dag_id)}"
            )

        k8_str_test(dag_id, "dag_id", self.project_type)
        self._dag_id = dag_id

    def add_task(self, task: BaseOperator) -> None:
        if any(t.task_id == task.task_id for t in self.tasks):
            raise AirgoInstantiationException(
                f"Task with id {task.task_id} has already been registered with DAG {self.dag_id}"
            )

        self.tasks.append(task)

    @cached_property
    def execution_date(self):
        creation_timestamp = os.getenv("CREATION_TIMESTAMP_OVERRIDE")
        creation_timestamp = (
            creation_timestamp
            if creation_timestamp
            else os.getenv("CREATION_TIMESTAMP")
        )
        return (
            self.format_date(creation_timestamp)
            if creation_timestamp
            else dt.datetime.utcnow()
        )

    @property
    def task_by_tree_depth(self) -> Dict[str, int]:
        initial_nodes = [v for v in self.tasks if len(v.upstream_tasks) == 0]
        queue = deque()
        queue.extend([(n, 0) for n in initial_nodes])
        traversed_nodes = set()
        tasks_by_depth = defaultdict(lambda: [])
        while queue:
            node, depth = queue.popleft()
            if node.task_id not in traversed_nodes:
                tasks_by_depth[depth].append(node)
                traversed_nodes.add(node.task_id)
            for child in node.downstream_tasks:
                queue.append((child, depth + 1))
        return tasks_by_depth

    @classmethod
    def format_date(
        cls, date: str, parameter_name: Optional[str] = None
    ) -> dt.datetime:
        correct_format = False
        if isinstance(date, str):
            for format_ in cls.DATE_FORMATS:
                try:
                    new_date = dt.datetime.strptime(date, format_)
                    correct_format = True
                    break

                except ValueError:
                    pass
        if not correct_format:
            raise AirgoInstantiationException(
                f"{f'Parameter {parameter_name}' if parameter_name else date} must be a datetime object or a string formatted as any of the following: {cls.DATE_FORMATS}"
            )

        return new_date

    @property
    def can_backfill(self) -> bool:
        return self.schedule_interval is not None and self.start_date is not None

    @property
    def task_map(self) -> Dict[str, BaseOperator]:
        return {task.task_id: task for task in self.tasks}

    @property
    def formatted_schedule(self):
        return literal_str(
            yaml.dump({"schedule": self.schedule_interval}, default_flow_style=False)
        )

    @property
    def project_config(self):
        return get_project_config()

    @property
    def project_name(self):
        return self.project_config["project_name"]

    @property
    def namespace(self):
        return self.project_config["namespace"]

    @property
    def project_type(self):
        return self.project_config["project_type"]

    @property
    def docker_repo(self):
        return self.project_config["docker_repo"]

    @property
    def aws_id(self):
        return self.project_config["aws_id"]

    @property
    def aws_region(self):
        return self.project_config["aws_region"]

    @property
    def aws_subnet_id(self):
        return self.project_config["aws_subnet_id"]

    @property
    def aws_security_group(self):
        return self.project_config["aws_security_group"]

    @property
    def aws_ecs_cluster_arn(self):
        return self.project_config["aws_ecs_cluster_arn"]

    @property
    def template_names(self):
        return sorted(set([task.final_template_name for task in self.tasks]))

    @property
    def argo_templates(self):
        raw_templates = get_argo_container_templates()
        final_template_tasks = {task.final_template_name: task for task in self.tasks}
        return {
            template_name: task.gen_template(deepcopy(raw_templates[task.template]))
            for template_name, task in sorted(final_template_tasks.items())
        }

    @property
    def volumes(self):
        return sorted(set([k for task in self.tasks for k in task.volumes.keys()]))

    @property
    def argo_workflow(self):
        dag_id = self.dag_id
        workflow = yaml.load(
            get_configuration_template("argo_workflow.yaml.j2").render(
                PROJECT_NAME=self.project_name, NAMESPACE=self.namespace, DAG_ID=dag_id
            ),
            Loader=yaml.FullLoader,
        )
        workflow["spec"] = {**workflow["spec"], **self.workflow_spec}
        return workflow

    @property
    def argo_cron_workflow(self) -> Dict[str, Any]:
        dag_id = self.dag_id
        workflow = yaml.load(
            get_configuration_template("argo_cron_workflow.yaml.j2").render(
                PROJECT_NAME=self.project_name,
                NAMESPACE=self.namespace,
                DAG_ID=dag_id,
                SCHEDULE=self.schedule_interval,
            ),
            Loader=yaml.FullLoader,
        )
        workflow["spec"]["workflowSpec"] = {
            **workflow["spec"]["workflowSpec"],
            **self.workflow_spec,
        }
        return workflow

    @property
    def workflow_spec(self) -> Dict[str, Any]:
        spec: Dict[str, Any] = {"templates": []}
        spec["entrypoint"] = self.dag_id
        sorted_tasks = [
            task.to_argo_dict() for task in sorted(self.tasks, key=lambda t: t.task_id)
        ]
        spec["podGC"] = {"strategy": "OnWorkflowCompletion"}
        if self.volumes:
            spec["volumes"] = spec.get("volumes", []) + [
                {"emptyDir": {}, "name": k} for k in sorted(self.volumes)
            ]
        spec["templates"].append({"name": self.dag_id, "dag": {"tasks": sorted_tasks}})
        return spec

    @property
    def state_machine_definition(self):
        return json.dumps(
            {
                "StartAt": "__DEFINE_DEFAULTS",
                "States": {task.task_id: task.to_sf_dict() for task in self.tasks},
            }
        )

    def get_schedule_event_rule(self, cron_schedule):
        return {
            "Type": "AWS::Events::Rule",
            "Properties": {
                "Description": f"Scheduled event on with cron schedule {cron_schedule}",
                "ScheduleExpression": cron_schedule
                if cron_schedule.startswith("cron(")
                else f"cron({cron_schedule})",
                "State": "ENABLED",
                "Targets": [
                    {
                        "Arn": {"Ref": "StateMachine"},
                        "Id": {"Fn::GetAtt": ["StateMachine", "Name"]},
                        "RoleArn": {"Fn::GetAtt": ["ScheduledEventIAMRole", "Arn"]},
                    }
                ],
            },
        }

    @property
    def state_machine_schedule_events(self):
        if isinstance(self.schedule_interval, str):
            return {
                f"ScheduleEventRule1": self.get_schedule_event_rule(
                    self.schedule_interval
                )
            }
        elif isinstance(self.schedule_interval, list):
            return {
                f"ScheduleEventRule{i+1}": self.get_schedule_event_rule(cron)
                for i, cron in enumerate(self.schedule_interval)
            }

    @property
    def state_machine(self):
        state_machine = yaml.load(
            get_configuration_template(
                "step_functions_stepfunction_template.yaml.j2"
            ).render(
                PROJECT_NAME=self.project_name,
                DAG_NAME=self.dag_id,
                STATE_MACHINE_DEFINITION=json.dumps(self.state_machine_definition),
            ),
            Loader=yaml.FullLoader,
        )
        if self.schedule_interval:
            state_machine["Resources"].update(**self.state_machine_schedule_events)
        if self.state_machine_email_notification_on_failure:
            state_machine["Resources"]["DAGFailureNotificationTopic"]["Properties"][
                "Subscription"
            ] = [
                {
                    "Protocol": "email",
                    "Endpoint": self.state_machine_email_notification_on_failure,
                }
            ]
        return state_machine

    @property
    def default_context(self) -> Dict[str, dt.datetime]:
        return {"execution_date": self.execution_date}

    @local_artifact_directory
    def execute(self) -> None:
        """
        Attempts to execute all upstream dependencies in order
        """
        execution_set: Dict[str, set] = {"executed": set(), "short_circuited": set()}
        for task in [t for t in self.tasks if not t.downstream_tasks]:
            task.execute_syncronously(execution_set, self.default_context)
