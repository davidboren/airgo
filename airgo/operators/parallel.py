import os
from contextlib import contextmanager
from airgo.utils.serialization import json_serializer, json_deserializer

from airgo.operators.base_operator import BaseOperator
from airgo.exceptions import AirgoException


class RuntimeParallelizeOperator(BaseOperator):
    """
    Class for parallelizing a task based on the artifact output of another task.

    :param runtime_task: Task that will generate the artifact over which we will parallelize
    :type runtime_task: BaseOperator
    :param property_name: Name of the property of the task that generates the parallelizable data.
    :type property_name: str
    """

    def __init__(
        self,
        runtime_task,
        property_name,
        artifact_serializer=json_serializer,
        artifact_deserializer=json_deserializer,
        **kwargs,
    ):
        self.runtime_task = runtime_task
        self.property_name = property_name
        self.artifact_deserializer = artifact_deserializer
        self.artifact_serializer = artifact_serializer
        if property_name not in dir(runtime_task):
            raise AirgoException(
                f"Property '{property_name}' does not exist on task '{runtime_task.task_id}'"
            )
        if "parameters" in kwargs:
            kwargs["parameters"][self.parameter_name] = "{{item}}"
        else:
            kwargs["parameters"] = {self.parameter_name: "{{item}}"}
        super().__init__(**kwargs)
        self.set_upstream(runtime_task)

    @contextmanager
    def _param_env(self, new_value):
        previous_value = os.getenv(self.env_name)
        os.environ[self.env_name] = new_value
        try:
            yield
        finally:
            if previous_value is not None:
                os.environ[self.env_name] = previous_value
            else:
                del os.environ[self.env_name]

    @property
    def hashable_attribute_names(self):
        return super().hashable_attribute_names + ["parameter_name"]

    @property
    def parameter_name(self):
        return self.runtime_task.task_id + "-" + self.property_name

    @property
    def env_name(self):
        return (
            (self.runtime_task.task_id + "_" + self.property_name)
            .upper()
            .replace("-", "_")
        )

    @property
    def parameter(self):
        return self.artifact_deserializer(os.getenv(self.env_name))

    def gen_template(self, template):
        template = super().gen_template(template)
        for env_dict in template["container"]["env"]:
            if env_dict["name"] == self.env_name:
                raise AirgoException(
                    f"Parameter Env variable {self.env_name} already set in template."
                )
        template["container"]["env"].append(
            {
                "name": self.env_name,
                "value": "{{inputs.parameters." + self.parameter_name + "}}",
            }
        )
        return template

    def to_sf_dict(self):
        core_sf_dict = super().to_sf_dict()
        next_state = core_sf_dict.pop("Next")
        if next_state is None:
            core_sf_dict.pop("End")
            next_or_end = {"End": True}
        else:
            next_or_end = {"Next": next_state}
            core_sf_dict["Parameters"]["Overrides"]["ContainerOverrides"][0][
                "Environment"
            ].append({"Name": "ELEMENT", "Value": "$.element"})
        return {
            "Type": "Map",
            "MaxConcurrency": self.dag.max_active_runs,
            "InputPath": "$",
            "ItemsPath": f"$.artifacts.{self.property_name}",
            "Parameters": {
                "element.$": "$$.Map.Item.Value",
            },
            "Iterator": {
                "StartAt": self.task_id,
                "States": {self.task_id: {**core_sf_dict, "End": True}},
            },
            **next_or_end,
        }

    def to_dict(self):
        dict_ = super().to_dict()
        dict_["withParam"] = (
            "{{tasks."
            + self.runtime_task.task_id
            + ".outputs.parameters."
            + self.property_name
            + "}}"
        )
        return dict_

    def execute_syncronously(self, execution_set, context):
        for dep in self.upstream_tasks:
            dep.execute_syncronously(execution_set, context)
        if self.should_execute(execution_set):
            self.logger.info(f"Executing task {self.task_id}")
            for param in getattr(self.runtime_task, self.property_name):
                with self._param_env(self.artifact_serializer(param)):
                    self.base_execute(context)
            execution_set["executed"].add(self.task_id)
