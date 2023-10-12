from airgo import DAG
from airgo.operators import BaseOperator
from airgo.utils.decorators import artifact_property
from airgo.operators.parallel import RuntimeParallelizeOperator


class ArtifactClassOperator(BaseOperator):
    def execute(self, context):
        self.logger.info(f"Executing {self.task_id}")
        try:
            self.dag.EXECUTED_TASKS.append(self.task_id)
        except AttributeError:
            self.dag.EXECUTED_TASKS = [self.task_id]

    @artifact_property()
    def stuff(self):
        return [i for i in range(10)]


class Parameterized(RuntimeParallelizeOperator):
    def execute(self, context):
        self.logger.info(f"Executing {self.task_id}")
        self.dag.EXECUTED_TASKS.append(self.task_id)
        try:
            self.dag.EXECUTED_PARAMETERS.append(self.parameter)
        except AttributeError:
            self.dag.EXECUTED_PARAMETERS = [self.parameter]


def test_artifact_parallelism(mock_get_project_config, default_args):
    dag = DAG(
        dag_id="parallel-dag",
        description="test parallelism",
        schedule_interval=None,
        default_args=default_args,
    )
    artifact_generator = ArtifactClassOperator(task_id="artifact-generator", dag=dag)
    parameterized = Parameterized(
        task_id="parameter-",
        runtime_task=artifact_generator,
        property_name="stuff",
        dag=dag,
    )

    dag.execute()

    assert dag.EXECUTED_TASKS == [artifact_generator.task_id] + [
        parameterized.task_id
    ] * len(artifact_generator.stuff)
    assert dag.EXECUTED_PARAMETERS == artifact_generator.stuff
