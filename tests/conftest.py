import pytest
import os
import subprocess

from typing import Dict
from unittest import mock
from tempfile import TemporaryDirectory

from airgo import DAG
from airgo.operators import BaseOperator


@pytest.fixture(scope="session")
def is_ci():
    return os.getenv("CIRCLE_BRANCH") is not None


@pytest.fixture(scope="function")
def default_args():
    return {
        "retries": 1,
        "requests_memory": "10Mi",
        "requests_cpu": "10m",
        "limits_cpu": "20Mi",
        "limits_memory": "20m",
    }


## Mock get_full_config function return value
@pytest.fixture(scope="function")
def mock_get_project_config():
    full_config = {
        "project_name": "test-project",
        "project_type": "argo",
    }
    with mock.patch("airgo.dag.get_project_config", return_value=full_config):
        yield full_config


@pytest.fixture(scope="function")
def example_dag(default_args):
    yield DAG(
        "test-dag",
        description="Hello World!",
        schedule_interval="0 */4 * * *",
        default_args=default_args,
    )

    DAG.REGISTERED_DAGS = []


@pytest.fixture(scope="function")
def test_project_name():
    return "test-project"


@pytest.fixture(scope="function")
def airgo_command(is_ci):
    return ["airgo"] if is_ci else ["python3", "-m", "airgo"]


@pytest.fixture(scope="function")
def docker_repo():
    return "dboren/argo"


@pytest.fixture(scope="function")
def init_command(airgo_command, test_project_name, docker_repo):
    return airgo_command + [
        "init",
        "--project-name",
        test_project_name,
        "--project-type",
        "argo",
        "--docker-repo",
        docker_repo,
    ]


@pytest.fixture(scope="function")
def init_stepfunction_command(airgo_command, test_project_name, docker_repo):
    return airgo_command + [
        "init",
        "--project-name",
        test_project_name,
        "--project-type",
        "step-functions",
        "--docker-repo",
        docker_repo,
        "--aws-subnet-id",
        "subnet-1234",
        "--aws-security-group",
        "sg-1234",
        "--aws-id",
        "1234",
        "--aws-region",
        "us-east-1234",
    ]


@pytest.fixture(scope="function")
def render_command(airgo_command):
    return airgo_command + ["render"]


@pytest.fixture(scope="function")
def image_tag():
    return "latest"


@pytest.fixture(scope="function")
def build_command(docker_repo):
    return ["docker", "build", "-t", f"{docker_repo}:{image_tag}"]


@pytest.fixture(scope="function")
def push_command(image_tag):
    return ["docker", "push", image_tag]


@pytest.fixture(scope="function")
def run_raise():
    def fn(command, cwd):
        p = subprocess.Popen(
            command, cwd=cwd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE
        )
        res = p.communicate()
        if p.returncode != 0:
            raise Exception(res[0])

    return fn


@pytest.fixture(scope="function")
def minikube_context(run_raise, is_ci):
    pass
    # if not is_ci:
    #     return run_raise(
    #         ["kubectl", "uses-context", "minikube-argo-events"], cwd=os.getcwd()
    #     )


@pytest.fixture(scope="function")
def temp_project_dir(init_command, image_tag, is_ci, run_raise, minikube_context):
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


## Mock get_full_config function return value
@pytest.fixture(scope="function")
def mock_get_temp_project_root(temp_project_dir):
    with mock.patch(
        "airgo.utils.traversal.get_root_dir", return_value=temp_project_dir
    ) as m:
        yield m


@pytest.fixture(scope="function")
def task_registration_op():
    class TaskRegistrationOperator(BaseOperator):
        def execute(self, context):
            if not hasattr(self.dag, "EXECUTED_TASKS"):
                self.dag.EXECUTED_TASKS = []
            if self.task_id in self.dag.EXECUTED_TASKS:
                raise Exception("TASK EXECUTED MULTIPLE TIMES!")
            self.logger.info(f"Executing {self.task_id}")
            self.dag.EXECUTED_TASKS.append(self.task_id)

    return TaskRegistrationOperator
