from airgo import traverse_dags_folder
from airgo.rendering import render_step_function_workflows
from airgo.utils.traversal import get_project_config

import pytest
import os


@pytest.fixture(scope="function")
def initialized_project(
    mock_get_temp_project_root, run_raise, init_command, temp_project_dir
):
    run_raise(init_command, temp_project_dir)
    return temp_project_dir


def test_init(initialized_project):
    dags_dict = traverse_dags_folder(os.path.join(initialized_project, "dags"))
    assert "hello-world" in dags_dict


@pytest.fixture(scope="function")
def rendered_project(initialized_project, render_command, run_raise):
    run_raise(render_command, initialized_project)
    return initialized_project


def test_rendering(rendered_project):
    assert os.path.exists(rendered_project)


@pytest.fixture(scope="function")
def initialized_stepfunction_project(
    mock_get_temp_project_root,
    run_raise,
    init_stepfunction_command,
    temp_project_dir,
):
    run_raise(init_stepfunction_command, temp_project_dir)
    return temp_project_dir


@pytest.fixture(scope="function")
def rendered_stepfunction_project(
    initialized_stepfunction_project, render_command, run_raise
):
    run_raise(render_command, initialized_stepfunction_project)
    return initialized_stepfunction_project


def test_stepfunction_init(initialized_stepfunction_project):
    dags_dict = traverse_dags_folder(
        os.path.join(initialized_stepfunction_project, "dags")
    )
    assert "hello-world" in dags_dict


def test_stepfunction_rendering(
    initialized_stepfunction_project,
):
    dag_dict = traverse_dags_folder(
        os.path.join(initialized_stepfunction_project, "dags")
    )

    rendered_yamls_dir = os.path.join(
        initialized_stepfunction_project, "airgo", "rendered_yamls"
    )
    render_step_function_workflows(
        project_config=get_project_config(),
        dags=dag_dict,
        rendered_yamls_dir=rendered_yamls_dir,
    )
    for k in dag_dict:
        assert os.path.exists(
            os.path.join(rendered_yamls_dir, "scheduled_workflows", k + ".yaml")
        )


def test_stepfunction_rendering_command(rendered_stepfunction_project):
    assert os.path.exists(rendered_stepfunction_project)
