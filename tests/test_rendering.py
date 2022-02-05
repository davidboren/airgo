from airgo import traverse_dags_folder
import pytest
import os


@pytest.fixture(scope="function")
def initialized_project(run_raise, init_command, temp_project):
    run_raise(init_command, temp_project)
    return temp_project


def test_init(initialized_project):
    dags_dict = traverse_dags_folder(os.path.join(initialized_project, "dags"))
    assert "hello-world" in dags_dict


@pytest.fixture(scope="function")
def rendered_project(initialized_project, render_command, run_raise):
    run_raise(render_command, initialized_project)
    return initialized_project


def test_rendering(rendered_project):
    assert os.path.exists(rendered_project)


# def test_backfill(rendered_project, airgo_command, image_tag, is_circle, run_raise):
#     if not is_circle:
#         run_raise(["pipenv", "lock"], rendered_project)
#         run_raise(["docker", "build", "--build-arg", f"GITHUB_OAUTH_TOKEN={os.getenv('GITHUB_OAUTH_TOKEN')}", "-t", image_tag, "."], rendered_project)
#         run_raise(["docker", "push", image_tag], rendered_project)
#         backfill_command = airgo_command + ['backfill', '--dag-id', 'hello-world', '--start-date', '"{}"'.format(dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))]
#         run_raise(backfill_command, rendered_project)
#         run_raise(airgo_command + ['watch', 'hello-world'], rendered_project)
