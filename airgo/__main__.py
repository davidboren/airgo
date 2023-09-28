from airgo.utils.traversal import get_project_config, get_config_path, get_full_config
from airgo.exceptions import AirgoException
from airgo.rendering import (
    traverse_dags_folder,
    render_argo_workflows,
    render_step_function_workflows,
)
from airgo.dag import DAG
from shutil import copy
import configparser
import click
import json
import hashlib
import os
import subprocess
import croniter
import datetime as dt
import pytz
import tempfile
import yaml

__author__ = "David Boren"
script_dir = os.path.dirname(__file__)
cwd = os.getcwd()
airgo_dir = os.path.join(cwd, "airgo")
templates_dir = os.path.join(airgo_dir, "templates")
rendered_yamls_dir = os.path.join(airgo_dir, "rendered_yamls")
dags_dir = os.path.join(cwd, "dags")
tests_dir = os.path.join(cwd, "tests")


def copy_template(template_filename, subdir, new_name=None, overwrite=False):
    target_dir = cwd if subdir is None else os.path.join(cwd, subdir)
    new_name = template_filename if new_name is None else new_name
    target_path = os.path.join(target_dir, new_name)
    if overwrite or not os.path.exists(target_path):
        copy(os.path.join(script_dir, "templates", template_filename), target_path)


def gen_sha():
    data = []
    for folder_name, subfolders, filenames in os.walk(rendered_yamls_dir):
        for filename in filenames:
            if os.path.splitext(filename)[1] != ".yaml":
                continue

            with open(os.path.join(folder_name, filename), "rb") as f:
                data.append(f.read().decode("utf8"))
    if len(data) == 0:
        return None

    obj = json.dumps(data, sort_keys=True)
    return hashlib.sha256(obj.encode("utf8")).hexdigest()


def get_commit_sha():
    return subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("utf-8").strip()


def submit_workflow(workflow_data, parameters):
    tf = tempfile.NamedTemporaryFile(delete=True)
    with open(tf.name, "w") as temp:
        yaml.dump(
            yaml.load(
                workflow_data.replace("${Image_Tag}", f"sha-{get_commit_sha()}"),
                Loader=yaml.FullLoader,
            ),
            temp,
            default_flow_style=False,
        )
    param_subcommand = [["-p", f'{k}="{v}"'] for k, v in parameters.items()]
    submit_command = [
        "argo",
        "submit",
        tf.name,
        *[el for list_ in param_subcommand for el in list_],
    ]
    p = subprocess.Popen(
        submit_command, cwd=cwd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE
    )
    res = p.communicate()
    if p.returncode != 0:
        raise Exception(res[0])


def get_workflow_by_name(dag_id, dags_dir):
    dags = traverse_dags_folder(dags_dir)
    if dag_id not in dags:
        raise AirgoException(f"Dag-id '{dag_id}' not found in dags folder '{dags_dir}'")

    dag = dags[dag_id]
    backfill_path = os.path.join(
        rendered_yamls_dir, "backfill_workflows", f"{dag.dag_id}.yaml"
    )
    if not os.path.exists(backfill_path):
        raise AirgoException(
            f"Dag-id '{dag_id}' has no backfill yaml.  Have you rendered your airgo lately?"
        )

    with open(backfill_path, "r") as f:
        return f.read()


def make_if_not(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


@click.group()
def main():
    """
    CLI for generating kubernetes dags from simple python code
    """
    pass


@main.command()
@click.option("--project-name", help="Project name used in argo yaml construction.")
@click.option(
    "--namespace",
    default="argo-events",
    help="Namespace for argo-events. Defaults to 'argo-events'",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Whether to overwrite existing files. Defaults to False",
)
@click.option("--project-type", default="argo", help="Either argo or step-functions")
@click.option(
    "--docker-repo",
    default="",
    help="Repository for docker image.  Defaults to empty string.",
)
@click.option(
    "--aws-subnet-id",
    default="",
    help="Subnet ID to use for Step Functions.  Defaults to empty string.",
)
@click.option(
    "--aws-security-group",
    default="",
    help="Security Group to use for Step Functions.  Defaults to empty string.",
)
@click.option(
    "--aws-id",
    default="",
    help="AWS ID to use for Step Functions.  Defaults to empty string.",
)
@click.option(
    "--aws-region",
    default="",
    help="AWS Region to use for Step Functions.  Defaults to empty string.",
)
@click.option(
    "--aws-ecs-cluster-arn",
    default="",
    help="AWS ECS Cluster to use for Step Functions.  Defaults to empty string.",
)
def init(
    project_name,
    namespace,
    overwrite,
    project_type,
    docker_repo,
    aws_id,
    aws_region,
    aws_subnet_id,
    aws_security_group,
    aws_ecs_cluster_arn,
):
    """
    Inits airgo directory with init (with project_name specification) and templates,
    a dags folder with a simple hello_world dag, and a simple test for dag traversal.
    """
    make_if_not(airgo_dir)
    make_if_not(templates_dir)
    make_if_not(os.path.join(templates_dir, "containers"))
    make_if_not(os.path.join(templates_dir, "configuration"))
    make_if_not(rendered_yamls_dir)
    make_if_not(os.path.join(rendered_yamls_dir, "manual_workflows"))
    make_if_not(os.path.join(rendered_yamls_dir, "scheduled_workflows"))
    make_if_not(os.path.join(rendered_yamls_dir, "backfill_workflows"))
    make_if_not(dags_dir)
    make_if_not(tests_dir)
    if project_type == "argo":
        with open(
            os.path.join(script_dir, "templates", "argo_default_template.yaml.j2"), "r"
        ) as f:
            default_container = f.read()
        if docker_repo:
            with open(
                os.path.join(
                    airgo_dir,
                    "templates",
                    "containers",
                    "default_template.yaml.j2",
                ),
                "w",
            ) as f:
                f.write(
                    default_container.replace(
                        "${SET IMAGE HERE}", f"{docker_repo}:${{Image_Tag}}"
                    )
                )
        copy_template(
            "argo_workflow.yaml.j2",
            os.path.join("airgo", "templates", "configuration"),
            overwrite=overwrite,
        )
        copy_template(
            "argo_cron_workflow.yaml.j2",
            os.path.join("airgo", "templates", "configuration"),
            overwrite=overwrite,
        )
    elif project_type == "step-functions":
        with open(
            os.path.join(
                script_dir,
                "templates",
                "step_functions_taskdefinition_template.yaml.j2",
            ),
            "r",
        ) as f:
            default_task = f.read()
        with open(
            os.path.join(
                airgo_dir,
                "templates",
                "containers",
                "default_template.yaml.j2",
            ),
            "w",
        ) as f:
            if docker_repo:
                default_task = default_task.replace(
                    "${SET IMAGE HERE}", f"{docker_repo}:${{Image_Tag}}"
                )

            f.write(default_task)
        copy_template(
            "step_functions_task_template.yaml.j2",
            os.path.join("airgo", "templates", "configuration"),
            overwrite=overwrite,
        )
        copy_template(
            "step_functions_roles_template.yaml.j2",
            os.path.join("airgo", "templates", "configuration"),
            overwrite=overwrite,
        )
        copy_template(
            "step_functions_stepfunction_template.yaml.j2",
            os.path.join("airgo", "templates", "configuration"),
            overwrite=overwrite,
        )
    else:
        raise AirgoException(
            f"Project type '{project_type}' not recognized.  Must be either 'argo' or 'step-functions'"
        )
    copy_template("hello_world.py", "dags", overwrite=overwrite)
    copy_template(
        "dag_traversal.py", "tests", "test_dag_traversal.py", overwrite=overwrite
    )
    copy_template("Dockerfile", None, overwrite=overwrite)
    if os.path.exists(get_config_path()):
        config = get_full_config()
    else:
        config = configparser.ConfigParser()
    if project_type == "argo":
        config["airgo"] = {
            "project_name": project_name,
            "project_type": project_type,
            "namespace": namespace,
            "docker_repo": docker_repo,
        }
    elif project_type == "step-functions":
        config["airgo"] = {
            "project_name": project_name,
            "project_type": project_type,
            "docker_repo": docker_repo,
            "aws_id": aws_id,
            "aws_region": aws_region,
            "aws_security_group": aws_security_group,
            "aws_subnet_id": aws_subnet_id,
            "aws_ecs_cluster_arn": aws_ecs_cluster_arn,
        }
    with open(get_config_path(), "w") as configfile:
        config.write(configfile)


@main.command()
@click.option(
    "--hash-check",
    is_flag=True,
    help="Raise non-zero exit code if rendering differs from previous rendering.",
)
def render(hash_check):
    """
    Renders workflow sensors, calendar configmap, and calendar gateway into the
    airgo rendered_yamls directory.
    """
    project_config = get_project_config()
    dags = traverse_dags_folder(dags_dir)
    if not dags:
        raise AirgoException("Your dags folder has no dag instances specified...")

    if hash_check:
        old_sha = gen_sha()
    if project_config["project_type"] == "argo":
        render_argo_workflows(project_config, dags, rendered_yamls_dir)
    else:
        render_step_function_workflows(
            project_config, dags, rendered_yamls_dir, templates_dir
        )
    if hash_check and old_sha != gen_sha():
        raise Exception(
            "Hashes of rendered_yaml directory before and after rendering do not match!"
        )


@main.command()
@click.option("--dag-id", help="Dag-ID for which to run a backfill")
@click.option(
    "--creation-timestamp",
    default=None,
    help=f"UTC workflow creationTimestamp to use (Can be formatted: {DAG.DATE_FORMATS}). Defaults to now.",
)
def submit(dag_id, creation_timestamp):
    """
    Runs single submission of dag.
    """
    workflow_data = get_workflow_by_name(dag_id, dags_dir)
    parameters = {}
    if creation_timestamp is not None:
        creation_datetime = DAG.format_date(creation_timestamp, "start_date")
        parameters = {
            "creationTimestampOverride": creation_datetime.strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
        }
    submit_workflow(workflow_data, parameters)


@main.command()
@click.option("--dag-id", help="Dag-ID for which to run a backfill")
@click.option(
    "--start-date",
    default=None,
    help=f"UTC start DateTime for backfill (Can be formatted: {DAG.DATE_FORMATS}). Defaults to start_date of dag.",
)
@click.option(
    "--end-date",
    default=dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    help="UTC End DateTime for backfill in format '%Y-%m-%d %H:%M:%S'. Defaults to now",
)
def backfill(dag_id, start_date, end_date):
    """
    Runs backfills for a given dag beginning with the dag's start_date parameter and
    ending with either utc now or supplied end_date parameter.
    """
    dags = traverse_dags_folder(dags_dir)
    dag = dags[dag_id]
    if not dag.can_backfill:
        raise AirgoException(
            f"Dag-id '{dag_id}' cannot be backfilled because schedule_interval has a None value"
        )

    workflow_data = get_workflow_by_name(dag_id, dags_dir)
    if dag.start_date is None and start_date is None:
        raise AirgoException(
            f"Dag-id '{dag_id}' has default start date and you have not passed one into the backfill command."
        )

    start_datetime = (
        dag.start_date
        if start_date is None
        else DAG.format_date(start_date, "start_date")
    )
    end_datetime = DAG.format_date(end_date, "end_date")
    dates = [start_datetime]
    cron = croniter.croniter(dag.schedule_interval, start_datetime)
    while True:
        date = cron.get_next(dt.datetime)
        if date > end_datetime:
            break

        if date.tzinfo is not None:
            date = pytz.utc.localize(date)
        dates.append(date)
    date_parameters = [
        {"creationTimestampOverride": date.strftime("%Y-%m-%d %H:%M:%S UTC")}
        for date in dates
    ]
    for parameters in date_parameters:
        submit_workflow(workflow_data, parameters)


if __name__ == "__main__":
    main()
