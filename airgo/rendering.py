from typing import Dict
import inspect
import os

from importlib.machinery import SourceFileLoader
from airgo.dag import DAG, yaml
from airgo.dag_to_state_machine import convert_dag_to_state_machine
from airgo.utils.traversal import (
    get_configuration_template,
)


def traverse_dags_folder(
    dags_dir: str = os.path.join(os.getcwd(), "dags")
) -> Dict[str, DAG]:
    dags = {}
    DAG.REGISTERED_DAGS = []
    for folder_name, subfolders, filenames in os.walk(dags_dir):
        for filename in [f for f in filenames if f.endswith(".py")]:
            loader = SourceFileLoader(
                os.path.join(folder_name, filename), os.path.join(folder_name, filename)
            )
            module = loader.load_module()  # type: ignore
            for _, value in inspect.getmembers(module):
                if isinstance(value, DAG):
                    dags[value.dag_id] = value
    return dags


def gen_shared_templates(project_name, namespace, workflow_templates):
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": project_name, "namespace": namespace},
        "spec": {
            "templates": [
                workflow_templates[k] for k in sorted(workflow_templates.keys())
            ]
        },
    }


def render_argo_workflows(project_config, dags, rendered_yamls_dir):
    manual_dags = {k: v for k, v in dags.items() if v.schedule_interval is None}
    scheduled_dags = {k: v for k, v in dags.items() if v.schedule_interval is not None}
    backfill_dags = {
        k: v for k, v in scheduled_dags.items() if scheduled_dags[k].can_backfill
    }
    for dag_id, dag in manual_dags.items():
        with open(
            os.path.join(rendered_yamls_dir, "manual_workflows", f"{dag_id}.yaml"), "w"
        ) as f:
            yaml.dump(dag.argo_workflow, f, default_flow_style=False)
    for dag_id, dag in backfill_dags.items():
        with open(
            os.path.join(rendered_yamls_dir, "backfill_workflows", f"{dag_id}.yaml"),
            "w",
        ) as f:
            yaml.dump(dag.argo_workflow, f, default_flow_style=False)

    workflow_templates = {
        template_name: template
        for k in sorted(scheduled_dags.keys())
        for template_name, template in scheduled_dags[k].templates.items()
    }
    with open(os.path.join(rendered_yamls_dir, "scheduled_workflows.yaml"), "w") as f:
        f.write(
            "---\n".join(
                [
                    yaml.dump(
                        scheduled_dags[dag_id].argo_cron_workflow,
                        default_flow_style=False,
                    )
                    for dag_id in sorted(scheduled_dags.keys())
                ]
                + [
                    yaml.dump(
                        gen_shared_templates(
                            project_config["project_name"],
                            project_config["namespace"],
                            workflow_templates,
                        ),
                        default_flow_style=False,
                    )
                ]
            )
        )


def render_step_function_workflows(
    project_config: Dict[str, str],
    dags: Dict[str, DAG],
    rendered_yamls_dir: str,
    templates_dir: str,
):
    for dag in dags.values():
        convert_dag_to_state_machine(dag)
    manual_dags = {k: v for k, v in dags.items() if v.schedule_interval is None}
    scheduled_dags = {k: v for k, v in dags.items() if v.schedule_interval is not None}
    for dag_id, dag in manual_dags.items():
        with open(
            os.path.join(rendered_yamls_dir, "manual_workflows", f"{dag_id}.yaml"), "w"
        ) as f:
            yaml.dump(
                dag.state_machine, f, default_flow_style=False, allow_unicode=True
            )
    for dag_id, dag in scheduled_dags.items():
        with open(
            os.path.join(rendered_yamls_dir, "scheduled_workflows", f"{dag_id}.yaml"),
            "w",
        ) as f:
            yaml.dump(
                dag.state_machine, f, default_flow_style=False, allow_unicode=True
            )

    roles = yaml.load(
        get_configuration_template("step_functions_roles_template.yaml.j2").render(
            PROJECT_NAME=project_config["project_name"],
            AWS_REGION=project_config["aws_region"],
            AWS_ID=project_config["aws_id"],
        ),
        Loader=yaml.BaseLoader,
    )
    with open(os.path.join(rendered_yamls_dir, f"roles.yaml"), "w") as f:
        yaml.dump(roles, f, default_flow_style=False, allow_unicode=True)

    for template_filename in os.listdir(templates_dir):
        if template_filename.endswith("yaml.j2"):
            get_configuration_template(template_filename).render(
                PROJECT_NAME=project_config["project_name"],
                AWS_REGION=project_config["aws_region"],
                AWS_ID=project_config["aws_id"],
            ),
            with open(os.path.join(rendered_yamls_dir, f"roles.yaml"), "w") as f:
                yaml.dump(
                    dag.state_machine, f, default_flow_style=False, allow_unicode=True
                )
