#!/bin/python3

from airgo import traverse_dags_folder
import os

working_dir = os.getcwd()
dags_dir = os.getenv("DAGS_DIRECTORY", os.path.join(working_dir, "dags"))
tags = {k: os.getenv(k) for k in ["DAG_ID", "TASK_ID"]}


def create_context(dag):
    context = dag.default_context
    context.update(**tags)
    return tags


if __name__ == "__main__":
    dags = traverse_dags_folder(dags_dir)
    dag = dags[os.environ["DAG_ID"]]
    dag.task_map[os.environ["TASK_ID"]].base_execute(context=create_context(dag))
