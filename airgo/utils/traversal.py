from jinja2 import Environment, FileSystemLoader, DebugUndefined
from airgo.utils.k8 import k8_str_filter
import configparser
import yaml
import os

script_dir = os.path.dirname(__file__)
project_template_dir = os.path.join(os.getcwd(), "airgo", "templates")
container_templates_dir = os.path.join(project_template_dir, "containers")
workflow_templates_dir = os.path.join(project_template_dir, "configuration")
config_path = os.path.join(os.getcwd(), "setup.cfg")


def get_full_config():
    config = configparser.ConfigParser()
    config.read_file(open(config_path))
    return config


def get_project_config():
    return {k: v.replace("_", "-") for k, v in get_full_config()["airgo"].items()}


def get_container_templates():
    dict_ = {}
    for filename in os.listdir(container_templates_dir):
        if filename.endswith("j2"):
            f = open(os.path.join(container_templates_dir, filename), "r")
            with open(os.path.join(container_templates_dir, filename), "r") as f:
                dict_[k8_str_filter(os.path.splitext(filename)[0])] = yaml.load(
                    f, Loader=yaml.FullLoader
                )
    return dict_


def get_configuration_template(filename):
    """
    Function for loading a jinja2 template from our templates directory

    :Returns: Jinja2 template
    :Usage: >>> service_template = get_template("service.j2")
    """
    return Environment(
        loader=FileSystemLoader(workflow_templates_dir), undefined=DebugUndefined
    ).get_template(filename)
