from jinja2 import Environment, FileSystemLoader, DebugUndefined
from airgo.utils.k8 import k8_str_filter
import configparser
import yaml
import os


def get_root_dir():
    return os.getcwd()


def get_project_template_dir():
    return os.path.join(get_root_dir(), "airgo", "templates")


def get_container_templates_dir():
    return os.path.join(get_project_template_dir(), "containers")


def get_workflow_templates_dir():
    return os.path.join(get_project_template_dir(), "configuration")


def get_config_path():
    return os.path.join(get_root_dir(), "setup.cfg")


def get_full_config():
    config = configparser.ConfigParser()
    config.read_file(open(get_config_path()))
    return config


def get_project_config():
    base_config = get_full_config()["airgo"]
    return {
        k: v.replace("_", "-") if base_config["project_type"] == "argo" else v
        for k, v in base_config.items()
    }


def get_argo_container_templates():
    dict_ = {}
    container_templates_dir = get_container_templates_dir()
    for filename in os.listdir(container_templates_dir):
        if filename.endswith("j2"):
            f = open(os.path.join(container_templates_dir, filename), "r")
            with open(os.path.join(container_templates_dir, filename), "r") as f:
                dict_[
                    k8_str_filter(filename.replace(".yaml.j2", ""), "argo")
                ] = yaml.load(f, Loader=yaml.FullLoader)
    return dict_


def get_template(dirpath, filename):
    """
    Function for loading a jinja2 template from a directory

    :Returns: Jinja2 template
    :Usage: >>> service_template = get_template("service.j2")
    """
    return Environment(
        loader=FileSystemLoader(dirpath), undefined=DebugUndefined
    ).get_template(filename)


def get_configuration_template(filename):
    return get_template(get_workflow_templates_dir(), filename)
