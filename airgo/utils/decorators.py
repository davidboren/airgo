import inspect
import os
import json

from cached_property import cached_property  # type: ignore
from copy import copy
from functools import wraps

from airgo.utils.serialization import json_list_serializer
from airgo.exceptions import AirgoException
from typing import Callable


def apply_defaults(func: Callable) -> Callable:
    """
    Function decorator that Looks for an argument named "default_args", and
    fills the unspecified arguments from it.
    """
    # Cache inspect.signature for the wrapper closure to avoid calling it
    # at every decorated invocation. This is separate sig_cache created
    # per decoration, i.e. each function decorated using apply_defaults will
    # have a different sig_cache.
    sig_cache = inspect.signature(func)
    non_optional_args = {
        name
        for (name, param) in sig_cache.parameters.items()
        if param.default == param.empty
        and param.name != "self"
        and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)
    }

    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 1:
            raise AirgoException("Use keyword arguments when initializing operators")

        dag_args = {}
        dag = kwargs.get("dag", None)
        if dag:
            dag_args = copy(dag.default_args) or {}
        default_args = {}
        if "default_args" in kwargs:
            default_args = kwargs["default_args"]
        dag_args.update(default_args)
        default_args = dag_args
        for arg in sig_cache.parameters:
            if arg not in kwargs and arg in default_args:
                kwargs[arg] = default_args[arg]
        missing_args = list(non_optional_args - set(kwargs))
        if missing_args:
            msg = "Argument {0} is required".format(missing_args)
            raise AirgoException(msg)

        result = func(*args, **kwargs)
        return result

    return wrapper


ARTIFACT_PROPERTIES = []


def artifact_property(serializer: Callable = json_list_serializer):
    def decorator(func):
        ARTIFACT_PROPERTIES.append(func.__qualname__)
        """
        Property decorator that is called after execution, saving the output to an artifact
        of the same name as the property.
        """

        @cached_property
        @wraps(func)
        def wrapper(self):
            self.logger.info(f"Getting Result for artifact `{func.__qualname__}`")
            res = func(self)
            property_name = self.__class__.qualname_to_property(func.__qualname__)
            if self.dag.project_config.project_type == "argo":
                artifact_dir = self.get_artifact_dir()
                if not os.path.exists(artifact_dir):
                    self.logger.info(f"Creating artifact directory `{artifact_dir}`")
                    os.makedirs(artifact_dir)
                artifact_path = self.__class__.artifact_property_to_path(property_name)
                with open(artifact_path, "w") as f:
                    self.logger.info(
                        f"Writing result of artifact artifact `{func.__qualname__}` to path `{artifact_path}`"
                    )
                    f.write(serializer(res))
                return res
            elif property_name == "short_circuit":
                res = int(res)
            return res

        return wrapper

    return decorator


def local_artifact_directory(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        old_artifact_dir = os.getenv("ARGO_ARTIFACT_DIRECTORY", "")
        os.environ["ARGO_ARTIFACT_DIRECTORY"] = os.path.expanduser("~/.argo-artifacts")
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            if old_artifact_dir:
                os.environ["ARGO_ARTIFACT_DIRECTORY"] = old_artifact_dir
            else:
                os.environ.pop("ARGO_ARTIFACT_DIRECTORY")

    return wrapper
