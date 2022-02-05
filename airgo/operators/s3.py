import os
from abc import ABCMeta, abstractmethod
from typing import Union, Dict, Generator, Tuple, Any
import boto3.s3.transfer as transfer  # type: ignore
from boto3 import resource  # type: ignore
from cached_property import cached_property  # type: ignore
import pandas as pd  # type: ignore

from airgo.exceptions import AirgoInstantiationException, AirgoRuntimeException
from airgo.operators.base_operator import BaseOperator
from airgo.operators.short_circuit import ShortCircuitOperator
from airgo.utils.decorators import apply_defaults

S3_PREFIX = "s3://"


class DatToS3Operator(ShortCircuitOperator, BaseOperator, metaclass=ABCMeta):

    """
    Task for outputting dataframes to s3 via a generator property called `output`.
    Must be used via subclassing.  This subclass MUST have a property called 'output'
    that yields pandas dataframes in chunks. These chunks are written to a file and
    pushed to an s3 path defined by the property `s3_path`

    :param hard_disk_path: Hard disk directory path to which to write file.
    :type hard_disk_path: string
    :param fmt: Fmt to which to write dataframes (currently just csvs)
    :type fmt: string
    :param delimiter: Delimeter to use
    :type delimeter: string
    :param on_zero_output: Dictates behavior when zero length outputs are written.  Can be 'short-circuit', 'error', or None
    :type on_zero_output: string
    """

    @apply_defaults
    def __init__(
        self,
        hard_disk_path: str,
        fmt: str = "csv",
        delimiter: str = ",",
        on_zero_output: Union[str, None] = None,
        **kwargs,
    ):
        super().__init__(hard_disk_path=hard_disk_path, **kwargs)
        if hard_disk_path is None:
            raise AirgoInstantiationException(
                "Use a non-None hard-disk path when writing using DatToS3Operator"
            )
        self.fmt = fmt
        self.delimiter = delimiter
        self.on_zero_output = on_zero_output
        self.has_output: Dict[str, bool] = {}

    @property
    def local_filepath(self) -> str:
        if self.hard_disk_path is None:
            raise AirgoInstantiationException(
                "Use a non-None hard-disk path when writing using DatToS3Operator"
            )
        return os.path.join(self.hard_disk_path, self.task_id) + "." + self.fmt

    @property
    def on_zero_output(self) -> Union[str, None]:
        return self._on_zero_output

    @on_zero_output.setter
    def on_zero_output(self, on_zero_output: Union[str, None]) -> None:
        if on_zero_output is not None:
            if on_zero_output not in {"short-circuit", "error"}:
                raise AirgoInstantiationException(
                    f"Parameter 'on_zero_output' must be one of: 'short-circuit', 'error', or None.  You have given '{on_zero_output}'"
                )
        self._on_zero_output = on_zero_output

    @staticmethod
    def parse_key(s3key: str) -> Tuple[str, str]:
        if S3_PREFIX not in s3key:
            raise ValueError(f"Your s3key {s3key} has no {S3_PREFIX} prefix...")

        split_list = s3key.split("s3://")[1].split("/")
        if len(split_list) == 1:
            raise ValueError("Your s3key {} has only a bucket...".format(s3key))

        return split_list[0], "/".join(split_list[1:])

    @cached_property
    def s3Resource(self) -> resource:
        return resource("s3")

    @property
    def metadata(self) -> Dict[str, str]:
        return {}

    @property
    @abstractmethod
    def s3_path(self) -> str:
        pass

    @property
    def s3_prefix(self) -> str:
        return self.s3_path

    @property
    @abstractmethod
    def output(self) -> Generator[pd.DataFrame, None, None]:
        pass

    @property
    def short_circuit_when(self) -> bool:
        return self.on_zero_output == "short-circuit" and not self.has_output

    @property
    def transferConfig(self) -> transfer.TransferConfig:
        return transfer.TransferConfig(
            multipart_threshold=8 * 1024 * 1024 ** 5,
            multipart_chunksize=8 * 1024 * 1024,
            max_concurrency=10,
        )

    def chunk_writer(self, i: int, obj: pd.DataFrame, filepath: str) -> None:
        if obj.shape[0] > 0:
            self.has_output[filepath] = True
        self.logger.info(
            f"Writing csv '{i}' with shape {obj.shape} for '{self.task_id}' to filepath '{filepath}'"
        )
        with open(filepath, ("w" if i == 0 else "a")) as f:
            obj.to_csv(f, header=(i == 0), index=False, sep=self.delimiter)

    def upload_to_s3(self, filepath: str, s3path: str) -> None:
        bucket, s3key = self.parse_key(s3path)
        self.logger.info(f"Uploading local file '{filepath}' to s3path '{s3path}'")
        self.s3Resource.meta.client.upload_file(
            filepath,
            bucket,
            s3key,
            Config=self.transferConfig,
            ExtraArgs={"Metadata": self.metadata, "ACL": "bucket-owner-full-control"},
        )
        self.logger.info("Finished uploading file")
        os.remove(filepath)

    def execute(self, context: Dict[str, Any]) -> None:
        for i, obj in enumerate(self.output):
            self.chunk_writer(i, obj, self.local_filepath)
        if os.path.exists(self.local_filepath):
            self.upload_to_s3(self.local_filepath, self.s3_path)
        elif self.on_zero_output == "error":
            raise AirgoRuntimeException(
                "No output produced when required for task {self.task_id}"
            )


class MultiDatToS3(DatToS3Operator):
    """
    Task for outputting multiple dataframes to s3 via a generator property called `output`.
    Must have the same subclassed properties as DatToS3Operator subsclasses. In addition, the
    output property should yield a dictionary of dataframes at every step.  The keys of
    this dictionary should correspond to the output_keys given as an input argument, and these
    keys will be appended to both the `local_filepath` as well as the `s3_path` properties
    to determine their respective local and s3 paths.  These paths are accessible via the `get_s3_path`
    instance function

    :param output_keys: Keys which are expected to come from the output property
    :type output_keys: list of str
    """

    def __init__(self, output_keys: Dict[str, str], **kwargs):
        self.output_keys = output_keys
        super().__init__(**kwargs)

    @property
    def local_filepath(self) -> str:
        if self.hard_disk_path is None:
            raise AirgoInstantiationException(
                "Use a non-None hard-disk path when writing using MultiDatToS3Operator"
            )
        return os.path.join(self.hard_disk_path, self.task_id)

    @property
    def short_circuit_when(self) -> bool:
        return self.on_zero_output == "short-circuit" and not (
            not self.has_output[self.get_local_filepath(k)] for k in self.output_keys
        )

    @property
    def filepaths(self) -> Dict[str, str]:
        return {k: self.get_local_filepath(k) for k in self.output_keys}

    @property
    def s3_paths(self) -> Dict[str, str]:
        return {k: self.get_s3_path(k) for k in self.output_keys}

    def get_local_filepath(self, key) -> str:
        return self.local_filepath + "__" + key

    def get_s3_path(self, key) -> str:
        return self.s3_path + "__" + key

    def execute(self, context: Dict[str, Any]) -> None:
        keyset = set()
        for i, obj in enumerate(self.output):
            if set(obj.keys()) != set(self.output_keys):
                raise AirgoRuntimeException("Output keys ")
            for k, dat in obj.items():
                self.chunk_writer(i, dat, self.get_local_filepath(k))
                keyset.add(k)
        zero_output_keys = set()
        for k in sorted(keyset):
            if os.path.exists(self.get_local_filepath(k)):
                self.upload_to_s3(self.get_local_filepath(k), self.get_s3_path(k))
            else:
                zero_output_keys.add(k)
        if zero_output_keys and self.on_zero_output == "error":
            raise AirgoRuntimeException(
                f"No output produced for keys {zero_output_keys}, which is required for task {self.task_id}"
            )
