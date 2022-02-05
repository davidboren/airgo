from abc import abstractmethod
import boto3  # type: ignore
import io
import os
import pandas as pd  # type: ignore
import snowflake.connector  # type: ignore
import json

from cached_property import cached_property  # type: ignore
from sqlalchemy import create_engine, engine as sa_engine  # type: ignore
from snowflake.sqlalchemy import URL  # type: ignore
from snowflake.connector.errors import ProgrammingError  # type: ignore
from typing import Union, Optional, Generator, List, Any, Dict, Callable, Tuple, Set

from airgo.exceptions import AirgoException
from airgo.utils.decorators import apply_defaults
from airgo.dag import DAG
from airgo.operators.base_operator import BaseOperator
from airgo.operators.s3 import DatToS3Operator
from collections import OrderedDict
from functools import reduce
import gzip


class SnowflakeOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        role: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        account: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        dict_cursor: bool = False,
        **kwargs,
    ):
        self.dict_cursor = dict_cursor
        self.defaults = {
            "ROLE": role,
            "DATABASE": database,
            "USER": user,
            "PASSWORD": password,
            "ACCOUNT": account,
            "WAREHOUSE": warehouse,
        }
        super().__init__(**kwargs)

    
    def from_env_or_var(self, var_name: str):
        return self.defaults[var_name] if self.defaults[var_name] else os.environ[f"SNOWFLAKE__{var_name}"]

    @property
    def role(self):
        return self.from_env_or_var("ROLE")

    @property
    def database(self):
        return self.from_env_or_var("DATABASE")

    @property
    def user(self):
        return self.from_env_or_var("USER")

    @property
    def password(self):
        return self.from_env_or_var("PASSWORD")

    @property
    def account(self):
        return self.from_env_or_var("ACCOUNT")

    @property
    def warehouse(self):
        return self.from_env_or_var("WAREHOUSE")

    @cached_property
    def connection(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
        )

    def get_cursor(self) -> snowflake.connector.cursor.SnowflakeCursor:
        return (
            self.connection.cursor(snowflake.connector.DictCursor)
            if self.dict_cursor
            else self.connection.cursor()
        )

    def set_role_warehouse(self, cursor) -> None:
        cursor.execute(
            f"USE ROLE {self.role}"
        )
        cursor.execute(f"USE WAREHOUSE {self.warehouse}")

    @property
    def cursor(self) -> snowflake.connector.cursor.SnowflakeCursor:
        if not hasattr(self, "_cursor"):
            self._cursor = self.get_cursor()
            cursor = self._cursor
            self.set_role_warehouse(cursor)
        else:
            cursor = self._cursor
            if cursor.is_closed():
                cursor = self.get_cursor()
                self._cursor = cursor
                self.set_role_warehouse(cursor)
        return cursor

    def does_table_exist(self, database: str, schema: str, table: str) -> bool:
        res = self.cursor.execute(
            f"""
                    SELECT * FROM {database.upper()}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{schema.upper()}'
                    AND TABLE_NAME = '{table.upper()}'
                """
        ).fetchone()
        return res is not None and len(res) != 0

    def pull_query(
        self,
        query,
        iterable_rows=None,
        chunk_by: Optional[Union[List[str], str]] = None,
    ) -> Generator[pd.DataFrame, None, None]:
        cur = self.cursor.execute(query)
        colnames = [x[0] for x in cur.description]
        if iterable_rows is None:
            return pd.DataFrame(cur.fetchall(), columns=colnames)
        else:
            return self.query_generator(cur, iterable_rows, chunk_by, colnames)

    def query_generator(
        self,
        cur: snowflake.connector.cursor.SnowflakeCursor,
        iterable_rows: Union[int, None],
        chunk_by: Union[List[str], str, None],
        columns: Union[List[str], None],
    ) -> Generator[pd.DataFrame, None, None]:
        dat = pd.DataFrame(cur.fetchmany(iterable_rows), columns=columns)
        is_final = False
        while dat.shape[0] > 0:
            if chunk_by is not None:
                if not is_final:
                    last_group = dat[chunk_by].iloc[-1].to_dict()
                    save_index = dat[
                        dat[chunk_by].apply(
                            lambda row: all(row[k] == v for k, v in last_group.items()),
                            axis=1,
                        )
                    ].index
                    previous_dat = dat.loc[save_index].copy()
                    dat.drop(index=save_index, inplace=True)
                if dat.shape[0] > 0:
                    yield dat

                    if is_final:
                        break

                dat = pd.DataFrame(cur.fetchmany(iterable_rows), columns=columns)
                if dat.shape[0] == 0:
                    dat = previous_dat
                    is_final = True
                else:
                    dat = pd.concat([previous_dat, dat], ignore_index=True)
            else:
                if dat.shape[0] > 0:
                    yield dat

                    dat = pd.DataFrame(cur.fetchmany(iterable_rows), columns=columns)
                else:
                    break


class SnowflakeS3StageOperator(SnowflakeOperator):
    """
    Class exposing S3 file access to and from snowflake.  Override the s3_prefix_or_op
    property in a subclass to allow runtime generation of an s3 path
    """

    def __init__(
        self,
        file_type: str,
        stage_database: str,
        stage_schema: str,
        s3_prefix_or_op: Optional[Union[str, DatToS3Operator]] = None,
        delimiter: str = ",",
        skip_header: bool = True,
        compression: Optional[str] = None,
        nullif: List[str] = ["N/A", "NULL", "null", '""'],
        stage_override: Optional[str] = None,
        encoding: Optional[str] = None,
        optionally_enclosed_by: Union[str, None] = '"',
        **kwargs,
    ):
        self.file_type = file_type.lower()
        self._s3_prefix_or_op = s3_prefix_or_op
        self.delimiter = delimiter
        self.skip_header = skip_header
        self.compression = compression
        self.nullif = nullif
        self.stage_location = f"{stage_database}.{stage_schema}"
        self.stage_override = stage_override
        self.encoding = encoding
        self.optionally_enclosed_by = optionally_enclosed_by

        if isinstance(s3_prefix_or_op, list):
            if not all(
                [
                    (isinstance(x, BaseOperator) and hasattr(x, "s3_prefix"))
                    for x in s3_prefix_or_op
                ]
            ):
                raise AirgoException(
                    "Arg s3_prefix_or_op as a list must be Airgo Operators with s3_prefix attributes."
                )

        if (
            not isinstance(s3_prefix_or_op, list)
            and not isinstance(s3_prefix_or_op, str)
            and not any(hasattr(s3_prefix_or_op, k) for k in ["s3_path", "s3_prefix"])
        ):
            raise AirgoException(
                "Arg s3_prefix_or_op must be a string, Airgo Operator, or list of Airgo Operators"
            )

        super().__init__(**kwargs)
        if isinstance(s3_prefix_or_op, list) or isinstance(
            s3_prefix_or_op, BaseOperator
        ):
            self.set_upstream(s3_prefix_or_op)

    @property
    def s3_prefix_or_op(self):
        if self._s3_prefix_or_op is None:
            raise AirgoException("_s3_prefix_or_op is Nonetype at runtime!")
        return self._s3_prefix_or_op

    @property
    def encoding_sql(self) -> str:
        return "ENCODING='{}'".format(self.encoding) if self.encoding else ""

    @property
    def optionally_enclosed_by_sql(self) -> str:
        return (
            "FIELD_OPTIONALLY_ENCLOSED_BY='{}'".format(self.optionally_enclosed_by)
            if self.optionally_enclosed_by
            else ""
        )

    def get_full_key(self, key_without_bucket) -> str:
        return f"s3://{self.s3_bucket}/{key_without_bucket}"

    def get_stage_path(self, s3key: str) -> str:
        full_s3_key = self.get_full_key(s3key)
        # Logic to differentiate pure bucket stages from pathed stages
        filtered_key = full_s3_key.replace(self.stage_prefix_dict[self.stage], "")
        if filtered_key.startswith("/") and not self.stage_prefix_dict[
            self.stage
        ].endswith("/"):
            filtered_key = filtered_key[1:]
        return f"{self.stage}/{filtered_key}"

    def parse_key(self, s3key: str) -> Tuple[str, str]:
        if "s3://" not in s3key:
            raise ValueError("Your s3key {} has no s3:// prefix...".format(s3key))

        split_list = s3key.split("s3://")[1].split("/")
        return split_list[0], "/".join(split_list[1:])

    @cached_property
    def s3_prefix(self) -> str:
        if isinstance(self.s3_prefix_or_op, list):
            prefixes = [x.s3_prefix for x in self.s3_prefix_or_op]
            if len(prefixes) > 1 and not all(
                x == y for x, y in zip(prefixes[:-1], prefixes[1:])
            ):
                raise AirgoException(
                    f"Your s3_prefixes ({prefixes}) do not all match.  Use separate operator instances for separate prefixes."
                )

            else:
                return prefixes[0]

        if isinstance(self.s3_prefix_or_op, str):
            return self.s3_prefix_or_op

        if hasattr(self.s3_prefix_or_op, "s3_path"):
            return self.s3_prefix_or_op.s3_path
        return self.s3_prefix_or_op.s3_prefix

    @cached_property
    def s3_bucket(self) -> str:
        return self.parse_key(self.s3_prefix)[0]

    @cached_property
    def s3_client(self) -> boto3.client:
        return boto3.client("s3")

    @property
    def nullif_str(self) -> str:
        return ", ".join([f"'{char}'" for char in self.nullif])

    @cached_property
    def stage_prefix_dict(self) -> Dict[str, str]:
        res = self.cursor.execute(
            f"SHOW STAGES IN SCHEMA {self.stage_location}"
        ).fetchall()
        return {
            x["NAME"]
            if self.dict_cursor
            else x[1]: x["URL"]
            if self.dict_cursor
            else x[4]
            for x in res
        }

    @cached_property
    def stage(self) -> str:
        if self.stage_override:
            stage = self.stage_override
            if stage.upper() not in self.stage_prefix_dict:
                raise ValueError(
                    f"Your override stage '{stage}' is not found in any of our public schema stages: {self.stage_prefix_dict}"
                )
            return stage
        try:
            stage = next(
                k
                for k, v in self.stage_prefix_dict.items()
                if self.s3_prefix.startswith(v)
            )
        except StopIteration:
            raise ValueError(
                f"Your s3prefix {self.s3_prefix} is not prefixed by any of our stage urls: {self.stage_prefix_dict}"
            )
        return stage

    @property
    def file_format(self) -> str:
        if self.file_type == "csv":
            return f"""
                TYPE = {self.file_type}
                FIELD_DELIMITER = '{self.delimiter}'
                SKIP_HEADER = {1 if self.skip_header else 0}
                NULL_IF = ({self.nullif_str})
                EMPTY_FIELD_AS_NULL=TRUE
                {self.encoding_sql}
                {self.optionally_enclosed_by_sql}
                COMPRESSION = {'NONE' if self.compression is None else self.compression}
                """

        elif self.file_type in ["json", "parquet"]:
            return f"""
                TYPE = {self.file_type}
                COMPRESSION = {'NONE' if self.compression is None else self.compression}
                """
        return ""


class SnowflakeQueryToStageOperator(SnowflakeS3StageOperator):
    """
    Allows a simple method of writing to an s3 stage via a query.  If the query or the s3_path need
    to be defined at run-time, simply override the s3_prefix_or_op or query properties in a subclass
    """

    @apply_defaults
    def __init__(
        self,
        s3_path: Optional[str] = None,
        query: Optional[str] = None,
        max_file_size_mb: int = 16,
        **kwargs,
    ):
        self._query = query
        self.max_file_size_mb = max_file_size_mb
        super().__init__(s3_prefix_or_op=s3_path, **kwargs)

    @property
    def query(self) -> str:
        if self._query is None:
            raise AirgoException("_query is Nonetype at runtime!")
        return str(self._query)

    @property
    def full_stage_path(self) -> str:
        return f"@{self.stage_location}.{self.get_stage_path(self.parse_key(self.s3_prefix_or_op)[1])}"

    @property
    def copy_to_s3_query(self) -> str:
        return f"""
            COPY INTO '{self.full_stage_path}'
            FROM ({self.query})
            SINGLE = TRUE
            OVERWRITE = TRUE
            FILE_FORMAT = ({self.file_format})
            HEADER = {'FALSE' if self.skip_header else 'TRUE'}
            MAX_FILE_SIZE = {self.max_file_size_mb * 1000 * 1000}
        """

    def execute(self, context: Dict[str, Any]) -> None:
        self.logger.info(
            f"Outputting Query {self.query} to stage path {self.full_stage_path}"
        )
        self.cursor.execute(self.copy_to_s3_query)
        self.logger.info("Finished Outputting Query")


class SnowflakeTableQueryOperator(SnowflakeOperator):
    @apply_defaults
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        overwrite_table: bool = True,
        query: Optional[str] = None,
        grant_roles: List[str] = ["SYSADMIN", "LOOKER_ROLE", "ANALYTICS_TEAM"],
        **kwargs,
    ):
        self.table_name = table_name
        self.schema_name = schema_name
        self.overwrite_table = overwrite_table
        self._query = query
        self.grant_roles = grant_roles
        super().__init__(**kwargs)

    def execute(self, context: Dict[str, Any]) -> None:
        if self.overwrite_table or not self.exists:
            self.logger.info(
                f"Creating Table {self.full_table_name} from query:\n{self.query}"
            )
            self.cursor.execute(self.create_table_query)
            self.cursor.connection.execute_string(";".join(self.assign_permissions_sql))
            self.logger.info("Finished Creating Table")
        else:
            self.logger.info(
                "Table {self.full_table_name} already exists.  Skipping creation."
            )
        self.cursor.close()

    def pull(
        self,
        iterable_rows: Optional[int] = None,
        columns: Optional[List[str]] = None,
        chunk_by: Optional[Union[List[str], str]] = None,
        final_filter="",
    ) -> Generator[pd.DataFrame, None, None]:
        if chunk_by is not None:
            chunk_by = [chunk_by] if isinstance(chunk_by, str) else chunk_by
            chunk_sel = ",".join(chunk_by)
        if columns is not None and chunk_by is not None:
            to_add = list(set(chunk_by).difference(columns))
            columns += to_add
        col_selection = ",".join(columns) if columns is not None else "*"
        chunk_filter = f"ORDER BY {chunk_sel}" if chunk_by is not None else ""
        return self.pull_query(
            query=f"SELECT {col_selection} FROM {self.full_table_name} {final_filter} {chunk_filter}",
            iterable_rows=iterable_rows,
            chunk_by=chunk_by,
        )

    @property
    def query(self) -> Union[str, None]:
        return self._query

    @property
    def assign_permissions_sql(self) -> List[str]:
        perms = []
        if self.grant_roles:
            for r in self.grant_roles:
                perms.append(
                    f"GRANT USAGE ON SCHEMA {self.database}.{self.schema_name} TO ROLE {r}"
                )
                perms.append(
                    f"GRANT SELECT ON TABLE {self.full_table_name} TO ROLE {r}"
                )
        return perms

    @property
    def full_table_name(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.table_name}"

    @property
    def create_table_query(self) -> str:
        return f"""
            CREATE OR REPLACE TABLE {self.full_table_name} AS ({self.query})
        """

    @property
    def exists(self) -> bool:
        return self.does_table_exist(self.database, self.schema_name, self.table_name)

    def get_cleanup_op(
        self, task_id: str, dag: DAG
    ) -> "SnowflakeTableQueryCleanupOperator":
        return SnowflakeTableQueryCleanupOperator(
            task_id=task_id,
            dag=dag,
            database=self.database,
            schema_name=self.schema_name,
            role=self.role,
            table_name=self.table_name,
            dict_cursor=self.dict_cursor,
        )


class SnowflakeTableQueryCleanupOperator(SnowflakeTableQueryOperator):
    @property
    def drop_table_query(self) -> str:
        return f"""
            DROP TABLE {self.full_table_name}
        """

    def execute(self, context: Dict[str, Any]):
        if self.exists:
            self.cursor.execute(self.drop_table_query)


class DFToSnowflake(SnowflakeTableQueryOperator):
    @property
    @abstractmethod
    def output(self) -> Generator[pd.DataFrame, None, None]:
        pass

    @property
    def engine(self) -> sa_engine.Engine:
        return create_engine(
            URL(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema_name,
                warehouse=self.warehouse,
                role=self.role,
            )
        )

    def execute(self, context: Dict[str, Any]) -> None:
        try:
            connection = self.engine.connect()
            for i, df in enumerate(self.output):
                df.to_sql(
                    f"{self.table_name}",
                    con=self.engine,
                    index=False,
                    if_exists="replace" if i == 0 else "append",
                    chunksize=100,
                )
            connection.close()
            self.engine.dispose()
        except ProgrammingError:
            raise


class MultiDFToSnowflake(SnowflakeOperator):
    def __init__(self, schema_name: str, table_names: Dict[str, str], **kwargs):
        self.schema_name = schema_name
        self.table_names = table_names
        super().__init__(**kwargs)

    @property
    @abstractmethod
    def output(self) -> Generator[Dict[str, pd.DataFrame], None, None]:
        pass

    @property
    def replace_first_iteration(self) -> Dict[str, bool]:
        return {key: True for key in self.table_names.keys()}

    @property
    def engine(self) -> sa_engine.Engine:
        return create_engine(
            URL(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema_name,
                warehouse=self.warehouse,
                role=self.role,
            )
        )

    def execute(self, context: Dict[str, Any]) -> None:
        try:
            connection = self.engine.connect()
            for i, df_dict in enumerate(self.output):
                for key, df in df_dict.items():
                    if_exists = (
                        "replace"
                        if i == 0 and self.replace_first_iteration[key]
                        else "append"
                    )
                    df.to_sql(
                        f"{self.table_names[key]}",
                        con=self.engine,
                        index=False,
                        if_exists=if_exists,
                        chunksize=100,
                    )
            connection.close()
            self.engine.dispose()
        except ProgrammingError:
            raise


class SnowflakeS3TableOperator(SnowflakeS3StageOperator):
    COLNAME_CHAR_MAP = {" ": "_", "$": "", "`": "", "'": "", '"': "", "&": "_and_"}
    MINIMAL_COLNAME_CHAR_MAP = {" ": "_"}
    EXTENSION_MAP = {"GZIP": "gz"}
    DF_TYPE_MAP = {
        "int64": "NUMBER",
        "object": "VARCHAR",
        "float64": "FLOAT",
        "datetime64[ns]": "DATETIME",
        "datetime64[ns, UTC]": "DATETIME",
        "bool": "BOOLEAN",
    }

    @apply_defaults
    def __init__(
        self,
        schema: str,
        table: str,
        s3_batch_history_schema: str,
        remap_colnames: bool = False,
        grant_roles: List[str] = [],
        dtype_override: Dict[str, str] = {},
        non_date_exceptions: List[str] = [],
        date_cols: List[str] = [],
        sample_typing: bool = True,
        extension_filter_override: Optional[str] = None,
        on_error: Optional[str] = None,
        trust_modification_timestamp: bool = True,
        s3_key_filter: Callable = lambda key: True,
        start_after_s3_key: Optional[str] = None,
        synced_column_name: str = "_airgo_synced",
        **kwargs,
    ):
        if not sample_typing and (
            not isinstance(dtype_override, OrderedDict) or len(dtype_override) == 0
        ):
            raise Exception(
                "Arg dtype_override must be an OrderedDict of all columns and types if no sampling is called for."
            )

        if not callable(s3_key_filter):
            raise Exception("Arg s3_key_filter is not callable...")

        self.non_date_exceptions = [c.lower() for c in non_date_exceptions]
        self.remap_colnames = remap_colnames
        self.schema = schema.upper()
        self.table = table.upper()
        self.s3_batch_history_schema = s3_batch_history_schema
        self.grant_roles = [r.upper() for r in grant_roles]
        self.on_error = on_error
        self.dtype_override = OrderedDict(
            [(self.colname_filter(k), v.upper()) for k, v in dtype_override.items()]
        )
        self.start_after_s3_key = start_after_s3_key
        self.trust_modification_timestamp = trust_modification_timestamp
        self.date_cols = date_cols
        self.sample_typing = sample_typing
        self.extension_filter_override = extension_filter_override
        self.s3_key_filter = s3_key_filter
        self.synced_column_name = synced_column_name
        super().__init__(**kwargs)

    @property
    def extension(self) -> str:
        if self.extension_filter_override is not None:
            return self.extension_filter_override

        return "." + (
            self.EXTENSION_MAP[self.compression.upper()]
            if self.compression is not None
            else self.file_type
        )

    @cached_property
    def s3_keys(self) -> List[Dict[str, Union[str, int]]]:
        kwargs = {"Bucket": self.s3_bucket, "Prefix": self.parse_key(self.s3_prefix)[1]}
        if self.start_after_s3_key:
            kwargs["StartAfter"] = self.start_after_s3_key.replace(
                f"s3://{self.s3_bucket}/", ""
            )
        files = []
        while True:
            resp = self.s3_client.list_objects_v2(**kwargs)
            if "Contents" not in resp:
                raise Exception(f"No s3 objects found at path '{self.s3_prefix}'")
            for obj in resp["Contents"]:
                modified = int(obj["LastModified"].strftime("%s"))
                if obj["Key"].lower().endswith(
                    f"{self.extension}"
                ) and self.s3_key_filter(obj["Key"]):
                    if (
                        not self.trust_modification_timestamp
                        or modified > self.max_batch_id_and_modified[1]
                    ):
                        files.append({"key": obj["Key"], "modified": modified})
            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

        return sorted(files, key=lambda x: x["modified"])

    @cached_property
    def all_pushed_s3keys(self) -> Set[str]:
        sql = f"""SELECT S3_KEY
                  FROM {self.batch_history_table}
                  WHERE DESTINATION_SCHEMA = '{self.schema}'
                  AND DESTINATION_TABLE = '{self.table}'
               """
        return set(
            [
                x["S3_KEY"] if self.dict_cursor else x[0]
                for x in self.cursor.execute(sql).fetchall()
            ]
        )

    @cached_property
    def unpushed_s3_keys(self) -> List[Dict[str, Union[str, int]]]:
        if not self.table_exists:
            return self.s3_keys

        if self.trust_modification_timestamp:
            return [
                dict_
                for i, dict_ in enumerate(self.s3_keys)
                if i > self.max_batch_id_and_modified[0]
                or dict_["modified"] > self.max_batch_id_and_modified[1]
            ]

        else:
            return [
                dict_
                for i, dict_ in enumerate(self.s3_keys)
                if dict_["key"] not in self.all_pushed_s3keys
            ]

    @property
    def exists(self) -> bool:
        if not self.table_exists:
            return False

        return len(self.unpushed_s3_keys) == 0

    @property
    def use_schema_sql(self) -> str:
        return f"""
                    USE SCHEMA {self.database}.{self.schema}
               """

    def colname_filter(self, colname: str) -> str:
        map = (
            self.COLNAME_CHAR_MAP
            if self.remap_colnames
            else self.MINIMAL_COLNAME_CHAR_MAP
        )
        return reduce(
            lambda string_, c: string_.replace(c, self.COLNAME_CHAR_MAP[c]).upper(),
            sorted(map.keys()),
            colname,
        )

    def sample_file_bytes(self, key: str) -> bytes:
        raw_sample_bytes = self.s3_client.get_object(
            Bucket=self.s3_bucket, Key=key, Range="bytes=0-5242880"
        )["Body"].read()
        if self.compression == "GZIP":
            raw_sample_bytes = gzip.decompress(raw_sample_bytes)
        return raw_sample_bytes

    def sample_file_df(self, key: str) -> pd.DataFrame:
        sample_bytes = self.sample_file_bytes(key)
        if self.file_type == "parquet":
            return pd.read_parquet(io.BytesIO(sample_bytes))
        sample_file = sample_bytes.decode(
            self.encoding if self.encoding is not None else "utf-8"
        )
        lines = sample_file.split("\r\n" if "\r\n" in sample_file else "\n")
        if self.file_type == "csv":
            sample = io.StringIO("\n".join(lines[0 : len(lines) - 2]))
            df = pd.read_csv(sample, delimiter=f"{self.delimiter}")
        elif self.file_type == "json":
            df = pd.DataFrame([json.loads(s) for s in lines[1 : len(lines) - 2]])
        return df

    @staticmethod
    def coerce_datetime(self, d):
        try:
            return pd.to_datetime(d)
        except pd.errors.OutOfBoundsDatetime:
            return None

    @cached_property
    def type_mapping(self) -> Tuple[List[str], List[str]]:
        if not self.sample_typing:
            return list(self.dtype_override.keys()), list(self.dtype_override.values())
        df = self.sample_file_df(self.unpushed_s3_keys[-1]["key"])
        if not self.date_cols:
            time_or_date_cols = [
                c
                for c in df.columns
                if (
                    "date" in c.lower()
                    or "week" in c.lower()
                    or "pst" in c.lower()
                    or "created_at" in c.lower()
                    or "install_time" in c.lower()
                    or "timestamp" in c.lower()
                )
            ]
        else:
            time_or_date_cols = self.date_cols
        self.logger.info(f"These are the default data types: \n {df.dtypes}")
        self.logger.info(f"Converting these columns {time_or_date_cols} into datetimes")
        for c in time_or_date_cols:
            if c not in self.non_date_exceptions:
                try:
                    df[c] = pd.to_datetime(df[c])
                except pd.errors.OutOfBoundsDatetime:
                    df[c] = df[c].apply(self.coerce_datetime)
        df.rename(columns={c: self.colname_filter(c) for c in df.columns}, inplace=True)
        variant_cols = [
            c
            for c in df.select_dtypes(include="object").columns
            if any(isinstance(obj, dict) for obj in df[c])
        ]
        self.logger.info(f"These are the variants {variant_cols}")
        final_column_names = list(df.columns.values)
        if self.file_type == "json":
            final_column_names = final_column_names + [
                c for c in self.dtype_override.keys() if c not in final_column_names
            ]
        df_dtypes = dict(df.dtypes)
        new_maps = {
            col: self.dtype_override.get(
                col,
                (
                    "VARIANT"
                    if col in variant_cols
                    else self.DF_TYPE_MAP[str(df_dtypes.get(col))]
                ),
            ).upper()
            for col in final_column_names
        }
        self.logger.info(
            f"{df.columns.values},\n {[new_maps[v].upper() for v in df.columns.values]}"
        )
        return (final_column_names, [new_maps[v] for v in final_column_names])

    @cached_property
    def table_exists(self) -> bool:
        return self.does_table_exist(self.database, self.schema, self.table)

    @property
    def full_table_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    @cached_property
    def columns(self) -> List[str]:
        res = self.cursor.execute(
            f"""
                    SELECT COLUMN_NAME FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{self.schema}'
                    AND TABLE_NAME = '{self.table}'
                """
        ).fetchall()
        return [(c["COLUMN_NAME"] if self.dict_cursor else c[0]).upper() for c in res]

    @property
    def create_table_sql(self) -> str:
        schema = ",".join([f"{k} {v}" for k, v in zip(*self.type_mapping)]).upper()
        return f"""
                    CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{self.table}
                    ({schema}, {self.extra_s3_types})
                """

    @property
    def assign_permissions_sql(self) -> List[str]:
        perms = []
        if self.grant_roles:
            for r in self.grant_roles:
                perms.append(f"GRANT USAGE ON SCHEMA {self.schema} TO ROLE {r}")
                perms.append(
                    f"GRANT SELECT ON ALL TABLES IN SCHEMA {self.schema} TO ROLE {r}"
                )
        return perms

    @property
    def has_new_cols(self) -> bool:
        if len(self.unpushed_s3_keys) == 0:
            return False
        return len(set(self.file_columns).difference(self.columns)) > 0

    @property
    def synced_col_value(self) -> str:
        return "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())"

    @property
    def has_sync_col(self) -> bool:
        return any([c.lower() == self.synced_column_name.lower() for c in self.columns])

    @property
    def add_sync_column(self) -> List[str]:
        return [
            f"""
                ALTER TABLE {self.database}.{self.schema}.{self.table}
                ADD COLUMN {self.synced_column_name} TIMESTAMP_TZ
                """,
            f"""
                UPDATE
                {self.database}.{self.schema}.{self.table}
                SET {self.synced_column_name} = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
                """,
        ]

    @property
    def add_columns(self) -> List[str]:
        add_cols = []
        new_cols = [col for col in self.file_columns if col not in self.columns]
        for n in new_cols:
            position = list(self.file_columns).index(n)
            d_type = self.type_mapping[1][position]
            add_cols.append(
                f"""
                ALTER TABLE {self.database}.{self.schema}.{self.table}
                ADD COLUMN {n} {d_type}
                """
            )
        return add_cols

    @property
    def file_columns(self) -> List[str]:
        return [c.upper() for c in self.type_mapping[0]]

    @property
    def batch_history_table(self) -> str:
        return f"{self.database}.{self.s3_batch_history_schema}.S3_BATCH_HISTORY"

    @cached_property
    def max_batch_id_and_modified(self) -> Tuple[int, int]:
        if not self.table_exists:
            return (-1, 0)

        sql = f"""SELECT
                        MAX(BATCH_ID),
                        MAX(S3_LAST_MODIFIED)
                  FROM {self.batch_history_table}
                  WHERE DESTINATION_SCHEMA = '{self.schema}'
                  AND DESTINATION_TABLE = '{self.table}'
                  """
        maxes = self.cursor.execute(sql).fetchone()
        if self.dict_cursor:
            maxes = (maxes["MAX(BATCH_ID)"], maxes["MAX(S3_LAST_MODIFIED)"])
        return maxes if maxes != (None, None) else (-1, 0)

    @property
    @abstractmethod
    def base_commands(self) -> List[str]:
        pass

    def execute(self, context: Dict[str, Any]) -> None:
        try:
            if not self.exists:
                full_command = ";".join(self.base_commands)
                self.logger.info(f"running: {full_command}")
                self.cursor.connection.execute_string(full_command)
            else:
                self.logger.info(f"Nothing To Update For {self.schema}.{self.table}")
        finally:
            self.cursor.close()

    @property
    def extra_s3_columns(self) -> List[str]:
        return ["S3_CSV_BATCH_ID"] + (
            [self.synced_column_name.upper()] if self.synced_column_name else []
        )

    @property
    def extra_s3_types(self) -> str:
        return "S3_CSV_BATCH_ID NUMERIC" + (
            f", {self.synced_column_name} TIMESTAMP_TZ"
            if self.synced_column_name
            else ""
        )

    def get_extra_s3_copy(self, batch_id) -> str:
        return f"{batch_id}" + (
            f", {self.synced_col_value}" if self.synced_column_name else ""
        )


class SnowflakeS3AppendOperator(SnowflakeS3TableOperator):
    @property
    def on_error_string(self) -> str:
        if self.on_error:
            return f"ON_ERROR = {self.on_error}"

        return ""

    @property
    def col_indexes(self) -> str:
        if self.file_type == "csv":
            return ", ".join([f"${r}" for r in range(1, len(self.file_columns) + 1)])

        elif self.file_type == "json":
            return ",".join(
                [
                    f"NULLIF(NULLIF($1:{col.lower()}, 'NULL'),'')"
                    for col in self.file_columns
                ]
            )
        elif self.file_type == "parquet":
            return ",".join([f"$1:{col.lower()}" for col in self.file_columns])
        else:
            raise AirgoException(
                f"Unknown file type for Append Operator: {self.file_type}"
            )

    def get_copy_sql(self, s3_key: str, i: int) -> str:
        # make sure all json keys are lowercase
        cols = ", ".join(self.file_columns)
        batch_id = self.max_batch_id_and_modified[0] + i + 1
        return f"""
            COPY INTO {self.database}.{self.schema}.{self.table} ({cols}, {", ".join(self.extra_s3_columns)})
            FROM (
                  SELECT {self.col_indexes}, {self.get_extra_s3_copy(batch_id)} FROM '@{self.stage_location}.{self.get_stage_path(s3_key)}'
                 )
            FILE_FORMAT = ({self.file_format})
            {self.on_error_string}
        """

    def insert_into_batch_history(self, s3_key: str, modified: int, i: int) -> str:
        batch_id = self.max_batch_id_and_modified[0] + i + 1
        return f"""
                    INSERT INTO {self.batch_history_table} (BATCH_ID, S3_KEY, DESTINATION_SCHEMA, DESTINATION_TABLE, S3_LAST_MODIFIED)
                        SELECT {batch_id}, '{s3_key}', '{self.schema}', '{self.table}',  {modified}
                """

    @property
    def base_commands(self) -> List[str]:
        base_commands = [self.use_schema_sql]
        if not self.table_exists:
            base_commands.append(self.create_table_sql)
            base_commands.extend(self.assign_permissions_sql)
        else:
            if self.has_new_cols:
                base_commands.extend(self.add_columns)
            if not self.has_sync_col:
                base_commands.extend(self.add_sync_column)
        for i, dict_ in enumerate(self.unpushed_s3_keys):
            base_commands.append(self.get_copy_sql(dict_["key"], i))
            base_commands.append(
                self.insert_into_batch_history(dict_["key"], dict_["modified"], i)
            )
        return base_commands


class SnowflakeS3ReplaceOperator(SnowflakeS3TableOperator):
    @property
    def col_indexes(self) -> str:
        if self.file_type == "csv":
            return ", ".join([f"${r}" for r in range(1, len(self.file_columns) + 1)])

        elif self.file_type in ["json", "parquet"]:
            return ",".join(
                [f"$1:{k.lower()}::{v}" for k, v in zip(*self.type_mapping)]
            )
        else:
            raise AirgoException(
                f"Unknown file type for Replace Operator: {self.file_type}"
            )

    @property
    def file_format_name(self) -> str:
        return f"{self.file_type}_{self.schema}_{self.table}"

    @property
    def file_format_sql(self) -> str:
        return f"""
        CREATE OR REPLACE FILE FORMAT {self.file_format_name}
        {self.file_format}
        """

    @property
    def copy_sql(self) -> str:
        batch_id = len(self.s3_keys) - 1
        s3_key = self.unpushed_s3_keys[-1]["key"]
        col_schema = ",".join([f"{k} {v}" for k, v in zip(*self.type_mapping)]).upper()
        return f"""
                    CREATE OR REPLACE TABLE {self.database}.{self.schema}.{self.table} ({col_schema}, {self.extra_s3_types})
                    AS
                    (SELECT {self.col_indexes}, {self.get_extra_s3_copy(batch_id)} FROM '@{self.stage_location}.{self.get_stage_path(s3_key)}'
                    (FILE_FORMAT=>'{self.file_format_name}'))
            """

    @property
    def insert_into_batch_history(self) -> str:
        batch_id = len(self.s3_keys) - 1
        s3_key = self.unpushed_s3_keys[-1]["key"]
        modified = self.unpushed_s3_keys[-1]["modified"]
        return f"""
                    INSERT INTO {self.batch_history_table} (BATCH_ID, S3_KEY, DESTINATION_SCHEMA, DESTINATION_TABLE, S3_LAST_MODIFIED)
                        SELECT {batch_id}, '{s3_key}', '{self.schema}', '{self.table}', {modified}
                """

    @property
    def base_commands(self) -> List[str]:
        items = [
            self.use_schema_sql,
            self.file_format_sql,
            self.copy_sql,
            self.insert_into_batch_history,
        ]
        items.extend(self.assign_permissions_sql)
        return items


class SnowflakeS3UpsertOperator(SnowflakeS3AppendOperator):
    def __init__(self, id_column: str, max_by_column: Optional[str] = None, **kwargs):
        self.id_column = id_column
        self.max_by_column = max_by_column
        super().__init__(**kwargs)

    def get_extra_s3_merge(self, batch_id: int) -> str:
        return ", ".join(
            [
                f"{v} {c}"
                for v, c in zip(
                    [batch_id, f"{self.synced_col_value}"], self.extra_s3_columns
                )
            ]
        )

    @property
    def file_format_name(self) -> str:
        return f"{self.file_type.upper()}_{self.schema}_{self.table}"

    @property
    def file_format_sql(self) -> str:
        return (
            f"""
        CREATE OR REPLACE FILE FORMAT {self.file_format_name}
            TYPE = '{self.file_type}'
            SKIP_HEADER = {1 if self.skip_header else 0}
            FIELD_DELIMITER = '{self.delimiter}'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ({self.nullif_str})
            EMPTY_FIELD_AS_NULL=TRUE
        """
            if self.file_type == "csv"
            else f"""
        CREATE OR REPLACE FILE FORMAT {self.file_format_name}
            TYPE = '{self.file_type}'
            NULL_IF = ({self.nullif_str})
            EMPTY_FIELD_AS_NULL=TRUE
        """
        )

    @property
    def base_commands(self) -> List[str]:
        base_commands = [self.use_schema_sql]
        if not self.table_exists:
            base_commands.append(self.create_table_sql)
            base_commands.extend(self.assign_permissions_sql)
        else:
            if self.has_new_cols:
                base_commands.extend(self.add_columns)
            if not self.has_sync_col:
                base_commands.extend(self.add_sync_column)
        base_commands.append(self.file_format_sql)
        for i, dict_ in enumerate(self.unpushed_s3_keys):
            base_commands.append(self.get_copy_sql(dict_["key"], i))
            base_commands.append(
                self.insert_into_batch_history(dict_["key"], dict_["modified"], i)
            )
        return base_commands

    def get_copy_sql(self, s3_key: str, i: int) -> str:
        # make sure all json keys are lowercase
        cols = ", ".join([f"{c}" for c in self.file_columns + self.extra_s3_columns])
        colset = ", ".join(
            [f"{c} = t.{c}" for c in self.file_columns + self.extra_s3_columns]
        )
        colinsert = ", ".join(
            [f"t.{c}" for c in self.file_columns + self.extra_s3_columns]
        )
        batch_id = self.max_batch_id_and_modified[0] + i + 1
        if self.file_type == "csv":
            col_indexes = ", ".join(
                [
                    f"${r} {c}"
                    for r, c in zip(
                        range(1, len(self.file_columns) + 1), self.file_columns
                    )
                ]
            )
        elif self.file_type == "json":
            col_indexes = ",".join(
                [
                    f"NULLIF(NULLIF($1:{col.lower()}, 'NULL'),'')"
                    for col in self.file_columns
                ]
            )
        elif self.file_type == "parquet":
            col_indexes = ",".join(
                [
                    f"NULLIF(NULLIF($1:{col.lower()}, 'NULL'),'')"
                    for col in self.file_columns
                ]
            )
        if not self.max_by_column:
            return f"""
                MERGE INTO {self.database}.{self.schema}.{self.table} base
                USING (
                    SELECT {col_indexes}, {self.get_extra_s3_merge(batch_id)}
                    FROM '@{self.stage_location}.{self.stage}/{s3_key}' (FILE_FORMAT => {self.file_format_name})
                ) t ON base.{self.id_column} = t.{self.id_column}
                WHEN MATCHED THEN UPDATE
                SET
                {colset}
                WHEN NOT MATCHED THEN INSERT
                ({cols})
                VALUES
                ({colinsert})
            """

        else:
            return f"""
                MERGE INTO {self.database}.{self.schema}.{self.table} base
                USING (
                    WITH all_rows as (
                        SELECT {col_indexes},
                               ROW_NUMBER() OVER(PARTITION BY {self.id_column} ORDER BY {self.max_by_column} DESC) AS rk,
                               {self.get_extra_s3_merge(batch_id)}
                        FROM '@{self.stage_location}.{self.stage}/{s3_key}' (FILE_FORMAT => {self.file_format_name})
                    )
                    SELECT * FROM all_rows
                    WHERE all_rows.rk = 1
                ) t ON base.{self.id_column} = t.{self.id_column}
                WHEN MATCHED THEN UPDATE
                SET
                {colset}
                WHEN NOT MATCHED THEN INSERT
                ({cols})
                VALUES
                ({colinsert})
            """
