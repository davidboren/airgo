import pandas as pd  # type: ignore

from typing import Dict, List, Generator
from cached_property import cached_property  # type: ignore

from airgo.utils.dropbox import fetch_dropbox
from .s3 import DatToS3Operator, S3_PREFIX


class DBToS3Operator(DatToS3Operator):
    """
    For fetching files from Dropbox and placing them on S3.
    """

    def __init__(
        self,
        path,
        s3_key: str,
        s3_bucket: str,
        delimiter: str = "\t",
        remap_columns: Dict[str, str] = {},
        non_date_exceptions: List[str] = [],
        **kwargs,
    ):
        super().__init__(delimiter=delimiter, **kwargs)
        self.path = path
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.non_date_exceptions = [c.lower() for c in non_date_exceptions]
        self.remap_columns = remap_columns

    @cached_property
    def s3_path(self) -> str:
        return f"{S3_PREFIX}{self.s3_bucket.strip('/')}/{self.s3_key.strip('/')}"

    @property
    def output(self) -> Generator[pd.DataFrame, None, None]:
        f = fetch_dropbox(self.path)
        df = pd.read_csv(f.raw, header=0)
        if self.remap_columns:
            df.rename(columns=self.remap_columns, inplace=True)
        df = df.rename(columns=lambda x: x.replace(" ", "_").lower())
        df["id"] = df.index
        cols = list(df)
        for c in cols:
            if "date" in c.lower() and c.lower() not in self.non_date_exceptions:
                df[c] = pd.to_datetime(df[c])
        yield df
