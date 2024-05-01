from connectors.base.file_format_connector import FileFormatConnector
from utils.spark_utils import read_spark_dataframe, write_spark_dataframe
from utils.data_lake_utils import verify_if_directory_exists

import polars as pl

from typing import List

from deltalake import DeltaTable


class DeltaConnector(FileFormatConnector):
    def __init__(self, options: dict=None):
        super().__init__()
        self._file_format = "delta"
        self._options = options

    def extract_data(self, path: str) -> pl.DataFrame:
        dataframe = pl.read_delta(
            source=path,
            pyarrow_options=self._options
        )
        return dataframe

    def load_data(self, dataframe: pl.DataFrame, path: str) -> None:
        # verify_if_directory_exists(dir=path)
        # dataframe.partition_by(by=self.)
        dataframe.write_delta(
            target=path,
            mode="overwrite",
            delta_write_options={"schema_mode": "overwrite"})
