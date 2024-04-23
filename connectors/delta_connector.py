from connectors.file_format_connector import FileFormatConnector
from utils.spark_utils import (
    start_spark_session, read_spark_dataframe, write_spark_dataframe)
from utils.data_lake_utils import verify_if_directory_existis

from pyspark.sql import DataFrame

class DeltaConnector(FileFormatConnector):
    def __init__(self):
        super().__init__()
        self._file_format = "delta"
        self._spark_session = start_spark_session(app_name="DeltaConnector")

    def extract_data(self, path: str) -> DataFrame:
        dataframe = read_spark_dataframe(
            path=path,
            spark_session=self._spark_session,
            file_format=self._file_format)
        return dataframe

    def load_data(self, dataframe: DataFrame, path: str) -> None:
        verify_if_directory_existis(dir=path)
        write_spark_dataframe(
            dataframe=dataframe,
            path=path,
            spark_session=self._spark_session,
            file_format=self._file_format,
            partitions=["year", "month", "day"]
        )