from connectors.file_format_connector import FileFormatConnector
from utils.spark_utils import start_spark_session, read_spark_dataframe

from pyspark.sql import DataFrame

class CsvConnector(FileFormatConnector):
    def __init__(self):
        super().__init__()
        self._file_format = "csv"

    def extract_data(self, path: str) -> DataFrame:
        params ={
            "delimiter": ";",
            "header": True
        }
        spark = start_spark_session(app_name="CsvConnector")
        self._logger.info(f"Extracting data from '{path}' ...")
        dataframe = read_spark_dataframe(
            path=path,
            spark_session=spark,
            file_format=self._file_format,
            **params)
        return dataframe

    def load_data(self, dataframe: DataFrame, path: str) -> None:
        dataframe.toPandas().to_csv(
            path,
            sep=";",
            encoding="utf-8",
            index=False
        )
