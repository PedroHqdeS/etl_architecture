from connectors.file_format_connector import FileFormatConnector
from utils.spark_utils import start_spark_session, read_spark_dataframe

from pyspark.sql import DataFrame

class CsvConnector(FileFormatConnector):
    def __init__(self, app_name: str):
        super().__init__(app_name=app_name)
        self._file_format = "csv"

    def extract_data(self, path: str) -> DataFrame:
        params ={

        }
        spark = start_spark_session(app_name="CsvConnector")
        read_spark_dataframe(
            path=path,
            spark_session=spark,
            file_format="csv",
            **params)

    def load_data(self, dataframe: DataFrame, path: str) -> None:
        dataframe.toPandas().to_csv(
            path,
            sep=";",
            encoding="utf-8",
            index=False
        )
