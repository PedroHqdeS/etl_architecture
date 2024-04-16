from connectors.file_format_connector import FileFormatConnector

from pyspark.sql import DataFrame

class CsvConnector(FileFormatConnector):
    def __init__(self, app_name: str):
        super().__init__(app_name=app_name)
        self._file_format = "csv"

    def _load_data(self, dataframe: DataFrame, path: str) -> None:
        dataframe.toPandas().to_csv(
            path,
            sep=";",
            encoding="utf-8",
            index=False
        )
