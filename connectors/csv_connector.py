import polars as pl

from connectors.base.file_format_connector import FileFormatConnector
from utils.data_lake_utils import verify_if_directory_exists


class CsvConnector(FileFormatConnector):
    def __init__(self):
        super().__init__()
        self._file_format = "csv"

    def extract_data(self, path: str) -> pl.DataFrame:
        self._logger.info(msg=f"Extracting data from '{path}' ...")
        try:
            dataframe = pl.read_csv(
                source=path + "/*.csv",
                separator=";"
            )
            self._logger.info(msg=f"Data extracted with success.")
        except FileNotFoundError as e:
            self._logger.warn(msg=f"'{path}' does not exist.")
            dataframe = pl.DataFrame(data=[])
        except Exception as e:
            self._logger.error(msg=e)
            raise
        return dataframe

    def load_data(self, dataframe: pl.DataFrame, path: str) -> None:
        self._logger.info(msg=f"Writing data in {path} ...")
        if not dataframe.is_empty():
            verify_if_directory_exists(dir=path)
            dataframe.write_csv(file=path + "/data.csv", separator=";")
            self._logger.info(msg=f"Data written with success.")
        else:
            self._logger.warn(
                msg="Empty dataframe. There is nothing to write."
            )
