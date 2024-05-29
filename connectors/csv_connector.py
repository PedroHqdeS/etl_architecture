import polars as pl
from connectors.base.file_format_connector import FileFormatConnector
from utils.data_lake_utils import verify_if_directory_exists


class CsvConnector(FileFormatConnector):
    """
    Component that defines the behavior in how extract and write
    data as CSV format in any Data Lake's layer.
    """
    def __init__(self):
        super().__init__()
        self._file_format = "csv"

    def extract_data(self, path: str) -> pl.DataFrame:
        """
        Extracts data in CSV format from the Data Lake's layer.
        If the path passed as parameter does not exist, it will
        return an empty dataframe.

        Parameters
        ----------
        path: str
            Data Lake's path from where data will be extracted.

        Returns
        -------
        DataFrame
        """
        self._logger.info(msg=f"Extracting data from '{path}' ...")
        try:
            dataframe = pl.read_csv(
                source=path + "/*.csv",
                separator=";"
            )
            self._logger.info(msg=f"Data extracted with success.")
        except Exception as e:
            if "no matching files found" in e.__str__():
                self._logger.warn(msg=f"'{path}' does not exist.")
                dataframe = pl.DataFrame(data=[])
            else:
                self._logger.error(msg=e)
                raise
        return dataframe

    def load_data(self, dataframe: pl.DataFrame, path: str) -> None:
        """
        Writes data as CSV format in any Data Lake's layer.
        If the DataFrame passed as parameter is empty, it will
        not write anything.

        Parameters
        ----------
        dataframe: DataFrame
            Contains the data will be stored.
        path: str
            Data Lake's path where data will be stored.

        Returns
        -------
        None
        """
        self._logger.info(msg=f"Writing data in {path} ...")
        if not dataframe.is_empty():
            verify_if_directory_exists(dir=path)
            dataframe.write_csv(file=path + "/data.csv", separator=";")
            self._logger.info(msg=f"Data written with success.")
        else:
            self._logger.warn(
                msg="Empty dataframe. There is nothing to write."
            )
