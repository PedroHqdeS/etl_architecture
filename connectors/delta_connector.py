import os
import polars as pl
from typing import List
from deltalake import DeltaTable
from connectors.base.file_format_connector import FileFormatConnector


class DeltaConnector(FileFormatConnector):
    """
    Component that defines the behavior in how extract and write
    data as Delta format in any Data Lake's layer.

    Parameters
    ----------
    partition_dict: dict
        Key(s) containing the name of DataFrame columns by which data will
        be partitioned. Value(s) containing the value of a partition for
        read operation.
    """
    def __init__(self, partition_dict: dict=None):
        super().__init__()
        self._file_format = "delta"
        self._partition_dict = self._validate_partition(
            partition_dict=partition_dict
        )

    def _validate_partition(self, partition_dict: dict) -> dict:
        """
        In case a non-empty dict is passed as parameter,
        it must validate if it is a valid dict, which
        must not have the key or value empty or equals
        to None.

        Parameters
        ----------
        partition_dict: dict
            Key(s) containing the name of DataFrame columns by which data will
            be partitioned. Value(s) containing the value of a partition for
            read operation.

        Returns
        -------
        dict
        """
        for key, value in partition_dict.items():
            if key is None or value is None or key == "" or value == "":
                raise ValueError(
                    "Dict key and value cannot be None or ''(empty).")
        return partition_dict

    def _get_partitions(self) -> List:
        """
        Transforms the dict containing the partition values in a list
        of tuples, which is accepted by Polars to read a partitioned
        source.

        Returns
        -------
        List
        """
        partitions = []
        for key, value in self._partition_dict.items():
            if value != "*":
                partitions.append((key, "=", value))
        return partitions

    def _detele_from_partitions(self,
                                partitions: List[str],
                                dataframe: pl.DataFrame,
                                path: str) -> None:
        """
        It will delete data from the target according to the value
        of the partition columns of the data being received. This
        operation is applied as a part of the aim to overwrite only
        certain data partitions that is receiving new data. For this,
        it uses merge to match the data that must be deleted.

        Parameters
        ----------
        partitions: List[str]
            Contains the name of DataFrame columns by which data is
            partitioned.
        dataframe: DataFrame
            Contains new data being writen in Data Lake.
        path: str
            Data Lake's path of data that will be overridden.

        Returns
        -------
        None
        """
        # Create the predicate according the partitions
        # of the Delta target
        predicate = " and ".join(
            [f"t.{item} == s.{item}" for item in partitions]
        )
        # Guarantees distinct values for the DataFrame columns used
        # to partition the data. Thus, the delete operation will
        # perform without errors
        partition_to_delete = dataframe.select(*partitions).unique()
        # Deletes partitions that match with data being received
        (partition_to_delete
            .write_delta(
                target=path,
                mode="merge",
                delta_merge_options={
                    "predicate": predicate,
                    "source_alias": "s",
                    "target_alias": "t"
                }
            )
            .when_matched_delete()
            .execute())

    def extract_data(self, path: str) -> pl.DataFrame:
        """
        Extracts data in Delta format from a Data Lake's layer.
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
            dataframe = pl.from_arrow(
                DeltaTable(table_uri=path).to_pyarrow_table(
                    partitions=self._get_partitions()
                )
            )
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
        Writes data as Delta format in any Data Lake's layer.
        If the DataFrame passed as parameter is empty, it will
        not write anything. The writing operation overwrites all
        existing data, or if the target is partitioned it will
        overwrite only the partitions which their values match
        with the new data.

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
            write_mode = "overwrite"
            partitions = list(self._partition_dict.keys())
            write_options = {
                "partition_by": partitions
            }
            if os.path.exists(path) and len(partitions) > 0:
                # Only existing partitions will be overridden
                self._logger.info(msg=f"Overwriting existing partitions ...")
                self._detele_from_partitions(
                    partitions=partitions,
                    dataframe=dataframe,
                    path=path)
                write_mode = "append"
                write_options["schema_mode"] = "merge"
                write_options["engine"] = "rust"

            dataframe.write_delta(
                target=path,
                mode=write_mode,
                delta_write_options=write_options,
            )
            self._logger.info(msg=f"Data written with success.")
        else:
            self._logger.warn(
                msg="Empty dataframe. There is nothing to write."
            )
