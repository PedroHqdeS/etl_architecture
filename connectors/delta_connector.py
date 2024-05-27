import os
import polars as pl
from typing import List
from deltalake import DeltaTable
from connectors.base.file_format_connector import FileFormatConnector


class DeltaConnector(FileFormatConnector):
    def __init__(self, partition_dict: dict=None):
        super().__init__()
        self._file_format = "delta"
        self._partition_dict = self._validate_partition(
            partition_dict=partition_dict
        )

    def _validate_partition(self, partition_dict: dict):
        for key, value in partition_dict.items():
            if key == "" or value == "":
                raise ValueError(
                    "Dict key and value cannot be ''(empty).")
        return partition_dict

    def _get_partitions(self):
        partition = []
        for key, value in self._partition_dict.items():
            if value != "*":
                partition.append((key, "=", value))
        return partition

    def _detele_from_partitions(self,
                                partitions: List[str],
                                dataframe: pl.DataFrame,
                                path: str) -> None:

        predicate = " and ".join(
            [f"t.{item} == s.{item}" for item in partitions]
        )
        partition_to_delete = dataframe.select(*partitions).unique()
        (partition_to_delete.write_delta(
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
        self._logger.info(msg=f"Writing data in {path} ...")
        if not dataframe.is_empty():
            write_mode = "overwrite"
            partitions = list(self._partition_dict.keys())
            write_options = {
                "partition_by": partitions
            }
            if os.path.exists(path) and len(partitions) > 0:
                # Only existing partitions will be overwritten
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
