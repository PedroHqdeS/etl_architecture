import polars as pl
from deltalake import DeltaTable
from connectors.base.file_format_connector import FileFormatConnector


class DeltaConnector(FileFormatConnector):
    def __init__(self, partition_dict: dict=None):
        super().__init__()
        self._file_format = "delta"
        self._partition_dict = partition_dict

    def _get_partitions(self):
        partition = []
        for key, value in self._partition_dict.items():
            if value != "*":
                partition.append((key, "=", value))
        print(partition)
        return partition

    def extract_data(self, path: str) -> pl.DataFrame:
        dataframe = pl.from_arrow(
            DeltaTable(table_uri=path).to_pyarrow_table(
                partitions=self._get_partitions()
            )
        )
        return dataframe

    def load_data(self, dataframe: pl.DataFrame, path: str) -> None:
        dataframe.write_delta(
            target=path,
            mode="overwrite",
            delta_write_options={
                "schema_mode": "overwrite",
                "partition_by": list(self._partition_dict.keys())
            })
