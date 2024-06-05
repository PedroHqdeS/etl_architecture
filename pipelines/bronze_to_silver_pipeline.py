import polars as pl
from typing import Optional
from datetime import datetime
from uuid import uuid4
from connectors.base.file_format_connector import FileFormatConnector
from connectors.delta_connector import DeltaConnector
from layers.bronze_layer_path import BronzeLayerPath
from layers.silver_layer_path import SilverLayerPath
from pipelines.base.data_pipeline import DataPipeline


class BronzeToSilverPipeline(DataPipeline):
    """
    Component to standardize the data processing
    from Data Lake's Bronze layer to Silver Layer.

    Parameters
    ----------
    source_connector: FileFormatConnector
        Component that connects and extracts data from Data Lake's
        Bronze layer in any needed format.
    bronze_path: BronzeLayerPath
        Component that define and standardize the path patterns
        for Bronze Layer in Data Lake.
    target_connector: DeltaConnector
        Component that defines the behavior in how writing
        data as Delta format in Data Lake's Silver layer.
    silver_path: SilverLayerPath
        Component that define and standardize the path patterns
        for Silver Layer in Data Lake.
    processing_date: datetime
        Reference date for data being processed.
    cast_map: dict
        Optional. Used to convert the dataframe columns data types
        while running the Bronze to Silver flow. Receives a dict
        containing as key(s) the entity column names to be cast,
        and as value(s) the new data types.
    """
    def __init__(self,
                 source_connector: FileFormatConnector,
                 bronze_path: BronzeLayerPath,
                 target_connector: DeltaConnector,
                 silver_path: SilverLayerPath,
                 processing_date: datetime,
                 cast_map: Optional[dict]):
        super().__init__(
            source_connector=source_connector,
            source_layer=bronze_path,
            target_connector=target_connector,
            target_layer=silver_path
        )
        self._processing_date = processing_date
        self._cast_map = cast_map

    def _cast_columns(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """
        Converts the dataframe columns data types, which are present
        in cast_map dict, while running the Bronze to Silver flow.

        Parameters
        ----------
        dataframe: DataFrame
            Contains the data being processed.

        Returns
        -------
        DataFrame
        """
        for column, dtype in self._cast_map.items():
            if dtype == pl.Datetime:
                dataframe = dataframe.with_columns([
                    pl.col(column).str.to_datetime()])
            else:
                dataframe = dataframe.with_columns(
                    pl.col(column).cast(dtype))
        return dataframe

    def transform(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the standard transformations according the rules
        defined for data stored in Silver Layer. The rules are:
            - Silver data should be partitioned by the reference date
              of data being processed (year, month and day);
            - Control fields must be created: Surrogate Key and
              Load at date;
            - Data types casting.

        Parameters
        ----------
        dataframe: DataFrame
            Contains the data being processed.

        Returns
        -------
        DataFrame
        """
        if not dataframe.is_empty():
            dataframe = self._cast_columns(dataframe=dataframe)
            dataframe = dataframe.with_columns([
                pl.lit(self._processing_date).alias("processing_date")
            ])
            execution_time = datetime.now()
            surrogate_keys = [str(uuid4()) for _ in range(len(dataframe))]
            dataframe = dataframe.with_columns([
                pl.Series(name="surrogate_key", values=surrogate_keys),
                pl.lit(execution_time).alias("data_lake_load_at"),
                pl.col("processing_date").dt.strftime("%Y").alias("year"),
                pl.col("processing_date").dt.strftime("%m").alias("month"),
                pl.col("processing_date").dt.strftime("%d").alias("day")
            ])
            dataframe = dataframe.drop("processing_date")
        return dataframe
