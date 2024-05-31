import polars as pl
from typing import Union, Optional
from utils.logging_utils import get_logger
from connectors.base.external_source import ExternalSource
from connectors.base.file_format_connector import FileFormatConnector
from layers.base.data_lake_layer_path import DataLakeLayerPath


class DataPipeline:
    """
    Base code defining the responsibilities of a
    component to standardize the data processing
    flow through Data Lake's layers.

    Parameters
    ----------
    source_connector: ExternalSource | FileFormatConnector
        Component that connects and extracts data from any external
        source or Data Lake's layer in any needed format.
    source_layer: DataLakeLayerPath | None
        If the data source is a layer in Data Lake, this must receive
        the component that standardize the data path in source layer.
        Otherwise, it can be null.
    target_connector: FileFormatConnector
        Component that connects and writes data in any Data Lake's layer
        in any needed format.
    target_layer: DataLakeLayerPath
        Component that standardize the path for Data Lake's target layer.
    """
    def __init__(self,
                 source_connector: Union[ExternalSource, FileFormatConnector],
                 source_layer: Optional[DataLakeLayerPath],
                 target_connector: FileFormatConnector,
                 target_layer: DataLakeLayerPath):
        self._logger = get_logger(name=__name__)
        self._source_connector = source_connector
        self._source_layer = source_layer
        self._target_connector = target_connector
        self._target_layer = target_layer

    def extract(self) -> pl.DataFrame:
        """
        Method to extract data from source. The source
        component must have the method 'extract_data'
        implemented.

        Returns
        ----------
        DataFrame
        """
        dataframe = self._source_connector.extract_data(
            path=self._source_layer.get_file_path()
        )
        return dataframe

    def transform(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """
        This method must be overridden in the descending classes
        if the target layer allows rules to be applied to the
        data to be stored.

        Parameters
        ----------
        dataframe: DataFrame
            Contains the data will be transformed.

        Returns
        ----------
        DataFrame
        """
        return dataframe

    def load(self, dataframe: pl.DataFrame) -> None:
        """
        Method to store data in a target layer in Data Lake.
        The target component must have the method 'load_data'
        implemented.

        Parameters
        ----------
        dataframe: DataFrame
            Contains the data will be stored.

        Returns
        ----------
        None
        """
        self._target_connector.load_data(
            dataframe=dataframe,
            path=self._target_layer.get_file_path()
        )

    def start_ingestion(self) -> None:
        """
        Orchestrates method calls to apply the ETL flow
        in data being processed. This method must be
        invoked to process data through Data Lake's layers.

        Returns
        ----------
        None
        """
        dataframe = self.extract()
        dataframe = self.transform(dataframe=dataframe)
        self.load(dataframe=dataframe)
