import polars as pl
from connectors.base.external_source import ExternalSource
from connectors.base.file_format_connector import FileFormatConnector
from layers.bronze_layer_path import BronzeLayerPath
from pipelines.base.data_pipeline import DataPipeline


class SourceToBronzePipeline(DataPipeline):
    """
    Component to standardize the data processing from
    any external source to Data Lake's Bronze Layer.

    Parameters
    ----------
    source_connector: ExternalSource
        Component that connects and extracts data from any external
        source.
    target_connector: FileFormatConnector
        Component that write data in Data Lake's Bronze layer
        in any needed format.
    bronze_path: BronzeLayerPath
        Component that define and standardize the path patterns
        for Bronze Layer in Data Lake.
    """
    def __init__(self,
                 source_connector: ExternalSource,
                 target_connector: FileFormatConnector,
                 bronze_path: BronzeLayerPath):
        super().__init__(
            source_connector=source_connector,
            source_layer=None,
            target_connector=target_connector,
            target_layer=bronze_path
        )

    def extract(self) -> pl.DataFrame:
        """
        Method to extract data from external sources.
        The source component must have the method
        'extract_data' implemented.

        Returns
        ----------
        DataFrame
        """
        return self._source_connector.extract_data()
