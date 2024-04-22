from datetime import datetime
from pipelines.source_to_bronze_pipeline import SourceToBronzePipeline
from layers.bronze_layer_path import BronzeLayerPath

from connectors.csv_connector import CsvConnector

from connectors.local_source import LocalSource

local_source = LocalSource()
csv = CsvConnector()
params = {
    "entity": "entity",
    "execution_time": datetime.now()
}
bronze_layer = BronzeLayerPath(parameters=params)

source_to_bronze = SourceToBronzePipeline(
    source_connector=local_source,
    bronze_path=bronze_layer,
    target_connector=csv
)

source_to_bronze.start_ingestion()
