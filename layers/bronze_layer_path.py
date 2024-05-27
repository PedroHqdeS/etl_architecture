from layers.base.data_lake_layer_path import DataLakeLayerPath


class BronzeLayerPath(DataLakeLayerPath):
    def __init__(self, parameters: dict):
        super().__init__(parameters=parameters)

    def _build_file_path(self) -> str:
        processing_date = (
            self._layer_path_params["processing_date"]
                .strftime("%Y/%m/%d")
        )
        entity = self._layer_path_params["entity"]
        path = f"{self._lake_root_url}bronze/{entity}/{processing_date}"
        return path
