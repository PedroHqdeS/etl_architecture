from layers.data_lake_layer_path import DataLakeLayerPath


class SilverLayerPath(DataLakeLayerPath):
    def __init__(self, parameters: dict):
        super().__init__(parameters=parameters)

    def _build_file_path(self) -> str:
        entity = self._layer_path_params["entity"]
        path = f"{self._lake_root_url}silver/{entity}"
        return path
