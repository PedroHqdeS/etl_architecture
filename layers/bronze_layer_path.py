from layers.data_lake_layer_path import DataLakeLayerPath

class BronzeLayerPath(DataLakeLayerPath):
    def __init__(self, parameters: dict):
        super().__init__(parameters=parameters)

    def _build_file_path(self) -> str:
        execution_time = (
            self._layer_path_params["execution_time"]
                .strftime("%Y/%m/%d")
        )
        entity = self._layer_path_params["entity"]
        path = f"bronze/{entity}/{execution_time}"
        return path
