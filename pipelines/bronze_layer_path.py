from pipelines.data_lake_layer_path import DataLakeLayerPath

class BronzeLayerPath(DataLakeLayerPath):
    def __init__(self, parameters: dict):
        super().__init__(parameters=parameters)

    def _build_file_path(self) -> str:
        execution_time = (
            self._layer_path_params["execution_time"]
                .strftime("%Y/%m/%d")
        )
        entity = self._layer_path_params["entity"]
        main_entity = self._layer_path_params.get("main_entity", None)
        aux_main_entity = entity if main_entity is None else main_entity
        path = (
            f"bronze/{aux_main_entity}/" +
            f"{execution_time}/{entity}"
        )
        return path
