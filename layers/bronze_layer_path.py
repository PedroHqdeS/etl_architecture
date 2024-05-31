from layers.base.data_lake_layer_path import DataLakeLayerPath


class BronzeLayerPath(DataLakeLayerPath):
    """
    Component to define and standardize the path
    patterns for Bronze Layer in Data Lake.

    Parameters
    ----------
    parameters: dict
        Contains the components(names) that builds the
        path pattern for a given entity in Bronze Layer.
    """
    def __init__(self, parameters: dict):
        super().__init__(parameters=parameters)

    def _build_file_path(self) -> str:
        """
        Builds the path pattern for a given entity in
        Bronze Layer. The path in this layer follows
        the pattern: bronze/<ENTITY_NAME>/<YEAR>/<MONTH>/<DAY>.
        To build this, the dict variable when this class is called
        must receive the 'processing_date' key, which value is the
        reference date (datetime) for data will be processed; and
        the 'entity' key, which value is the entity (table) name.

        Returns
        -------
        str
        """
        processing_date = (
            self._layer_path_params["processing_date"]
                .strftime("%Y/%m/%d")
        )
        entity = self._layer_path_params["entity"]
        path = f"{self._lake_root_url}bronze/{entity}/{processing_date}"
        return path
