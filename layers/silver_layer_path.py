from layers.base.data_lake_layer_path import DataLakeLayerPath


class SilverLayerPath(DataLakeLayerPath):
    """
    Component to define and standardize the path
    patterns for Silver Layer in Data Lake.

    Parameters
    ----------
    parameters: dict
        Contains the components(names) that builds the
        path pattern for a given entity in Silver Layer.
    """
    def __init__(self, parameters: dict):
        super().__init__(parameters=parameters)

    def _build_file_path(self) -> str:
        """
        Builds the path pattern for a given entity in
        Silver Layer. The path in this layer follows
        the pattern: silver/<ENTITY_NAME>.
        To build this, the dict variable when this class
        is called must receive the 'entity' key, which value
        is the entity (table) name.

        Returns
        -------
        str
        """
        entity = self._layer_path_params["entity"]
        path = f"{self._lake_root_url}silver/{entity}"
        return path
