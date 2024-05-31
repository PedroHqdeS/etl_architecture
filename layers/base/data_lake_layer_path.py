from abc import ABC, abstractmethod
from utils.data_lake_utils import get_url
from utils.logging_utils import get_logger


class DataLakeLayerPath(ABC):
    """
    Base code defining the responsibilities of a
    component to standardize the path patterns
    for Data Lake's layers.

    Parameters
    ----------
    parameters: dict
        Contains the components(names) that builds the
        path pattern for a given entity in a given layer.
    """
    def __init__(self, parameters: dict):
        self._logger = get_logger(name=__name__)
        self._layer_path_params = parameters
        self._lake_root_url = get_url()
        self._file_path = self._build_file_path()

    @abstractmethod
    def _build_file_path(self) -> str:
        """
        This method must be implemented to build the
        path pattern for a given entity in the layer
        being implemented.

        Returns
        -------
        str
        """
        pass

    def set_file_path(self, new_path: str) -> None:
        """
        For specific use cases, this function can be called
        to define manually the path for an entity in Data Lake.
        It is used if you have to consume any data that its
        path is out of defined standard.

        Parameters
        ----------
        new_path: str
            New path that should be used to access data in Data Lake

        Returns
        -------
        None
        """
        self._file_path = new_path

    def get_file_path(self) -> str:
        """
        Returns the path for a given entity in Data Lake's
        implemented layer.

        Returns
        -------
        str
        """
        return self._file_path
