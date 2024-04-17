import logging
from abc import ABC, abstractmethod

class DataLakeLayerPath(ABC):
    def __init__(self, parameters: dict):
        self._logger = logging.getLogger(__name__)
        self._layer_path_params = parameters
        self._file_path = self._build_file_path()

    @abstractmethod
    def _build_file_path(self) -> str:
        pass

    def _set_file_path(self, new_path: str) -> None:
        self._file_path = new_path

    def get_file_path(self) -> str:
        return self._file_path
