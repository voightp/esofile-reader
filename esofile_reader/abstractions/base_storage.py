from abc import ABC, abstractmethod
from pathlib import Path

from esofile_reader.processing.progress_logger import BaseLogger
from esofile_reader.typehints import ResultsFileType, PathLike


class BaseStorage(ABC):
    """
    An abstract class to define metadata for result storage.

    """

    EXT = ".cfs"

    def __init__(self):
        self.files = {}
        self.path = None

    @classmethod
    @abstractmethod
    def load_storage(cls, path: PathLike, logger: BaseLogger = None) -> "BaseStorage":
        """ Load ParquetStorage from filesystem. """
        pass

    @abstractmethod
    def save_as(self, dir_: PathLike, name: str, logger: BaseLogger = None) -> Path:
        """ Save storage into given location. """
        pass

    @abstractmethod
    def save(self, logger: BaseLogger = None) -> Path:
        """ Save storage into previously set location. """
        pass

    @abstractmethod
    def merge_with(self, storage_path: PathLike, logger: BaseLogger = None) -> None:
        """ Merge this storage with another one. """
        pass

    @abstractmethod
    def store_file(self, results_file: ResultsFileType, logger: BaseLogger = None) -> int:
        """ Store file in the instance database. """
        pass

    @abstractmethod
    def delete_file(self, id_: int, logger: BaseLogger = None) -> None:
        """ Delete file identified by given id. """
        pass

    def get_all_file_names(self):
        # always return sorted by id
        return [file.file_name for file in dict(sorted(self.files.items())).values()]
