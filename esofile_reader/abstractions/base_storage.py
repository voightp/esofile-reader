from abc import ABC, abstractmethod

from esofile_reader.typehints import ResultsFileType


class BaseStorage(ABC):
    """
    An abstract class to define metadata for result storage.

    """

    def __init__(self):
        self.files = {}

    @abstractmethod
    def store_file(self, results_file: ResultsFileType) -> int:
        """ Store file in the 'class' database. """
        pass

    @abstractmethod
    def delete_file(self, id_: int) -> None:
        """ Delete file from the 'class' database. """
        pass

    @abstractmethod
    def get_all_file_names(self):
        """ Get all stored names. """
        pass
