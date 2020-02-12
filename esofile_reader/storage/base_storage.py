from abc import ABC, abstractmethod

from esofile_reader.utils.mini_classes import ResultsFile


class BaseStorage(ABC):
    """
    An abstract class to define metadata for result storage.

    """

    def __init__(self):
        self.files = {}

    @abstractmethod
    def store_file(self, results_file: ResultsFile, totals: bool = False) -> int:
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
