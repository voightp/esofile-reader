from esofile_reader import TotalsFile
from esofile_reader.base_file import BaseFile
from esofile_reader.mini_classes import ResultsFile
from esofile_reader.storage.base_storage import BaseStorage


class DFFile(BaseFile):
    """
    A class to represent database results set.

    Attributes need to be populated from one of file
    wrappers ('EsoFile', 'DiffFile', 'TotalsFile').


    Attributes
    ----------
    id_ : int
        Unique id identifier.
    file : ResultsFile
        One of ('EsoFile', 'DiffFile', 'TotalsFile') results files..

    Notes
    -----
    Reference file must be complete!

    """

    def __init__(self, id_: int, file: ResultsFile):
        super().__init__()
        self.id_ = id_
        self.file_path = file.file_path
        self.file_name = file.file_name
        self.data = file.data
        self.file_created = file.file_created
        self.search_tree = file.search_tree
        self.totals = isinstance(file, TotalsFile)


class DFStorage(BaseStorage):
    def __init__(self):
        super().__init__()
        self.files = {}

    def _id_generator(self):
        id_ = 0
        while id_ in self.files.keys():
            id_ += 1
        return id_

    def store_file(self, results_file: ResultsFile) -> int:
        id_ = self._id_generator()
        self.files[id_] = DFFile(id_, results_file)
        return id_

    def delete_file(self, id_: int) -> None:
        del self.files[id_]

    def get_all_file_names(self):
        return [f.file_name for f in self.files.values()]
