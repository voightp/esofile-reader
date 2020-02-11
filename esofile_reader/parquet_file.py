from datetime import datetime

from esofile_reader.base_file import BaseFile
from esofile_reader.utils.search_tree import Tree
from esofile_reader.utils.mini_classes import Data, ResultsFile


class ParquetFile(BaseFile):
    def __init__(self, id_: int, file_name: str, storage: Data, file_created: datetime,
                 totals: bool = False, search_tree: Tree = None, file_path: str = None):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.storage = storage
        self.file_created = file_created
        self._search_tree = search_tree
        self.totals = totals


class ParquetWarehouse:
    def __init__(self, path=None):
        self.path = path
        self.files = {}

    def _id_generator(self):
        id_ = 0
        while id_ in self.files.keys():
            id_ += 1
        return id_

    def store_file(self, results_file: ResultsFile, totals: bool = False) -> int:
        id_ = self._id_generator()
        pq_file = ParquetFile(id_, results_file.file_name, results_file.storage,
                              results_file.file_created, totals=totals,
                              search_tree=results_file._search_tree,
                              file_path=results_file.file_path)

        self.files[id] = pq_file

        return id_

    def delete_file(self, id_: int) -> None:
        del self.files[id_]

    def get_all_file_names(self):
        return [f.file_name for f in self.files.values()]
