from pathlib import Path

from esofile_reader.base_file import BaseFile
from esofile_reader.database_file import DatabaseFile
from esofile_reader.eso_file import EsoFile
from esofile_reader.storage.base_storage import BaseStorage
from esofile_reader.utils.mini_classes import ResultsFile, Data
from esofile_reader.utils.search_tree import Tree
from datetime import datetime


class PqtStorage(BaseStorage):

    def __init__(self):
        super().__init__()
        self.files = {}

    def _id_generator(self):
        id_ = 0
        while id_ in self.files.keys():
            id_ += 1
        return id_

    def store_file(self, results_file: ResultsFile, totals: bool = False) -> int:
        id_ = self._id_generator()
        db_file = DatabaseFile(id_, results_file.file_name, results_file.data,
                               results_file.file_created, totals=totals,
                               search_tree=results_file._search_tree,
                               file_path=results_file.file_path)

        # store file in class database
        self.files[id_] = db_file

        return id_

    def delete_file(self, id_: int) -> None:
        del self.files[id_]

    def get_all_file_names(self):
        return [f.file_name for f in self.files.values()]


class ParquetFile(BaseFile):
    def __init__(self, id_: int, file_name: str, data: Data, file_created: datetime,
                 totals: bool = False, search_tree: Tree = None, file_path: str = None):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.data = data
        self.file_created = file_created
        self._search_tree = search_tree
        self.totals = totals


if __name__ == "__main__":
    p = Path(Path(__file__).parents[2], "tests", "eso_files", "eplusout1.eso")
    ef = EsoFile(p)
    print(ef.data)
