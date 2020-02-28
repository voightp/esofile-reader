from esofile_reader.storage.base_storage import BaseStorage
from esofile_reader.storage.storage_files import DFFile
from esofile_reader.utils.mini_classes import ResultsFile


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
