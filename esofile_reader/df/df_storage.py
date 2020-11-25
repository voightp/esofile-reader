from esofile_reader.abstractions.base_storage import BaseStorage
from esofile_reader.df.df_file import DFFile
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.typehints import ResultsFileType


class DFStorage(BaseStorage):
    def __init__(self):
        super().__init__()
        self.files = {}

    def store_file(self, results_file: ResultsFileType) -> int:
        id_gen = incremental_id_gen(checklist=set(self.files.keys()))
        id_ = next(id_gen)
        self.files[id_] = DFFile(id_, results_file)
        return id_

    def delete_file(self, id_: int) -> None:
        del self.files[id_]

    def get_all_file_names(self):
        # always return sorted by id
        return [file.file_name for file in dict(sorted(self.files.items())).values()]
