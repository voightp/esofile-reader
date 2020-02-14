from esofile_reader.base_file import BaseFile
from esofile_reader.eso_file import EsoFile
from esofile_reader.storage.df_storage import DFStorage
from esofile_reader.utils.mini_classes import ResultsFile
from esofile_reader.data.pqt_data import ParquetData
from esofile_reader.storage.storage_files import ParquetFile
import tempfile
from pathlib import Path
from zipfile import ZipFile
from esofile_reader.totals_file import TotalsFile
import json

import shutil


class ParquetStorage(DFStorage):
    def __init__(self, path=None):
        super().__init__()
        self.files = {}
        self.path = path
        self.temp_dir = tempfile.mkdtemp(prefix="chartify-")

    def __del__(self):
        print("REMOVING PARQUET STORAGE " + str(self.temp_dir))
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def store_file(self, results_file: ResultsFile) -> int:
        id_ = self._id_generator()
        self.files[id_] = ParquetFile(id_, results_file, pardir=self.temp_dir)
        return id_

    def delete_file(self, id_: int) -> None:
        shutil.rmtree(self.files[id_].path, ignore_errors=True)
        del self.files[id_]

    def save(self):
        pass

    def save_as(self, root, name):
        files = [f.as_dict() for f in self.files.values()]
        print(files)
        print(json.dumps(files, indent=4))


if __name__ == "__main__":
    p = Path(Path(__file__).parents[2], "tests", "eso_files", "eplusout1.eso")
    # p = "C:/users/vojtechp1/desktop/eplusout.eso"
    st = ParquetStorage()
    st.store_file(EsoFile(p))
    st.store_file(EsoFile(p))
    st.store_file(EsoFile(p))
    st.save_as(1,2)
