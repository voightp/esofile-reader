from pathlib import Path

from esofile_reader.base_file import BaseFile
from esofile_reader.database_file import DatabaseFile
from esofile_reader.eso_file import EsoFile
from esofile_reader.storage.base_storage import BaseStorage
from esofile_reader.utils.mini_classes import ResultsFile, Data
from esofile_reader.utils.search_tree import Tree
from datetime import datetime
from esofile_reader.data.pqt_data import ParquetData
import tempfile
from profilehooks import profile
from pathlib import Path

import shutil

from fastparquet import write
from pyarrow.parquet import write_table
import pyarrow as pa


class ParquetStorage(BaseStorage):
    def __init__(self, path=None, temp_dir=None):
        super().__init__()
        self.files = {}
        self.path = path
        self.temp_dir = temp_dir if temp_dir else tempfile.mkdtemp(prefix="chartify-")

    def _id_generator(self):
        id_ = 0
        while id_ in self.files.keys():
            id_ += 1
        return id_

    def store_file(self, results_file: ResultsFile, totals: bool = False) -> int:
        id_ = self._id_generator()

        # store file in the instance storage
        self.files[id_] = ParquetFile(id_, results_file, totals=totals, dir=self.temp_dir)

        return id_

    def delete_file(self, id_: int) -> None:
        shutil.rmtree(self.files[id_].temp_dir, ignore_errors=True)
        del self.files[id_]

    def get_all_file_names(self):
        return [f.file_name for f in self.files.values()]

    def save(self):
        pass

    def save_as(self, root, name):
        pass


class ParquetFile(BaseFile):
    def __init__(self, id_: int, file: ResultsFile, totals: bool = False, path=None, dir=None):
        super().__init__()
        self.id_ = id_
        self.file_path = file.file_path
        self.file_name = file.file_name
        self.file_created = file.file_created
        self._search_tree = file._search_tree
        self.path = path
        self.temp_dir = tempfile.mkdtemp(prefix="chartify-", dir=dir)
        self.data = ParquetData(file.data.tables, self.temp_dir)
        self.totals = totals

    # @profile
    # def save(self):
    #     header_df = self.data.get_all_variables_df()
    #     header_table = pa.Table.from_pandas(header_df)
    #     header_path = tempfile.mkstemp(dir=self.temp_dir, prefix="header-")[1]
    #     print(header_path)
    #
    #     write_table(header_table, header_path)
    #
    #     import time
    #     time.sleep(10)
    #
    # def save_as(self, root, name):
    #     p = Path(root, f"{name}.parquet")
    #     self.path = p
    #     shutil.make_archive(p, "zip", self.temp_dir)
    #
    # def load(self):
    #     pass


if __name__ == "__main__":
    p = Path(Path(__file__).parents[2], "tests", "eso_files", "eplusout1.eso")
    # p = "C:/users/vojtechp1/desktop/eplusout.eso"
    st = ParquetStorage()
    st.store_file(EsoFile(p))


