import json
import os
import shutil
import tempfile
from pathlib import Path
import contextlib

from esofile_reader.eso_file import EsoFile
from esofile_reader.storage.df_storage import DFStorage
from esofile_reader.storage.storage_files import ParquetFile
from esofile_reader.totals_file import TotalsFile
from esofile_reader.utils.mini_classes import ResultsFile


class ParquetStorage(DFStorage):
    EXT = ".cfy"

    def __init__(self, path=None):
        super().__init__()
        self.files = {}
        self.path = Path(path) if path else path
        self.temp_dir = tempfile.mkdtemp(prefix="chartify-")

    def __del__(self):
        print("REMOVING PARQUET STORAGE " + str(self.temp_dir))
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def store_file(self, results_file: ResultsFile) -> int:
        id_ = self._id_generator()
        file = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            data=results_file.data,
            file_created=results_file.file_created,
            search_tree=results_file.search_tree,
            totals=isinstance(results_file, TotalsFile),
            pardir=self.temp_dir,
        )
        self.files[id_] = file
        return id_

    def delete_file(self, id_: int) -> None:
        shutil.rmtree(self.files[id_].path, ignore_errors=True)
        del self.files[id_]

    def save_as(self, dir_, name):
        # store json summary file
        files = [f.as_dict() for f in self.files.values()]
        tempson = str(Path(self.temp_dir, "files.json"))
        with open(tempson, "w") as f:
            json.dump(files, f, indent=4)

        # store all the tempdir content
        zf = shutil.make_archive(str(Path(dir_, f"{name}")), "zip", self.temp_dir)

        # change zip to custom extension
        p = Path(zf)
        path = p.with_suffix(self.EXT)
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)
        p.rename(path)
        self.path = path

    def save(self):
        if not self.path:
            raise FileNotFoundError("Path not defined! Call 'save_as' first.")
        dir_ = self.path.parent
        name = self.path.with_suffix("").name
        self.save_as(dir_, name)


if __name__ == "__main__":
    p = Path(Path(__file__).parents[2], "tests", "eso_files", "eplusout1.eso")
    # p = "C:/users/vojtechp1/desktop/eplusout.eso"
    st = ParquetStorage()
    st.store_file(EsoFile(p))
    st.store_file(EsoFile(p))
    st.store_file(EsoFile(p))
    st.save_as(r"C:/users/vojte/desktop", "blabla")
    st.delete_file(0)
    st.save()
