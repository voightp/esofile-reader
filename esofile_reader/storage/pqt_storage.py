import contextlib
import os
import shutil
import tempfile
from pathlib import Path
from typing import Union
from zipfile import ZipFile
from profilehooks import profile
import io

from esofile_reader.storage.df_storage import DFStorage
from esofile_reader.storage.storage_files import ParquetFile
from esofile_reader.totals_file import TotalsFile
from esofile_reader.utils.mini_classes import ResultsFile


class ParquetStorage(DFStorage):
    EXT = ".cfs"

    def __init__(self, path=None):
        super().__init__()
        self.files = {}
        self.path = Path(path) if path else path
        self.workdir = tempfile.mkdtemp(prefix="chartify-")

    def __del__(self):
        print("REMOVING PARQUET STORAGE " + str(self.workdir))
        shutil.rmtree(self.workdir, ignore_errors=True)

    @classmethod
    @profile(entries=10, sort="time")
    def load(cls, path: Union[str, Path]):
        """ Load ParquetStorage from filesystem. """
        path = path if isinstance(path, Path) else Path(path)
        if path.suffix != cls.EXT:
            raise IOError(f"Invalid file type loaded. Only '{cls.EXT}' files are allowed")

        pqs = ParquetStorage(path)
        with ZipFile(path, "r") as zf:
            # tmp = tempfile.mkdtemp()
            # zf.extractall(tmp)
            # for f in [p for p in Path(tmp).iterdir() if p.suffix == ParquetFile.EXT]:
            #     print(f)
            #     pqf = ParquetFile.load_file(f, pqs.workdir)
            #     pqs.files[pqf.id_] = pqf
            # shutil.rmtree(tmp, ignore_errors=True)

            for name in zf.namelist():
                bf = io.BytesIO(zf.read(name))
                pqf = ParquetFile.load_file(bf, pqs.workdir)
                pqs.files[pqf.id_] = pqf

        return pqs

    def store_file(self, results_file: ResultsFile) -> int:
        """ Store results file as 'ParquetFile'. """
        id_ = self._id_generator()
        file = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            data=results_file.data,
            file_created=results_file.file_created,
            search_tree=results_file.search_tree,
            totals=isinstance(results_file, TotalsFile),
            pardir=self.workdir,
        )
        self.files[id_] = file
        return id_

    def delete_file(self, id_: int) -> None:
        """ Delete file with given id. """
        shutil.rmtree(self.files[id_].workdir, ignore_errors=True)
        del self.files[id_]

    @profile(entries=10, sort="time")
    def save_as(self, dir_, name):
        """ Save parquet storage into given location. """
        self.path = str(Path(dir_, f"{name}{self.EXT}"))

        # save all files
        with ZipFile(self.path, "w") as zf:
            for f in self.files.values():
                p = f.save_as(self.workdir, f.name)
                zf.write(p, arcname=f.name + ParquetFile.EXT)

    def save(self):
        """ Save parquet storage. """
        if not self.path:
            raise FileNotFoundError("Path not defined! Call 'save_as' first.")
        dir_ = self.path.parent
        name = self.path.with_suffix("").name
        self.save_as(dir_, name)
