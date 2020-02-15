import shutil
import tempfile
from datetime import datetime

from esofile_reader.base_file import BaseFile
from esofile_reader.data.pqt_data import ParquetDFData
from esofile_reader.data.sql_data import SQLData
from esofile_reader.data.df_data import DFData
from esofile_reader.totals_file import TotalsFile
from esofile_reader.utils.mini_classes import ResultsFile
from pathlib import Path
import json


class SQLFile(BaseFile):
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

    def __init__(
            self,
            id_: int,
            file_path: str,
            file_name: str,
            sql_data: SQLData,
            file_created: datetime,
            search_tree,
            totals
    ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.data = sql_data
        self.file_created = file_created
        self.search_tree = search_tree
        self.totals = totals

    def rename(self, name: str) -> None:
        self.file_name = name
        self.data.update_file_name(name)


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


class ParquetFile(BaseFile):
    def __init__(self,
                 id_: int,
                 file_path: str,
                 file_name: str,
                 data: DFData,
                 file_created: datetime,
                 search_tree,
                 totals,
                 pardir,
                 name=None
                 ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.file_created = file_created
        self.search_tree = search_tree
        self.totals = totals
        self.path = Path(pardir, name) if name else Path(pardir, f"file-{id_}")
        self.path.mkdir()
        self.data = ParquetDFData(data.tables, self.path)

    def __del__(self):
        print("REMOVING PARQUET FILE " + str(self.id_))
        shutil.rmtree(self.path, ignore_errors=True)

    def as_dict(self):
        return {
            "id_": self.id_,
            "file_path": str(self.file_path),
            "file_name": self.file_name,
            "file_created": self.file_created.timestamp(),
            "totals": self.totals,
            "results_tables": self.data.relative_results_paths(self.path),
            "header_tables": self.data.relative_header_paths(self.path)
        }

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
