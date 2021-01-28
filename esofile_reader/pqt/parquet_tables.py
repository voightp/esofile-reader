import shutil
from pathlib import Path

from esofile_reader.df.df_tables import DFTables
from esofile_reader.pqt.parquet_frame import ParquetFrame, VirtualParquetFrame
from esofile_reader.processing.progress_logger import BaseLogger


class ParquetTables(DFTables):
    _Frame = ParquetFrame

    def __init__(self):
        super().__init__()

    @classmethod
    def from_dftables(
        cls, dftables: DFTables, pardir: Path, logger: BaseLogger = None
    ) -> "ParquetTables":
        """ Create parquet data from DataFrame like class. """
        pqt = cls()
        for k, v in dftables.tables.items():
            pqt.tables[k] = cls._Frame.from_df(v, k, pardir, logger=logger)
        return pqt

    @classmethod
    def from_fs(cls, pardir: Path):
        """ Create parquet data from filesystem directory. """
        pqt = cls()
        dirs = [p for p in Path(pardir).iterdir() if p.is_dir()]
        for p in dirs:
            table = str(p.name).split("-", maxsplit=1)[1]
            pqf = cls._Frame.from_fs(p)
            pqt.tables[table] = pqf
        return pqt

    def copy_to(self, new_pardir: Path) -> "ParquetTables":
        """ Copy parquet tables to another location. """
        new_tables = type(self)()
        for table, pqf in self.tables.items():
            new_tables[table] = pqf.copy_to(new_pardir)
        return new_tables


class VirtualParquetTables(ParquetTables):
    _Frame = VirtualParquetFrame

    def __init__(self):
        super().__init__()

    @classmethod
    def from_fs(cls, pardir: Path):
        pqt = super().from_fs(pardir)
        for p in Path(pardir).iterdir():
            shutil.rmtree(p)
        return pqt
