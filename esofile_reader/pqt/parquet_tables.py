from pathlib import Path

from esofile_reader.df.df_tables import DFTables
from esofile_reader.pqt.parquet_frame import ParquetFrame
from esofile_reader.processing.progress_logger import BaseLogger


class ParquetTables(DFTables):
    def __init__(self):
        super().__init__()

    @classmethod
    def from_dftables(
        cls, dftables: DFTables, pardir: Path, logger: BaseLogger = None
    ) -> "ParquetTables":
        """ Create parquet data from DataFrame like class. """
        pqt = ParquetTables()
        for k, v in dftables.tables.items():
            pqt.tables[k] = ParquetFrame.from_df(v, k, pardir, logger=logger)
        return pqt

    @classmethod
    def from_fs(cls, pardir: Path):
        """ Create parquet data from filesystem directory. """
        pqt = ParquetTables()
        dirs = [p for p in Path(pardir).iterdir() if p.is_dir()]
        for p in dirs:
            table = str(p.name).split("-", maxsplit=1)[1]
            pqf = ParquetFrame.from_fs(p)
            pqt.tables[table] = pqf
        return pqt

    def copy_to(self, new_pardir: Path) -> "ParquetTables":
        new_tables = ParquetTables()
        for table, pqf in self.tables.items():
            new_tables[table] = pqf.copy_to(new_pardir)
        return new_tables
