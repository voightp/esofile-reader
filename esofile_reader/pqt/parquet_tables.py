from pathlib import Path
from typing import Type

from esofile_reader.df.df_tables import DFTables
from esofile_reader.pqt.parquet_frame import ParquetFrame, VirtualParquetFrame, DfParquetFrame
from esofile_reader.processing.progress_logger import BaseLogger


class ParquetTables(DFTables):
    _Frame = ParquetFrame

    def __init__(self):
        super().__init__()

    @classmethod
    def predict_n_chunks(cls, df_tables: DFTables) -> int:
        return sum([cls._Frame.predict_n_chunks(*df.shape) for df in df_tables.tables.values()])

    @classmethod
    def from_dftables(
        cls, dftables: DFTables, pardir: Path, logger: BaseLogger = None
    ) -> "ParquetTables":
        """ Create parquet data from DataFrame like class. """
        pqt_tables = cls()
        for table_name, df in dftables.tables.items():
            pqt_tables.tables[table_name] = cls._Frame.from_df(
                df, f"table-{table_name}", pardir, logger=logger
            )
        return pqt_tables

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


class DfParquetTables(VirtualParquetTables):
    _Frame = DfParquetFrame

    def __init__(self):
        super().__init__()


def get_conversion_n_steps(
    old_tables: ParquetTables, new_tables_class: Type[ParquetTables]
) -> int:
    n = 0
    for table in old_tables.values():
        steps_get_df = table.n_chunks
        steps_store_df = new_tables_class._Frame.predict_n_chunks(
            len(table.index), len(table.columns)
        )
        n += steps_get_df + steps_store_df
    return n


def convert_tables(
    old_tables: ParquetTables, new_tables_class: Type[ParquetTables], logger: BaseLogger
) -> ParquetTables:
    new_tables = new_tables_class()
    for table_name, table in old_tables.tables.items():
        logger.log_section("reading dataframe")
        df = table.as_df(logger)

        logger.log_section("deleting old data")
        pardir = table.workdir.parent
        table.clean_up()

        logger.log_section("creating new tables")
        new_tables.tables[table_name] = new_tables._Frame.from_df(
            df, table_name, pardir, logger=logger
        )
    return new_tables
