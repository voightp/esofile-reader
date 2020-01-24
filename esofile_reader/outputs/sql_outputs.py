import pandas as pd
from datetime import datetime
from typing import Sequence, List, Dict
from esofile_reader.constants import *
from esofile_reader.outputs.base_outputs import BaseOutputs
from esofile_reader.outputs.sql_outputs_functions import results_table_generator, \
    dates_table_generator, create_index_insert, create_results_insert
from uuid import uuid1
from esofile_reader import EsoFile
from esofile_reader.utils.utils import profile
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, \
    DateTime, Boolean, Sequence, Text, inspect, select
import contextlib
from sqlalchemy import exc
import sqlalchemy

from esofile_reader.outputs.df_outputs_functions import merge_peak_outputs, slicer

from esofile_reader.utils.utils import id_gen
from esofile_reader.utils.mini_classes import Variable


class SQLOutputs(BaseOutputs):
    FILE_TABLE = "result_files"

    def __init__(self, path=None):
        (self.engine,
         self.metadata) = self.set_up_db(path=path)

    @classmethod
    @profile
    def set_up_db(cls, path=None, echo=False):
        path = path if path else ":memory:"

        engine = create_engine(f'sqlite:///{path}', echo=echo)
        metadata = MetaData(engine, reflect=True)

        if cls.FILE_TABLE not in metadata.tables.keys():
            file = Table(
                cls.FILE_TABLE, metadata,
                Column("id", Integer, Sequence('db_id_seq'), primary_key=True),
                Column("file_path", String(120)),
                Column("file_name", String(50)),
                Column("file_timestamp", DateTime),
                Column("indexes_table", String(20)),
                Column("timestep_table", String(20)),
                Column("hourly_table", String(20)),
                Column("daily_table", String(20)),
                Column("monthly_table", String(20)),
                Column("annual_table", String(20)),
                Column("runperiod_table", String(20))
            )

            with contextlib.suppress(exc.InvalidRequestError, exc.OperationalError):
                file.create()

        return engine, metadata

    @classmethod
    @profile
    def store_file(cls, result_file, engine, metadata):
        f = metadata.tables[cls.FILE_TABLE]
        ins = f.insert().values(
            file_path=result_file.file_path,
            file_name=result_file.file_name,
            file_timestamp=result_file.created
        )

        # insert new file data
        with engine.connect() as conn:
            id_ = conn.execute(ins).inserted_primary_key[0]
            indexes_name = dates_table_generator(metadata, id_)
            conn.execute(f.update().where(f.c.id == id_).values(indexes_table=indexes_name))

            indexes_ins = {}
            for interval in result_file.available_intervals:
                outputs = result_file.data
                # create result table for specific interval
                results_name = results_table_generator(metadata, id_, interval)

                # create data inserts
                results_ins = create_results_insert(outputs.get_only_numeric_data(interval))
                indexes_ins.update(create_index_insert(interval, outputs.tables[interval]))

                # insert results into tables
                conn.execute(metadata.tables[results_name].insert(), results_ins)

                # store result table reference
                conn.execute(f.update().where(f.c.id == id_).values(**{f"{interval}_table": results_name}))

            # store index data
            conn.execute(metadata.tables[indexes_name].insert().values(**indexes_ins))

        return id_

    def set_data(self, interval: str, df: pd.DataFrame):
        pass

    def get_available_intervals(self) -> List[str]:
        pass

    def get_variables_dct(self, interval: str) -> Dict[int, Variable]:
        pass

    def get_all_variables_dct(self) -> Dict[str, Dict[int, Variable]]:
        pass

    def get_variable_ids(self, interval: str) -> List[int]:
        pass

    def get_all_variable_ids(self) -> List[int]:
        pass

    def get_variables_df(self, interval: str) -> pd.DataFrame:
        pass

    def get_all_variables_df(self) -> pd.DataFrame:
        pass

    def rename_variable(self, interval: str, id_, key_name, var_name) -> None:
        pass

    def add_variable(self, variable: str, array: Sequence) -> None:
        pass

    def remove_variables(self, interval: str, ids: List[int]) -> None:
        return

    def get_number_of_days(self, interval: str, start_date: datetime = None, end_date: datetime = None) -> pd.Series:
        pass

    def get_days_of_week(self, interval: str, start_date: datetime = None, end_date: datetime = None) -> pd.Series:
        pass

    def get_results(self, interval: str, ids: List[int], start_date: datetime = None, end_date: datetime = None,
                    include_day: bool = False) -> pd.DataFrame:
        pass

    def get_global_max_results(self, interval: str, ids: List[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        pass

    def get_global_min_results(self, interval: str, ids: List[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        pass


if __name__ == "__main__":
    ef = EsoFile(r"C:\Users\vojtechp1\PycharmProjects\eso_reader\tests\eso_files\eplusout1.eso",
                 report_progress=True)
    # ef = EsoFile(r"C:\Users\vojtechp1\desktop\eplusout.eso", report_progress=True)

    # eng, meta = SQLOutputs.set_up_db(echo=False)
    #
    # SQLOutputs.store_file(ef, eng, meta)

    eng, meta = SQLOutputs.set_up_db("test.db", echo=False)

    SQLOutputs.store_file(ef, eng, meta)
