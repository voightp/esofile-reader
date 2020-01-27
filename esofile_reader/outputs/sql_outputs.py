import pandas as pd
from datetime import datetime
from typing import Sequence, List, Dict
from esofile_reader.constants import *
from esofile_reader.database_file import DatabaseFile
from esofile_reader.outputs.base_outputs import BaseOutputs
from esofile_reader.outputs.df_outputs_functions import df_dt_slicer, sr_dt_slicer, merge_peak_outputs
from esofile_reader.outputs.sql_outputs_functions import results_table_generator, \
    dates_table_generator, create_index_insert, create_results_insert, destringify_df
from esofile_reader import EsoFile
from esofile_reader.utils.utils import profile
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, \
    DateTime, Boolean, Text, inspect, select
import contextlib

from sqlalchemy import exc
from esofile_reader.utils.mini_classes import Variable


class SQLOutputs(BaseOutputs):
    FILE_TABLE = "result_files"
    SEPARATOR = "\t"
    ENGINE = None
    METADATA = None

    def __init__(self, id_):
        self.id_ = id_

    @classmethod
    @profile
    def set_up_db(cls, path=None, echo=False):
        path = path if path else ":memory:"

        engine = create_engine(f'sqlite:///{path}', echo=echo)
        metadata = MetaData(bind=engine)
        metadata.reflect()

        if cls.FILE_TABLE not in metadata.tables.keys():
            file = Table(
                cls.FILE_TABLE, metadata,
                Column("id", Integer, autoincrement=True, primary_key=True),
                Column("file_path", String(120)),
                Column("file_name", String(50)),
                Column("file_created", DateTime),
                Column("indexes_table", String(20)),
                Column("timestep_table", String(20)),
                Column("hourly_table", String(20)),
                Column("daily_table", String(20)),
                Column("monthly_table", String(20)),
                Column("annual_table", String(20)),
                Column("runperiod_table", String(20)),
            )

            with contextlib.suppress(exc.InvalidRequestError, exc.OperationalError):
                file.create()

        cls.ENGINE = engine
        cls.METADATA = metadata

    @classmethod
    @profile
    def store_file(cls, result_file):
        engine = cls.ENGINE
        metadata = cls.METADATA

        f = metadata.tables[cls.FILE_TABLE]
        ins = f.insert().values(
            file_path=result_file.file_path,
            file_name=result_file.file_name,
            file_created=result_file.file_created
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
                results_ins = create_results_insert(outputs.get_all_results(interval), cls.SEPARATOR)
                indexes_ins.update(create_index_insert(interval, outputs.tables[interval], cls.SEPARATOR))

                # insert results into tables
                conn.execute(metadata.tables[results_name].insert(), results_ins)

                # store result table reference
                conn.execute(f.update().where(f.c.id == id_).values(**{f"{interval}_table": results_name}))

            # store index data
            conn.execute(metadata.tables[indexes_name].insert().values(**indexes_ins))

        db_file = DatabaseFile(id_, result_file.file_name, SQLOutputs(id_),
                               result_file.file_created, result_file._search_tree,
                               result_file.file_path)

        return db_file

    @classmethod
    @profile
    def delete_file(cls, id_):
        files = cls.METADATA.tables[cls.FILE_TABLE]

        with cls.ENGINE.connect() as conn:
            columns = [
                files.c.indexes_table,
                files.c.timestep_table,
                files.c.hourly_table,
                files.c.daily_table,
                files.c.monthly_table,
                files.c.annual_table,
                files.c.runperiod_table
            ]

            res = conn.execute(select(columns).where(files.c.id == id_)).first()
            if res:
                # remove tables based on file reference
                for table_name in [t for t in res if t]:
                    cls.METADATA.tables[table_name].drop()

                # remove result file
                conn.execute(files.delete().where(files.c.id == id_))

            else:
                raise KeyError(f"Cannot delete file id '{id_}'.")

        # reset metadata to reflect changes
        cls.METADATA = MetaData(bind=cls.ENGINE)
        cls.METADATA.reflect()

    def set_data(self, interval: str, df: pd.DataFrame):
        pass

    def _get_results_table(self, interval):
        files = self.METADATA.tables[self.FILE_TABLE]

        switch = {
            TS: files.c.timestep_table,
            H: files.c.hourly_table,
            D: files.c.daily_table,
            M: files.c.monthly_table,
            A: files.c.annual_table,
            RP: files.c.runperiod_table
        }

        with self.ENGINE.connect() as conn:
            table_name = conn.execute(select([switch[interval]]).where(files.c.id == self.id_)).scalar()
            table = self.METADATA.tables[table_name]

        return table

    def _get_index_table(self):
        files = self.METADATA.tables[self.FILE_TABLE]

        with self.ENGINE.connect() as conn:
            table_name = conn.execute(select([files.c.indexes_table]).where(files.c.id == self.id_)).scalar()
            table = self.METADATA.tables[table_name]

        return table

    def get_available_intervals(self) -> List[str]:
        files = self.METADATA.tables[self.FILE_TABLE]
        columns = [
            files.c.timestep_table,
            files.c.hourly_table,
            files.c.daily_table,
            files.c.monthly_table,
            files.c.runperiod_table,
            files.c.annual_table
        ]

        intervals = []
        with self.ENGINE.connect() as conn:
            res = conn.execute(select(columns).where(files.c.id == self.id_)).first()
            for interval, table in zip([TS, H, D, M, RP, A], res):
                if table:
                    intervals.append(interval)
        return intervals

    def get_datetime_index(self, interval: str) -> pd.DatetimeIndex:
        table = self._get_index_table()

        switch = {
            TS: table.c.timestep_dt,
            H: table.c.hourly_dt,
            D: table.c.daily_dt,
            M: table.c.monthly_dt,
            A: table.c.annual_dt,
            RP: table.c.runperiod_dt
        }

        with self.ENGINE.connect() as conn:
            res = conn.execute(select([switch[interval]])).scalar()
            datetime_index = pd.DatetimeIndex(res.split(self.SEPARATOR), name="timestamp")

        return datetime_index

    @profile
    def get_variables_dct(self, interval: str) -> Dict[int, Variable]:
        variables_dct = {}
        table = self._get_results_table(interval)
        with self.ENGINE.connect() as conn:
            res = conn.execute(select([table.c.id, table.c.interval, table.c.key,
                                       table.c.variable, table.c.units]))

            for row in res:
                variables_dct[row[0]] = Variable(row[1], row[2], row[3], row[4])

        return variables_dct

    def get_all_variables_dct(self) -> Dict[str, Dict[int, Variable]]:
        all_variables_dct = {}
        for interval in self.get_available_intervals():
            all_variables_dct[interval] = self.get_variables_dct(interval)
        return all_variables_dct

    def get_variable_ids(self, interval: str) -> List[int]:
        table = self._get_results_table(interval)
        with self.ENGINE.connect() as conn:
            res = conn.execute(select([table.c.id]))
            ids = [row[0] for row in res]
        return ids

    def get_all_variable_ids(self) -> List[int]:
        all_ids = []
        for interval in self.get_available_intervals():
            all_ids.extend(self.get_variable_ids(interval))
        return all_ids

    def get_variables_df(self, interval: str) -> pd.DataFrame:
        table = self._get_results_table(interval)
        with self.ENGINE.connect() as conn:
            res = conn.execute(select([table.c.id, table.c.interval, table.c.key,
                                       table.c.variable, table.c.units]))
            df = pd.DataFrame(res, columns=["id", "interval", "key", "variable", "units"])
        return df

    def get_all_variables_df(self) -> pd.DataFrame:
        frames = []
        for interval in self.get_available_intervals():
            frames.append(self.get_variables_df(interval))
        return pd.concat(frames)

    def rename_variable(self, interval: str, id_, key_name, var_name) -> None:
        table = self._get_results_table(interval)
        with self.ENGINE.connect() as conn:
            conn.execute(table.update() \
                         .where(table.c.id == id_) \
                         .values(key=key_name, variable=var_name))

    def add_variable(self, variable: Variable, array: Sequence) -> None:
        table = self._get_results_table(variable.interval)
        str_array = self.SEPARATOR.join([str(i) for i in array])

        with self.ENGINE.connect() as conn:
            statement = table.insert().values({**variable._asdict(), "values": str_array})
            id_ = conn.execute(statement).inserted_primary_key[0]

        return id_

    def remove_variables(self, interval: str, ids: List[int]) -> None:
        table = self._get_results_table(interval)

        with self.ENGINE.connect() as conn:
            conn.execute(table.delete().where(table.c.id.in_(ids)))

    def get_number_of_days(self, interval: str, start_date: datetime = None, end_date: datetime = None) -> pd.Series:
        table = self._get_index_table()

        switch = {
            M: table.c.monthly_n_days,
            A: table.c.annual_n_days,
            RP: table.c.runperiod_n_days
        }

        with self.ENGINE.connect() as conn:
            res = conn.execute(select([switch[interval]])).scalar()
            if res:
                n_days = [int(i) for i in res.split(self.SEPARATOR)]
                sr = pd.Series(n_days, name=N_DAYS_COLUMN)
            else:
                raise KeyError(f"'{N_DAYS_COLUMN}' column is not available "
                               f"on the given data set.")

        index = self.get_datetime_index(interval)
        if index is not None:
            sr.index = index

        return sr_dt_slicer(sr, start_date, end_date)

    def get_days_of_week(self, interval: str, start_date: datetime = None, end_date: datetime = None) -> pd.Series:
        table = self._get_index_table()

        switch = {
            TS: table.c.timestep_days,
            H: table.c.hourly_days,
            D: table.c.daily_days
        }

        with self.ENGINE.connect() as conn:
            res = conn.execute(select([switch[interval]])).scalar()
            if res:
                sr = pd.Series(res.split(self.SEPARATOR), name=DAY_COLUMN)
            else:
                raise KeyError(f"'{DAY_COLUMN}' column is not available "
                               f"on the given data set.")

        index = self.get_datetime_index(interval)
        if index is not None:
            sr.index = index

        return sr_dt_slicer(sr, start_date, end_date)

    def get_results(self, interval: str, ids: List[int], start_date: datetime = None, end_date: datetime = None,
                    include_day: bool = False) -> pd.DataFrame:
        table = self._get_results_table(interval)
        with self.ENGINE.connect() as conn:
            res = conn.execute(table.select().where(table.c.id.in_(ids)))
            df = pd.DataFrame(res, columns=["id", "interval", "key", "variable", "units", "values"])
            if df.empty:
                raise KeyError(f"Cannot find results, any of given ids: "
                               f"'{', '.join([str(id_) for id_ in ids])}' "
                               f"is not included.")

            df.set_index(["id", "interval", "key", "variable", "units"], inplace=True)
            df = destringify_df(df)

        index = self.get_datetime_index(interval)
        if index is not None:
            df.index = index

        if include_day:
            try:
                day_sr = self.get_days_of_week(interval)
                df.insert(0, day_sr)
                df.set_index(DAY_COLUMN, append=True, inplace=True)
            except KeyError:
                try:
                    df[DAY_COLUMN] = df.index.strftime("%A")
                    df.set_index(DAY_COLUMN, append=True, inplace=True)
                except AttributeError:
                    pass

        return df_dt_slicer(df, start_date, end_date)

    def get_all_results(self, interval: str) -> pd.DataFrame:
        ids = self.get_variable_ids(interval)
        df = self.get_results(interval, ids)
        return df

    def _global_peak(self, interval, ids, start_date, end_date, max_=True):
        """ Return maximum or minimum value and datetime of occurrence. """
        df = self.get_results(interval, ids, start_date, end_date)

        vals = pd.DataFrame(df.max() if max_ else df.min()).T
        ixs = pd.DataFrame(df.idxmax() if max_ else df.idxmin()).T

        df = merge_peak_outputs(ixs, vals)
        df = df.iloc[[0]]  # report only first occurrence

        return df

    def get_global_max_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        return self._global_peak(interval, ids, start_date, end_date)

    def get_global_min_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        return self._global_peak(interval, ids, start_date, end_date, max_=False)


if __name__ == "__main__":
    ef = EsoFile(r"C:\Users\vojte\Desktop\Python\eso_reader\tests\eso_files\eplusout_all_intervals.eso",
                 report_progress=True)
    # ef = EsoFile(r"C:\Users\vojtechp1\desktop\eplusout.eso", report_progress=True)

    # eng, meta = SQLOutputs.set_up_db(echo=False)
    #
    # SQLOutputs.store_file(ef, eng, meta)

    SQLOutputs.set_up_db(r"C:\Users\vojte\Desktop\Python\eso_reader\esofile_reader\outputs\test.db", echo=False)

    f = SQLOutputs.store_file(ef)
    d = f.get_datetime_index("daily")
    print(d)
