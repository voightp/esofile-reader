import contextlib
import traceback
from datetime import datetime
from typing import Sequence, List, Dict

import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, \
    DateTime, select
from sqlalchemy import exc

from esofile_reader.constants import *
from esofile_reader.database_file import DatabaseFile
from esofile_reader.outputs.base_data import BaseData
from esofile_reader.outputs.df_functions import df_dt_slicer, sr_dt_slicer, merge_peak_outputs
from esofile_reader.outputs.sql_functions import create_results_table, \
    create_datetime_table, merge_df_values, create_value_insert, create_n_days_table, \
    create_day_table, destringify_values
from esofile_reader.utils.mini_classes import Variable
from esofile_reader.utils.search_tree import Tree
from esofile_reader.utils.utils import profile


class SQLData(BaseData):
    FILE_TABLE = "result-files"
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
                Column("range_outputs_table", String(50)),
                Column("timestep_outputs_table", String(50)),
                Column("hourly_outputs_table", String(50)),
                Column("daily_outputs_table", String(50)),
                Column("monthly_outputs_table", String(50)),
                Column("annual_outputs_table", String(50)),
                Column("runperiod_outputs_table", String(50)),
                Column("timestep_dt_table", String(50)),
                Column("hourly_dt_table", String(50)),
                Column("daily_dt_table", String(50)),
                Column("monthly_dt_table", String(50)),
                Column("annual_dt_table", String(50)),
                Column("runperiod_dt_table", String(50)),
                Column("timestep_day_table", String(50)),
                Column("hourly_day_table", String(50)),
                Column("daily_day_table", String(50)),
                Column("monthly_n_days_table", String(50)),
                Column("annual_n_days_table", String(50)),
                Column("runperiod_n_days_table", String(50))
            )

            with contextlib.suppress(exc.InvalidRequestError, exc.OperationalError):
                file.create()

        cls.ENGINE = engine
        cls.METADATA = metadata

    @classmethod
    @profile
    def store_file(cls, result_file):
        if not cls.METADATA or not cls.ENGINE:
            raise AttributeError(f"Cannot store file into database."
                                 f"\nIt's required to call '{cls.__name__}.set_up_db(path)'"
                                 f" to create a database engine and metadata first.")

        f = cls.METADATA.tables[cls.FILE_TABLE]
        ins = f.insert().values(
            file_path=result_file.file_path,
            file_name=result_file.file_name,
            file_created=result_file.file_created
        )

        # insert new file data
        with cls.ENGINE.connect() as conn:
            id_ = conn.execute(ins).inserted_primary_key[0]

            for interval in result_file.available_intervals:
                f_upd = {}

                outputs = result_file.data.tables[interval]
                # create result table
                results_table = create_results_table(cls.METADATA, id_, interval)
                f_upd[f"{interval}_outputs_table"] = results_table.name

                # store numeric values
                df = result_file.data.get_all_results(interval)
                sr = merge_df_values(df, cls.SEPARATOR)

                ins = []
                for index, values in sr.iteritems():
                    ins.append(
                        {
                            "id": index[0],
                            "interval": index[1],
                            "key": index[2],
                            "variable": index[3],
                            "units": index[4],
                            "str_values": values
                        }
                    )
                conn.execute(results_table.insert(), ins)

                # create index table
                if isinstance(outputs.index, pd.DatetimeIndex):
                    index_table = create_datetime_table(cls.METADATA, id_, interval)
                    conn.execute(index_table.insert(), create_value_insert(outputs.index))
                    f_upd[f"{interval}_dt_table"] = index_table.name

                # store 'n days' data
                if N_DAYS_COLUMN in outputs.columns:
                    n_days_table = create_n_days_table(cls.METADATA, id_, interval)
                    conn.execute(n_days_table.insert(), create_value_insert(outputs[N_DAYS_COLUMN]))
                    f_upd[f"{interval}_n_days_table"] = n_days_table.name

                # store 'day of week' data
                if DAY_COLUMN in outputs.columns:
                    day_table = create_day_table(cls.METADATA, id_, interval)
                    conn.execute(day_table.insert(), create_value_insert(outputs[DAY_COLUMN]))
                    f_upd[f"{interval}_day_table"] = day_table.name

                conn.execute(f.update().where(f.c.id == id_).values(f_upd))

                db_file = DatabaseFile(id_, result_file.file_name, SQLData(id_),
                                       result_file.file_created, result_file._search_tree,
                                       result_file.file_path)

        return db_file

    @classmethod
    @profile
    def delete_file(cls, id_: int) -> None:
        files = cls.METADATA.tables[cls.FILE_TABLE]

        with cls.ENGINE.connect() as conn:
            res = conn.execute(files.select().where(files.c.id == id_)).first()
            if res:
                # remove tables based on file reference
                for table_name in res:
                    try:
                        cls.METADATA.tables[table_name].drop()
                    except KeyError:
                        pass

                # remove result file
                conn.execute(files.delete().where(files.c.id == id_))

            else:
                raise KeyError(f"Cannot delete file id '{id_}'.")

        # reset metadata to reflect changes
        cls.METADATA = MetaData(bind=cls.ENGINE)
        cls.METADATA.reflect()

    @classmethod
    @profile
    def load_file(cls, id_: int) -> DatabaseFile:
        files = cls.METADATA.tables[cls.FILE_TABLE]

        with cls.ENGINE.connect() as conn:
            res = conn.execute(
                select([files.c.id, files.c.file_name, files.c.file_created,
                        files.c.file_path]).where(files.c.id == id_)).first()
        if res:
            data = SQLData(res[0])

            tree = Tree()
            tree.populate_tree(data.get_all_variables_dct())

            return DatabaseFile(res[0], res[1], data, res[2],
                                file_path=res[3], search_tree=tree)

        else:
            raise KeyError(f"Cannot load file id '{id_}'.\n"
                           f"{traceback.format_exc()}")

    @classmethod
    @profile
    def load_all_files(cls) -> List[DatabaseFile]:
        files = cls.METADATA.tables[cls.FILE_TABLE]

        with cls.ENGINE.connect() as conn:
            res = conn.execute(files.select(files.c.id))

        ids = [r[0] for r in res]
        db_files = [cls.load_file(id_) for id_ in ids]

        return db_files

    def update_file_name(self, name: str) -> None:
        files = self.METADATA.tables[self.FILE_TABLE]

        with self.ENGINE.connect() as conn:
            conn.execute(files.update() \
                         .where(files.c.id == self.id_) \
                         .values(file_name=name))

    def _get_table(self, column: str, files: Table) -> Table:
        with self.ENGINE.connect() as conn:
            table_name = conn.execute(select([column]) \
                                      .where(files.c.id == self.id_)).scalar()
            table = self.METADATA.tables[table_name]
        return table

    def _get_results_table(self, interval: str) -> Table:
        files = self.METADATA.tables[self.FILE_TABLE]

        switch = {
            TS: files.c.timestep_outputs_table,
            H: files.c.hourly_outputs_table,
            D: files.c.daily_outputs_table,
            M: files.c.monthly_outputs_table,
            A: files.c.annual_outputs_table,
            RP: files.c.runperiod_outputs_table,
            RANGE: files.c.range_outputs_table
        }

        return self._get_table(switch[interval], files)

    def _get_datetime_table(self, interval: str) -> Table:
        files = self.METADATA.tables[self.FILE_TABLE]

        switch = {
            TS: files.c.timestep_dt_table,
            H: files.c.hourly_dt_table,
            D: files.c.daily_dt_table,
            M: files.c.monthly_dt_table,
            A: files.c.annual_dt_table,
            RP: files.c.runperiod_dt_table,
        }

        return self._get_table(switch[interval], files)

    def _get_n_days_table(self, interval: str) -> Table:
        files = self.METADATA.tables[self.FILE_TABLE]

        switch = {
            M: files.c.monthly_n_days_table,
            A: files.c.annual_n_days_table,
            RP: files.c.runperiod_n_days_table
        }

        return self._get_table(switch[interval], files)

    def _get_day_table(self, interval: str) -> Table:
        files = self.METADATA.tables[self.FILE_TABLE]

        switch = {
            TS: files.c.timestep_day_table,
            H: files.c.hourly_day_table,
            D: files.c.daily_day_table
        }

        return self._get_table(switch[interval], files)

    def get_available_intervals(self) -> List[str]:
        files = self.METADATA.tables[self.FILE_TABLE]
        columns = [
            files.c.timestep_outputs_table,
            files.c.hourly_outputs_table,
            files.c.daily_outputs_table,
            files.c.monthly_outputs_table,
            files.c.runperiod_outputs_table,
            files.c.annual_outputs_table,
            files.c.range_outputs_table
        ]

        intervals = []
        with self.ENGINE.connect() as conn:
            res = conn.execute(select(columns).where(files.c.id == self.id_)).first()
            for interval, table in zip([TS, H, D, M, RP, A, RANGE], res):
                if table:
                    intervals.append(interval)
        return intervals

    def get_datetime_index(self, interval: str) -> pd.DatetimeIndex:
        table = self._get_datetime_table(interval)

        with self.ENGINE.connect() as conn:
            res = conn.execute(table.select()).fetchall()
            datetime_index = pd.DatetimeIndex([r[0] for r in res], name="timestamp")

        return datetime_index

    def get_daterange_index(self, interval: str) -> pd.DatetimeIndex:
        table = self._get_datetime_table(interval)

        with self.ENGINE.connect() as conn:
            res = conn.execute(table.select()).fetchall()
            datetime_index = pd.DatetimeIndex([r[0] for r in res], name="timestamp")

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

    def update_variable_name(self, interval: str, id_, key_name, var_name) -> None:
        table = self._get_results_table(interval)
        with self.ENGINE.connect() as conn:
            conn.execute(table.update() \
                         .where(table.c.id == id_) \
                         .values(key=key_name, variable=var_name))

    def _validate(self, interval: str, array: Sequence[float]) -> bool:
        table = self._get_results_table(interval)

        with self.ENGINE.connect() as conn:
            res = conn.execute(select([table.c.str_values])).scalar()

        # number of elements in array
        n = res.count(self.SEPARATOR) + 1

        return len(array) == n

    def insert_variable(self, variable: Variable, array: Sequence[float]) -> None:
        if self._validate(variable.interval, array):
            table = self._get_results_table(variable.interval)
            str_array = self.SEPARATOR.join([str(i) for i in array])

            with self.ENGINE.connect() as conn:
                statement = table.insert().values({**variable._asdict(), "str_values": str_array})
                id_ = conn.execute(statement).inserted_primary_key[0]

            return id_
        else:
            print("Cannot add new variable '{0} {1} {2} {3}'. "
                  "Number of elements '({4})' does not match!".format(*variable, len(array)))

    def update_variable(self, interval: str, id_: int, array: Sequence[float]):
        if self._validate(interval, array):
            table = self._get_results_table(interval)
            str_array = self.SEPARATOR.join([str(i) for i in array])

            with self.ENGINE.connect() as conn:
                conn.execute(table.update() \
                             .where(table.c.id == id_) \
                             .values(str_values=str_array))
            return id_
        else:
            print(f"Cannot update variable '{id_}'. "
                  f"Number of elements '({len(array)})' does not match!")

    def delete_variables(self, interval: str, ids: List[int]) -> None:
        table = self._get_results_table(interval)

        with self.ENGINE.connect() as conn:
            conn.execute(table.delete().where(table.c.id.in_(ids)))

    def get_number_of_days(self, interval: str, start_date: datetime = None,
                           end_date: datetime = None) -> pd.Series:
        table = self._get_n_days_table(interval)

        with self.ENGINE.connect() as conn:
            res = conn.execute(table.select()).fetchall()
            if res:
                sr = pd.Series([r[0] for r in res], name=N_DAYS_COLUMN)
            else:
                raise KeyError(f"'{N_DAYS_COLUMN}' column is not available "
                               f"on the given data set.")

        index = self.get_datetime_index(interval)
        if index is not None:
            sr.index = index

        return sr_dt_slicer(sr, start_date, end_date)

    def get_days_of_week(self, interval: str, start_date: datetime = None,
                         end_date: datetime = None) -> pd.Series:
        table = self._get_day_table(interval)

        with self.ENGINE.connect() as conn:
            res = conn.execute(table.select()).fetchall()
            if res:
                sr = pd.Series([r[0] for r in res], name=DAY_COLUMN)
            else:
                raise KeyError(f"'{DAY_COLUMN}' column is not available "
                               f"on the given data set.")

        index = self.get_datetime_index(interval)
        if index is not None:
            sr.index = index

        return sr_dt_slicer(sr, start_date, end_date)

    def get_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                    end_date: datetime = None, include_day: bool = False) -> pd.DataFrame:
        ids = ids if isinstance(ids, list) else [ids]
        table = self._get_results_table(interval)

        with self.ENGINE.connect() as conn:
            res = conn.execute(table.select().where(table.c.id.in_(ids)))
            df = pd.DataFrame(res, columns=["id", "interval", "key", "variable", "units", "values"])
            if df.empty:
                raise KeyError(f"Cannot find results, any of given ids: "
                               f"'{', '.join([str(id_) for id_ in ids])}' "
                               f"is not included.")

            df.set_index(["id", "interval", "key", "variable", "units"], inplace=True)
            df = destringify_values(df)

        if interval == RANGE:
            # create default 'range' index
            df.index.rename(RANGE, inplace=True)
        else:
            df.index = self.get_datetime_index(interval)
            if include_day:
                try:
                    day_sr = self.get_days_of_week(interval, start_date, end_date)
                    df.insert(0, DAY_COLUMN, day_sr)
                    df.set_index(DAY_COLUMN, append=True, inplace=True)
                except KeyError:
                    df[DAY_COLUMN] = df.index.strftime("%A")
                    df.set_index(DAY_COLUMN, append=True, inplace=True)

        return df_dt_slicer(df, start_date, end_date)

    def get_all_results(self, interval: str) -> pd.DataFrame:
        ids = self.get_variable_ids(interval)
        df = self.get_results(interval, ids)
        return df

    def _global_peak(self, interval: str, ids: Sequence[int], start_date: datetime,
                     end_date: datetime, max_: bool = True) -> pd.DataFrame:
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
