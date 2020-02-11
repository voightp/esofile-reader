import logging
from datetime import datetime
from typing import Sequence, List, Dict

import pandas as pd
from sqlalchemy import Table, select

from esofile_reader.constants import *
from esofile_reader.data.base_data import BaseData
from esofile_reader.data.df_functions import df_dt_slicer, sr_dt_slicer, merge_peak_outputs
from esofile_reader.storage.sql_functions import destringify_values
from esofile_reader.utils.mini_classes import Variable
from esofile_reader.utils.utils import profile


class SQLData(BaseData):

    def __init__(self, id_, sql_storage):
        self.id_ = id_
        self.storage = sql_storage

    def update_file_name(self, name: str) -> None:
        ft = self.storage.file_table

        with self.storage.engine.connect() as conn:
            conn.execute(ft.update() \
                         .where(ft.c.id == self.id_) \
                         .values(file_name=name))

    def _get_table(self, column: str, ft: Table) -> Table:
        with self.storage.engine.connect() as conn:
            table_name = conn.execute(select([column]) \
                                      .where(ft.c.id == self.id_)).scalar()
            table = self.storage.metadata.tables[table_name]
        return table

    def _get_results_table(self, interval: str) -> Table:
        ft = self.storage.file_table

        switch = {
            TS: ft.c.timestep_outputs_table,
            H: ft.c.hourly_outputs_table,
            D: ft.c.daily_outputs_table,
            M: ft.c.monthly_outputs_table,
            A: ft.c.annual_outputs_table,
            RP: ft.c.runperiod_outputs_table,
            RANGE: ft.c.range_outputs_table
        }

        return self._get_table(switch[interval], ft)

    def _get_datetime_table(self, interval: str) -> Table:
        ft = self.storage.file_table

        switch = {
            TS: ft.c.timestep_dt_table,
            H: ft.c.hourly_dt_table,
            D: ft.c.daily_dt_table,
            M: ft.c.monthly_dt_table,
            A: ft.c.annual_dt_table,
            RP: ft.c.runperiod_dt_table,
        }

        return self._get_table(switch[interval], ft)

    def _get_n_days_table(self, interval: str) -> Table:
        ft = self.storage.file_table

        switch = {
            M: ft.c.monthly_n_days_table,
            A: ft.c.annual_n_days_table,
            RP: ft.c.runperiod_n_days_table
        }

        return self._get_table(switch[interval], ft)

    def _get_day_table(self, interval: str) -> Table:
        ft = self.storage.file_table

        switch = {
            TS: ft.c.timestep_day_table,
            H: ft.c.hourly_day_table,
            D: ft.c.daily_day_table
        }

        return self._get_table(switch[interval], ft)

    def get_available_intervals(self) -> List[str]:
        ft = self.storage.file_table
        columns = [
            ft.c.timestep_outputs_table,
            ft.c.hourly_outputs_table,
            ft.c.daily_outputs_table,
            ft.c.monthly_outputs_table,
            ft.c.runperiod_outputs_table,
            ft.c.annual_outputs_table,
            ft.c.range_outputs_table
        ]

        intervals = []
        with self.storage.engine.connect() as conn:
            res = conn.execute(select(columns).where(ft.c.id == self.id_)).first()
            for interval, table in zip([TS, H, D, M, RP, A, RANGE], res):
                if table:
                    intervals.append(interval)
        return intervals

    def get_datetime_index(self, interval: str) -> pd.DatetimeIndex:
        table = self._get_datetime_table(interval)

        with self.storage.engine.connect() as conn:
            res = conn.execute(table.select()).fetchall()
            datetime_index = pd.DatetimeIndex([r[0] for r in res], name="timestamp")

        return datetime_index

    @profile
    def get_variables_dct(self, interval: str) -> Dict[int, Variable]:
        variables_dct = {}
        table = self._get_results_table(interval)
        with self.storage.engine.connect() as conn:
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
        with self.storage.engine.connect() as conn:
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
        with self.storage.engine.connect() as conn:
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
        with self.storage.engine.connect() as conn:
            conn.execute(table.update() \
                         .where(table.c.id == id_) \
                         .values(key=key_name, variable=var_name))

    def _validate(self, interval: str, array: Sequence[float]) -> bool:
        table = self._get_results_table(interval)

        with self.storage.engine.connect() as conn:
            res = conn.execute(select([table.c.str_values])).scalar()

        # number of elements in array
        n = res.count(self.storage.SEPARATOR) + 1

        return len(array) == n

    def insert_variable(self, variable: Variable, array: Sequence[float]) -> None:
        if self._validate(variable.interval, array):
            table = self._get_results_table(variable.interval)
            str_array = self.storage.SEPARATOR.join([str(i) for i in array])

            with self.storage.engine.connect() as conn:
                statement = table.insert().values({**variable._asdict(), "str_values": str_array})
                id_ = conn.execute(statement).inserted_primary_key[0]

            return id_
        else:
            logging.warning("Cannot add new variable '{0} {1} {2} {3}'. "
                            "Number of elements '({4})' does not match!".format(*variable, len(array)))

    def update_variable(self, interval: str, id_: int, array: Sequence[float]):
        if self._validate(interval, array):
            table = self._get_results_table(interval)
            str_array = self.storage.SEPARATOR.join([str(i) for i in array])

            with self.storage.engine.connect() as conn:
                conn.execute(table.update() \
                             .where(table.c.id == id_) \
                             .values(str_values=str_array))
            return id_
        else:
            logging.warning(f"Cannot update variable '{id_}'. "
                            f"Number of elements '({len(array)})' does not match!")

    def delete_variables(self, interval: str, ids: List[int]) -> None:
        table = self._get_results_table(interval)

        with self.storage.engine.connect() as conn:
            conn.execute(table.delete().where(table.c.id.in_(ids)))

    def get_number_of_days(self, interval: str, start_date: datetime = None,
                           end_date: datetime = None) -> pd.Series:
        table = self._get_n_days_table(interval)

        with self.storage.engine.connect() as conn:
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

        with self.storage.engine.connect() as conn:
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

        with self.storage.engine.connect() as conn:
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
