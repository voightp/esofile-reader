import logging
from datetime import datetime
from typing import Sequence, List, Dict, Optional, Union

import pandas as pd
from sqlalchemy import Table, select, String, Integer

from esofile_reader.constants import *
from esofile_reader.data.base_data import BaseData
from esofile_reader.data.df_functions import (
    df_dt_slicer,
    sr_dt_slicer,
    merge_peak_outputs,
)
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import Variable, SimpleVariable
from esofile_reader.storage.sql_functions import (
    destringify_values,
    get_table_name,
    parse_table_name,
    create_special_table,
    create_value_insert,
)


class SQLData(BaseData):
    def __init__(self, id_, sql_storage):
        self.id_ = id_
        self.storage = sql_storage

    def update_file_name(self, name: str) -> None:
        ft = self.storage.file_table
        with self.storage.engine.connect() as conn:
            conn.execute(ft.update().where(ft.c.id == self.id_).values(file_name=name))

    def _get_table_names(self, table_type: str) -> List[str]:
        ft = self.storage.file_table
        switch = {
            "numeric": ft.c.numeric_tables,
            "special": ft.c.special_tables,
            "datetime": ft.c.datetime_tables,
        }
        col = switch[table_type]
        # check if there's table reference
        with self.storage.engine.connect() as conn:
            res = conn.execute(select([col]).where(ft.c.id == self.id_)).scalar()
            return res.split(self.storage.SEPARATOR) if res else []

    def _get_table(self, table_name: str, table_type: str) -> Table:
        names = self._get_table_names(table_type)
        if table_name not in names:
            logging.warning(f"Cannot find file reference {table_name} in {table_type}!")
        return self.storage.metadata.tables[table_name]

    def _get_results_table(self, interval: str) -> Table:
        name = get_table_name(self.id_, "results", interval)
        return self._get_table(name, "numeric")

    def _get_datetime_table(self, interval: str) -> Table:
        name = get_table_name(self.id_, "index", interval)
        return self._get_table(name, "datetime")

    def _get_special_table(self, interval: str, key: str) -> Table:
        name = get_table_name(self.id_, key, interval)
        return self._get_table(name, "special")

    def is_simple(self, interval: str) -> bool:
        return len(self.get_levels(interval)) == 4

    def get_levels(self, interval: str) -> List[str]:
        table = self._get_results_table(interval)
        levels = [c.name for c in table.columns if c.name != STR_VALUES]
        return levels

    def get_available_intervals(self) -> List[str]:
        names = self._get_table_names("numeric")
        return [parse_table_name(r)[2] for r in names]

    def get_datetime_index(self, interval: str) -> pd.DatetimeIndex:
        table = self._get_datetime_table(interval)
        with self.storage.engine.connect() as conn:
            res = conn.execute(table.select()).fetchall()
            datetime_index = pd.DatetimeIndex([r[0] for r in res], name=TIMESTAMP_COLUMN)
        return datetime_index

    def get_variables_dct(self, interval: str) -> Dict[int, Union[SimpleVariable, Variable]]:
        variables_dct = {}
        variables_df = self.get_variables_df(interval)
        v = SimpleVariable if self.is_simple(interval) else Variable
        for row in variables_df.to_numpy():
            variables_dct[row[0]] = v(*row[1:])
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
        columns = [ID_LEVEL, INTERVAL_LEVEL, KEY_LEVEL, TYPE_LEVEL, UNITS_LEVEL]
        if self.is_simple(interval):
            s = [table.c.id, table.c.interval, table.c.key, table.c.units]
            columns.remove(TYPE_LEVEL)
        else:
            s = [table.c.id, table.c.interval, table.c.key, table.c.type, table.c.units]
        with self.storage.engine.connect() as conn:
            res = conn.execute(select(s))
            df = pd.DataFrame(res, columns=columns)
        return df

    def get_all_variables_df(self) -> pd.DataFrame:
        frames = []
        for interval in self.get_available_intervals():
            frames.append(self.get_variables_df(interval))
        return pd.concat(frames)

    def update_variable_name(
            self, interval: str, id_: int, new_key: str, new_type: str = ""
    ) -> None:
        table = self._get_results_table(interval)
        with self.storage.engine.connect() as conn:
            kwargs = {"key": new_key} if self.is_simple(interval) \
                else {"key": new_key, "type": new_type}
            conn.execute(
                table.update().where(table.c.id == id_).values(**kwargs)
            )

    def _validate(self, interval: str, array: Sequence[float]) -> bool:
        table = self._get_results_table(interval)
        with self.storage.engine.connect() as conn:
            res = conn.execute(select([table.c.str_values])).scalar()
        # number of elements in array
        n = res.count(self.storage.SEPARATOR) + 1
        return len(array) == n

    def insert_column(self, variable: Variable, array: Sequence[float]) -> Optional[int]:
        if self._validate(variable.interval, array):
            table = self._get_results_table(variable.interval)
            str_array = self.storage.SEPARATOR.join([str(i) for i in array])
            all_ids = self.get_all_variable_ids()
            id_gen = incremental_id_gen(checklist=all_ids, start=100)
            id_ = next(id_gen)
            with self.storage.engine.connect() as conn:
                statement = table.insert().values(
                    {ID_LEVEL: id_, **variable._asdict(), STR_VALUES: str_array}
                )
                conn.execute(statement)
            return id_
        else:
            logging.warning(
                "Cannot add new variable '{0} {1} {2} {3}'. "
                "Number of elements '({4})' does not match!".format(*variable, len(array))
            )

    def update_variable_values(self, interval: str, id_: int, array: Sequence[float]):
        if self._validate(interval, array):
            table = self._get_results_table(interval)
            str_array = self.storage.SEPARATOR.join([str(i) for i in array])
            with self.storage.engine.connect() as conn:
                conn.execute(
                    table.update().where(table.c.id == id_).values(str_values=str_array)
                )
            return id_
        else:
            logging.warning(
                f"Cannot update variable '{id_}'. "
                f"Number of elements '({len(array)})' does not match!"
            )

    def insert_special_column(self, interval: str, key: str, array: Sequence) -> None:
        ft = self.storage.file_table
        if self._validate(interval, array):
            column_type = Integer if all(map(lambda x: isinstance(x, int), array)) else String
            special_table = create_special_table(
                self.storage.metadata, self.id_, interval, key, column_type
            )
            with self.storage.engine.connect() as conn:
                conn.execute(special_table.insert(), create_value_insert(array))
                res = conn.execute(
                    select([ft.c.special_tables]).where(ft.c.id == self.id_)
                ).scalar()
                new_res = res + self.storage.SEPARATOR + special_table.name
                conn.execute(
                    ft.update().where(ft.c.id == self.id_).values(special_tables=new_res)
                )
        else:
            logging.warning(
                f"Cannot add special variable '{key} into table {interval}'. "
                f"Number of elements '({len(array)})' does not match!"
            )

    def delete_variables(self, interval: str, ids: List[int]) -> None:
        table = self._get_results_table(interval)
        with self.storage.engine.connect() as conn:
            conn.execute(table.delete().where(table.c.id.in_(ids)))

    def get_special_column(
            self,
            interval: str,
            key: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> pd.Series:
        table = self._get_special_table(interval, key)
        with self.storage.engine.connect() as conn:
            res = conn.execute(table.select()).fetchall()
            if self.is_simple(interval):
                name = (SPECIAL, interval, key, "")
            else:
                name = (SPECIAL, interval, key, "", "")
            sr = pd.Series([r[0] for r in res], name=name)
        index = self.get_datetime_index(interval)
        if index is not None:
            sr.index = index
        return sr_dt_slicer(sr, start_date, end_date)

    def get_results(
            self,
            interval: str,
            ids: Sequence[int],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            include_day: bool = False,
    ) -> pd.DataFrame:
        ids = ids if isinstance(ids, list) else [ids]
        columns = [ID_LEVEL, INTERVAL_LEVEL, KEY_LEVEL, TYPE_LEVEL, UNITS_LEVEL]
        if self.is_simple(interval):
            columns.remove(TYPE_LEVEL)
        table = self._get_results_table(interval)
        with self.storage.engine.connect() as conn:
            res = conn.execute(table.select().where(table.c.id.in_(ids)))
            df = pd.DataFrame(res, columns=[*columns, "values"])
            if df.empty:
                raise KeyError(
                    f"Cannot find results, any of given ids: "
                    f"'{', '.join([str(id_) for id_ in ids])}' "
                    f"is not included."
                )

            df.set_index(columns, inplace=True)
            df = destringify_values(df)
        if interval == RANGE:
            # create default 'range' index
            df.index.rename(RANGE, inplace=True)
        else:
            df.index = self.get_datetime_index(interval)
            if include_day:
                try:
                    day_sr = self.get_special_column(interval, DAY_COLUMN, start_date, end_date)
                    df.insert(0, DAY_COLUMN, day_sr)
                    df.set_index(DAY_COLUMN, append=True, inplace=True)
                except KeyError:
                    df[DAY_COLUMN] = df.index.strftime("%A")
                    df.set_index(DAY_COLUMN, append=True, inplace=True)

        return df_dt_slicer(df, start_date, end_date)

    def get_numeric_table(self, interval: str) -> pd.DataFrame:
        ids = self.get_variable_ids(interval)
        df = self.get_results(interval, ids)
        return df

    def _global_peak(
            self,
            interval: str,
            ids: Sequence[int],
            start_date: datetime,
            end_date: datetime,
            max_: bool = True,
    ) -> pd.DataFrame:
        """ Return maximum or minimum value and datetime of occurrence. """
        df = self.get_results(interval, ids, start_date, end_date)

        vals = pd.DataFrame(df.max() if max_ else df.min()).T
        ixs = pd.DataFrame(df.idxmax() if max_ else df.idxmin()).T

        df = merge_peak_outputs(ixs, vals)
        df = df.iloc[[0]]  # report only first occurrence

        return df

    def get_global_max_results(
            self,
            interval: str,
            ids: Sequence[int],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        return self._global_peak(interval, ids, start_date, end_date)

    def get_global_min_results(
            self,
            interval: str,
            ids: Sequence[int],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        return self._global_peak(interval, ids, start_date, end_date, max_=False)
