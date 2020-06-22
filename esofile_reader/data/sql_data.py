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
    sort_by_ids
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

    def _get_results_table(self, table: str) -> Table:
        name = get_table_name(self.id_, "results", table)
        return self._get_table(name, "numeric")

    def _get_datetime_table(self, table: str) -> Table:
        name = get_table_name(self.id_, "index", table)
        return self._get_table(name, "datetime")

    def _get_special_table(self, table: str, key: str) -> Table:
        name = get_table_name(self.id_, key, table)
        return self._get_table(name, "special")

    def is_simple(self, table: str) -> bool:
        return len(self.get_levels(table)) == 4

    def get_levels(self, table: str) -> List[str]:
        sql_table = self._get_results_table(table)
        levels = [c.name for c in sql_table.columns if c.name != STR_VALUES]
        return levels

    def get_table_names(self) -> List[str]:
        names = self._get_table_names("numeric")
        return [parse_table_name(r)[2] for r in names]

    def get_datetime_index(self, table: str) -> pd.DatetimeIndex:
        sql_table = self._get_datetime_table(table)
        with self.storage.engine.connect() as conn:
            res = conn.execute(sql_table.select()).fetchall()
            datetime_index = pd.DatetimeIndex([r[0] for r in res], name=TIMESTAMP_COLUMN)
        return datetime_index

    def get_variables_dct(self, table: str) -> Dict[int, Union[SimpleVariable, Variable]]:
        variables_dct = {}
        variables_df = self.get_variables_df(table)
        v = SimpleVariable if self.is_simple(table) else Variable
        for row in variables_df.to_numpy():
            variables_dct[row[0]] = v(*row[1:])
        return variables_dct

    def get_all_variables_dct(self) -> Dict[str, Dict[int, Variable]]:
        all_variables_dct = {}
        for table in self.get_table_names():
            all_variables_dct[table] = self.get_variables_dct(table)
        return all_variables_dct

    def get_variable_ids(self, table: str) -> List[int]:
        table = self._get_results_table(table)
        with self.storage.engine.connect() as conn:
            res = conn.execute(select([table.c.id]))
            ids = [row[0] for row in res]
        return ids

    def get_all_variable_ids(self) -> List[int]:
        all_ids = []
        for table in self.get_table_names():
            all_ids.extend(self.get_variable_ids(table))
        return all_ids

    def get_variables_df(self, table: str) -> pd.DataFrame:
        sql_table = self._get_results_table(table)
        if self.is_simple(table):
            s = [sql_table.c.id, sql_table.c.table, sql_table.c.key, sql_table.c.units]
            columns = SIMPLE_COLUMN_LEVELS
        else:
            s = [sql_table.c.id, sql_table.c.table, sql_table.c.key, sql_table.c.type,
                 sql_table.c.units]
            columns = COLUMN_LEVELS
        with self.storage.engine.connect() as conn:
            res = conn.execute(select(s))
            df = pd.DataFrame(res, columns=columns)
        return df

    def get_all_variables_df(self) -> pd.DataFrame:
        frames = []
        for table in self.get_table_names():
            frames.append(self.get_variables_df(table))
        return pd.concat(frames)

    def update_variable_name(
            self, table: str, id_: int, new_key: str, new_type: str = ""
    ) -> None:
        sql_table = self._get_results_table(table)
        with self.storage.engine.connect() as conn:
            kwargs = {"key": new_key} if self.is_simple(table) \
                else {"key": new_key, "type": new_type}
            conn.execute(
                sql_table.update().where(sql_table.c.id == id_).values(**kwargs)
            )

    def _validate(self, table: str, array: Sequence[float]) -> bool:
        sql_table = self._get_results_table(table)
        with self.storage.engine.connect() as conn:
            res = conn.execute(select([sql_table.c.str_values])).scalar()
        # number of elements in array
        n = res.count(self.storage.SEPARATOR) + 1
        return len(array) == n

    def insert_column(self, variable: Variable, array: Sequence[float]) -> Optional[int]:
        if self._validate(variable.table, array):
            table = self._get_results_table(variable.table)
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

    def update_variable_values(self, table: str, id_: int, array: Sequence[float]):
        if self._validate(table, array):
            table = self._get_results_table(table)
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

    def insert_special_column(self, table: str, key: str, array: Sequence) -> None:
        ft = self.storage.file_table
        if self._validate(table, array):
            column_type = Integer if all(map(lambda x: isinstance(x, int), array)) else String
            special_table = create_special_table(
                self.storage.metadata, self.id_, table, key, column_type
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
                f"Cannot add special variable '{key} into table {table}'. "
                f"Number of elements '({len(array)})' does not match!"
            )

    def delete_variables(self, table: str, ids: List[int]) -> None:
        table = self._get_results_table(table)
        with self.storage.engine.connect() as conn:
            conn.execute(table.delete().where(table.c.id.in_(ids)))

    def get_special_column(
            self,
            table: str,
            key: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> pd.Series:
        sql_table = self._get_special_table(table, key)
        with self.storage.engine.connect() as conn:
            res = conn.execute(sql_table.select()).fetchall()
            if self.is_simple(table):
                name = (SPECIAL, table, key, "")
            else:
                name = (SPECIAL, table, key, "", "")
            sr = pd.Series([r[0] for r in res], name=name)
        index = self.get_datetime_index(table)
        if index is not None:
            sr.index = index
        return sr_dt_slicer(sr, start_date, end_date)

    def get_results(
            self,
            table: str,
            ids: Sequence[int],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            include_day: bool = False,
    ) -> pd.DataFrame:
        ids = ids if isinstance(ids, list) else [ids]
        columns = SIMPLE_COLUMN_LEVELS if self.is_simple(table) else COLUMN_LEVELS
        sql_table = self._get_results_table(table)
        with self.storage.engine.connect() as conn:
            res = conn.execute(sql_table.select().where(sql_table.c.id.in_(ids)))
            df = pd.DataFrame(res, columns=[*columns, "values"])
            if df.empty:
                raise KeyError(
                    f"Cannot find results, any of given ids: "
                    f"'{', '.join([str(id_) for id_ in ids])}' "
                    f"is not included."
                )

            df.set_index(list(columns), inplace=True)
            df = destringify_values(df)
        if table == RANGE:
            # create default 'range' index
            df.index.rename(RANGE, inplace=True)
        else:
            df.index = self.get_datetime_index(table)
            if include_day:
                try:
                    day_sr = self.get_special_column(table, DAY_COLUMN, start_date, end_date)
                    df.insert(0, DAY_COLUMN, day_sr)
                    df.set_index(DAY_COLUMN, append=True, inplace=True)
                except KeyError:
                    df[DAY_COLUMN] = df.index.strftime("%A")
                    df.set_index(DAY_COLUMN, append=True, inplace=True)
        df = df_dt_slicer(df, start_date, end_date)
        return sort_by_ids(df, ids)

    def get_numeric_table(self, table: str) -> pd.DataFrame:
        ids = self.get_variable_ids(table)
        df = self.get_results(table, ids)
        return df

    def _global_peak(
            self,
            table: str,
            ids: Sequence[int],
            start_date: datetime,
            end_date: datetime,
            max_: bool = True,
    ) -> pd.DataFrame:
        """ Return maximum or minimum value and datetime of occurrence. """
        df = self.get_results(table, ids, start_date, end_date)

        vals = pd.DataFrame(df.max() if max_ else df.min()).T
        ixs = pd.DataFrame(df.idxmax() if max_ else df.idxmin()).T

        df = merge_peak_outputs(ixs, vals)
        df = df.iloc[[0]]  # report only first occurrence

        return df

    def get_global_max_results(
            self,
            table: str,
            ids: Sequence[int],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        return self._global_peak(table, ids, start_date, end_date)

    def get_global_min_results(
            self,
            table: str,
            ids: Sequence[int],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        return self._global_peak(table, ids, start_date, end_date, max_=False)
