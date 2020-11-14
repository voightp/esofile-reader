import logging
from copy import copy
from datetime import datetime
from typing import Sequence, List, Dict, Optional, Union

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import SimpleVariable, Variable, VariableType
from esofile_reader.abstractions.base_tables import BaseTables
from esofile_reader.df.df_functions import (
    merge_peak_outputs,
    slice_df,
    slice_series_by_datetime_index,
)


class DFTables(BaseTables):
    """
    The results are stored in a dictionary using string table identifiers
    as keys and pandas.DataFrame classes as values.

    Attributes
    ----------
    tables : dict
        A dictionary to store result pd.DataFrames

    Notes
    -----
    A structure for data bins is as follows:

    tables = {
        TS : pd.DataFrame,
        H : pd.DataFrame,
        D : pd.DataFrame,
        M : pd.DataFrame,
        A : pd.DataFrame,
        RP : pd.DataFrame,
        range: pd.DataFrame
    }

    DataFrame can have 6 levels for complete variable:

        id                         54898                  54902    \
        table                   daily                  daily
        key              GROUND:CORRIDOR            GROUND:FLAT
        type        Air Changes per Hour   Air Changes per Hour
        units                        ach                    ach
        timestamp
        2002-01-01              0.359897               0.112835
        2002-01-02              0.505683               0.148829
        2002-01-03              0.869096               0.217162
        2002-01-04              0.671173               0.276128
        2002-01-05              0.000000               0.000000
        ...                          ...                    ...

    or 5 levels for 'simple' variable

        id                         54801                  54802    \
        table                   daily                  daily
        type             Air Temperature   Air Changes per Hour
        units                          C                    ach
        timestamp
        2002-01-01                 24.01               0.112835
        2002-01-02                 24.20               0.148829
        2002-01-03                 25.30               0.217162
        2002-01-04                 26.70               0.276128
        2002-01-05                 31.10               0.000000
        ...                          ...                    ...


    There can be some special columns with special getter:
        get_special_column(table, name)

    """

    def __init__(self):
        self._tables = {}

    @property
    def tables(self):
        return self._tables

    @property
    def empty(self):
        return not bool(self._tables)

    def __setitem__(self, key: str, value: pd.DataFrame) -> None:
        # verify column names
        checklist = [SIMPLE_COLUMN_LEVELS, COLUMN_LEVELS, PEAK_COLUMN_LEVELS]
        if tuple(value.columns.names) not in checklist:
            raise TypeError(
                f"Cannot set table, column names must be [{', '.join(SIMPLE_COLUMN_LEVELS)}]"
                f" or {', '.join(COLUMN_LEVELS)}."
            )
        self._tables[key] = value

    def __getitem__(self, item: str):
        return self._tables[item]

    def __delitem__(self, key: str):
        del self._tables[key]

    def __eq__(self, other):
        def tables_match():
            for table_name in self.get_table_names():
                df = self.get_table(table_name)
                df.columns = df.columns.droplevel(ID_LEVEL)
                df = df.sort_values(df.columns.names, axis=1)
                other_df = other.get_table(table_name)
                other_df.columns = other_df.columns.droplevel(ID_LEVEL)
                other_df = other_df.sort_values(other_df.columns.names, axis=1)
                try:
                    pd.testing.assert_frame_equal(
                        df, other_df, check_freq=False, check_dtype=False
                    )
                except AssertionError:
                    return False
            return True

        table_names_match = self.tables.keys() == other.tables.keys()
        return table_names_match and tables_match()

    def __copy__(self):
        new_tables = self.__class__()
        for table, df in self.tables.items():
            new_tables[table] = copy(df)
        return new_tables

    def keys(self):
        return self._tables.keys()

    def values(self):
        return self._tables.values()

    def items(self):
        return self._tables.items()

    def extend(self, tables: Dict[str, pd.DataFrame]):
        for k, v in tables.items():
            self[k] = v

    def is_simple(self, table: str) -> bool:
        return len(self.get_levels(table)) == 4

    def is_index_datetime(self, table: str) -> bool:
        return isinstance(self.tables[table].index, pd.DatetimeIndex)

    def get_levels(self, table: str) -> List[str]:
        return self.tables[table].columns.names

    def get_table_names(self) -> List[str]:
        return list(self.tables.keys())

    def get_datetime_index(self, table: str) -> Optional[pd.DatetimeIndex]:
        index = self.tables[table].index
        if isinstance(index, pd.DatetimeIndex):
            return index

    def get_variables_count(self, table: str) -> int:
        return len(self.get_numeric_table(table).columns)

    def get_all_variables_count(self) -> int:
        return sum([self.get_variables_count(table) for table in self.get_table_names()])

    def get_variables_dct(self, table: str) -> Dict[int, VariableType]:
        cls = SimpleVariable if self.is_simple(table) else Variable
        header_dct = {}
        for row in self.get_variables_df(table).to_numpy():
            header_dct[row[0]] = cls(*row[1:])
        return header_dct

    def get_all_variables_dct(self) -> Dict[str, Dict[int, VariableType]]:
        all_variables = {}
        for table in self.get_table_names():
            all_variables[table] = self.get_variables_dct(table)
        return all_variables

    def get_variable_ids(self, table: str) -> List[int]:
        mi = self.tables[table].columns.get_level_values(ID_LEVEL).tolist()
        return list(filter(lambda x: x != SPECIAL, mi))

    def get_all_variable_ids(self) -> List[int]:
        all_ids = []
        for table in self.get_table_names():
            ids = self.get_variable_ids(table)
            all_ids.extend(ids)
        return all_ids

    def get_variables_df(self, table: str) -> pd.DataFrame:
        mi = self.tables[table].columns
        return mi[mi.get_level_values(ID_LEVEL) != SPECIAL].to_frame(index=False)

    def get_all_variables_df(self) -> pd.DataFrame:
        frames = []
        for table in self.get_table_names():
            frames.append(self.get_variables_df(table))
        return pd.concat(frames)

    def update_variable_name(
        self, table: str, id_: int, new_key: str, new_type: str = ""
    ) -> None:
        mi_df = self.tables[table].columns.to_frame(index=False)
        if self.is_simple(table):
            mi_df.loc[mi_df.id == id_, [KEY_LEVEL]] = [new_key]
        else:
            mi_df.loc[mi_df.id == id_, [KEY_LEVEL, TYPE_LEVEL]] = [new_key, new_type]
        self.tables[table].columns = pd.MultiIndex.from_frame(mi_df)

    def _validate(self, table: str, variable: VariableType, array: Sequence) -> bool:
        df_length = len(self.tables[table].index)
        valid = len(array) == df_length
        if not valid:
            logging.warning(
                f"New variable contains {len(array)} values, "
                f"df length is {df_length}! Variable '{variable}' cannot be added."
            )
        return valid

    def insert_column(
        self, variable: Union[SimpleVariable, Variable], array: Sequence
    ) -> Optional[int]:
        if self._validate(variable.table, variable, array):
            all_ids = self.get_all_variable_ids()
            # skip some ids as usually there's always few variables
            id_gen = incremental_id_gen(checklist=all_ids, start=100)
            id_ = next(id_gen)
            if isinstance(variable, Variable):
                table, key, type_, units = variable
                self.tables[table][id_, table, key, type_, units] = array
            else:
                table, key, units = variable
                self.tables[table][id_, table, key, units] = array
            return id_

    def insert_special_column(self, table: str, key: str, array: Sequence) -> None:
        if self.is_simple(table):
            v = (SPECIAL, table, key, "")
        else:
            v = (SPECIAL, table, key, "", "")
        if self._validate(table, v, array):
            self.tables[table].insert(0, v, array)

    def update_variable_values(self, table: str, id_: int, array: Sequence[float]):
        df_length = len(self.tables[table].index)
        valid = len(array) == df_length
        if not valid:
            logging.warning(
                f"Variable contains {len(array)} values, "
                f"df length is {df_length}! Variable cannot be updated."
            )
        else:
            cond = self.tables[table].columns.get_level_values(ID_LEVEL) == id_
            self.tables[table].loc[:, cond] = array

    def delete_variables(self, table: str, ids: Sequence[int]) -> None:
        all_ids = self.tables[table].columns.get_level_values(ID_LEVEL)
        if not all(map(lambda x: x in all_ids, ids)):
            raise KeyError(
                f"Cannot remove ids: '{', '.join([str(id_) for id_ in ids])}',"
                f"\nids {[str(id_) for id_ in ids if id_ not in all_ids]}"
                f" are not included."
            )
        self.tables[table].drop(columns=ids, inplace=True, level=ID_LEVEL)

    def get_special_column(
        self,
        table: str,
        name: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.Series:
        if self.is_simple(table):
            v = (SPECIAL, table, name, "")
        else:
            v = (SPECIAL, table, name, "", "")
        col = slice_series_by_datetime_index(self.tables[table].loc[:, v], start_date, end_date)
        if isinstance(col, pd.DataFrame):
            col = col.iloc[:, 0]
        return col

    def get_table(self, table: str):
        return self.tables[table].loc[:, :].copy()

    def get_special_table(self, table: str):
        mi = self.tables[table].columns
        cond = mi.get_level_values(ID_LEVEL) == SPECIAL
        return self.tables[table].loc[:, cond].copy()

    def get_numeric_table(self, table: str) -> pd.DataFrame:
        mi = self.tables[table].columns
        cond = mi.get_level_values(ID_LEVEL) != SPECIAL
        return self.tables[table].loc[:, cond].copy()

    def add_day_to_index(
        self,
        df: pd.DataFrame,
        table: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        day_column = None
        try:
            day_column = self.get_special_column(table, DAY_COLUMN, start_date, end_date)
        except KeyError:
            if isinstance(df.index, pd.DatetimeIndex):
                day_column = df.index.strftime("%A")
        if day_column is not None:
            df.index = pd.MultiIndex.from_arrays(
                [df.index, day_column], names=[df.index.name, DAY_COLUMN]
            )
        return df

    def get_results_df(
        self,
        table: str,
        ids: Sequence[int],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        include_day: bool = False,
    ) -> pd.DataFrame:
        df = slice_df(self.tables[table], ids, start_date=start_date, end_date=end_date)
        df = df.copy()
        if include_day and self.is_index_datetime(table):
            df = self.add_day_to_index(df, table, start_date, end_date)
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
        df = self.get_results_df(table, ids, start_date, end_date)

        vals = pd.DataFrame(df.max() if max_ else df.min()).T
        ixs = pd.DataFrame(df.idxmax() if max_ else df.idxmin()).T

        df = merge_peak_outputs(ixs, vals)
        df = df.iloc[[0]]  # report only first occurrence

        return df

    def get_global_max_results_df(
        self,
        table: str,
        ids: Sequence[int],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        return self._global_peak(table, ids, start_date, end_date)

    def get_global_min_results_df(
        self,
        table: str,
        ids: Sequence[int],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        return self._global_peak(table, ids, start_date, end_date, max_=False)
