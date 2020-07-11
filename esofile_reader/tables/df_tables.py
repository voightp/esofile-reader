from datetime import datetime
from typing import Sequence, List, Dict, Optional, Union

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.logger import logger
from esofile_reader.mini_classes import SimpleVariable, Variable
from esofile_reader.tables.base_tables import BaseTables
from esofile_reader.tables.df_functions import merge_peak_outputs, slicer, sr_dt_slicer


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

    def keys(self):
        return self._tables.keys()

    def values(self):
        return self._tables.values()

    def items(self):
        return self._tables.items()

    def is_simple(self, table: str) -> bool:
        return len(self.get_levels(table)) == 4

    def get_levels(self, table: str) -> List[str]:
        return self.tables[table].columns.names

    def get_table_names(self) -> List[str]:
        return list(self.tables.keys())

    def get_datetime_index(self, table: str) -> Optional[pd.DatetimeIndex]:
        index = self.tables[table].index
        if isinstance(index, pd.DatetimeIndex):
            return index

    def get_variables_dct(self, table: str) -> Dict[int, Union[Variable, SimpleVariable]]:
        def create_variable(sr):
            return (
                sr[ID_LEVEL],
                Variable(sr[TABLE_LEVEL], sr[KEY_LEVEL], sr[TYPE_LEVEL], sr[UNITS_LEVEL]),
            )

        def create_simple_variable(sr):
            return (
                sr[ID_LEVEL],
                SimpleVariable(sr[TABLE_LEVEL], sr[KEY_LEVEL], sr[UNITS_LEVEL]),
            )

        header_df = self.get_variables_df(table)
        func = create_simple_variable if self.is_simple(table) else create_variable
        var_df = header_df.apply(func, axis=1, result_type="expand")
        var_df.set_index(0, inplace=True)

        return var_df.to_dict(orient="dict")[1]

    def get_all_variables_dct(self) -> Dict[str, Dict[int, Union[Variable, SimpleVariable]]]:
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

    def _validate(
            self, table: str, variable: Union[Variable, SimpleVariable], array: Sequence
    ) -> bool:
        df_length = len(self.tables[table].index)
        valid = len(array) == df_length
        if not valid:
            logger.warning(
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
            logger.warning(
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
                f"are not included."
            )

        self.tables[table].drop(columns=ids, inplace=True, level=ID_LEVEL)

    def get_special_column(
            self,
            table: str,
            name: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ) -> pd.Series:
        if name not in self.tables[table].columns.get_level_values(KEY_LEVEL):
            raise KeyError(f"'{name}' column is not available " f"on the given data set.")
        if self.is_simple(table):
            v = (SPECIAL, table, name, "")
        else:
            v = (SPECIAL, table, name, "", "")
        col = sr_dt_slicer(self.tables[table].loc[:, v], start_date, end_date)
        if isinstance(col, pd.DataFrame):
            col = col.iloc[:, 0]
        return col

    def get_numeric_table(self, table: str) -> pd.DataFrame:
        mi = self.tables[table].columns
        cond = mi.get_level_values(ID_LEVEL) != SPECIAL
        return self.tables[table].loc[:, cond].copy()

    def get_results(
            self,
            table: str,
            ids: Sequence[int],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            include_day: bool = False,
    ) -> pd.DataFrame:
        df = slicer(self.tables[table], ids, start_date=start_date, end_date=end_date)
        df = df.copy()
        if include_day:
            try:
                days_sr = self.get_special_column(table, DAY_COLUMN, start_date, end_date)
                df[DAY_COLUMN] = days_sr
                df.set_index(DAY_COLUMN, append=True, inplace=True)
            except KeyError:
                try:
                    df[DAY_COLUMN] = df.index.strftime("%A")
                    df.set_index(DAY_COLUMN, append=True, inplace=True)
                except AttributeError:
                    pass
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
