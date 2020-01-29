import pandas as pd
from datetime import datetime
from typing import Sequence, List, Dict
from esofile_reader.constants import *
from esofile_reader.outputs.base_outputs import BaseData
from esofile_reader.outputs.df_outputs_functions import merge_peak_outputs, slicer

from esofile_reader.utils.utils import id_gen
from esofile_reader.utils.mini_classes import Variable


class DFData(BaseData):
    """
    The results are stored in a dictionary using string interval identifiers
    as keys and pandas.DataFrame classes as values.

    Attributes
    ----------
    tables : dict
        A dictionary to store result pd.DataFrames

    Notes
    -----
    A structure for data bins is as follows:

    outputs = {
        TS : pd.DataFrame,
        H : pd.DataFrame,
        D : pd.DataFrame,
        M : pd.DataFrame,
        A : pd.DataFrame,
        RP : pd.DataFrame,
    }

    pd.DataFrame:

        id                         54898                  54902    \
        interval                   daily                  daily
        key              GROUND:CORRIDOR            GROUND:FLAT
        variable    Air Changes per Hour   Air Changes per Hour
        units                        ach                    ach
        timestamp
        2002-01-01              0.359897               0.112835
        2002-01-02              0.505683               0.148829
        2002-01-03              0.869096               0.217162
        2002-01-04              0.671173               0.276128
        2002-01-05              0.000000               0.000000
        ...                          ...                    ...


    There can be some special columns with predefined getters:
        get_number_of_days(interval)
        get_days_of_week(interval)

    """

    def __init__(self):
        self.tables = {}

    def set_data(self, interval: str, df: pd.DataFrame):
        self.tables[interval] = df

    def get_available_intervals(self) -> List[str]:
        return list(self.tables.keys())

    def get_datetime_index(self, interval: str) -> pd.DatetimeIndex:
        """ Store table in database. """
        index = self.tables[interval].index
        if isinstance(index, pd.DatetimeIndex):
            return index

    def get_variables_dct(self, interval: str) -> Dict[int, Variable]:

        def create_variable(sr):
            return sr["id"], Variable(sr["interval"], sr["key"], sr["variable"], sr["units"])

        header_df = self.get_variables_df(interval)
        var_df = header_df.apply(create_variable, axis=1, result_type="expand")
        var_df.set_index(0, inplace=True)

        return var_df.to_dict(orient="dict")[1]

    def get_all_variables_dct(self) -> Dict[str, Dict[int, Variable]]:
        all_variables = {}
        for interval in self.get_available_intervals():
            ids = self.get_variables_dct(interval)
            all_variables[interval] = ids
        return all_variables

    def get_variable_ids(self, interval: str) -> List[int]:
        mi = self.tables[interval].columns.get_level_values("id").tolist()
        return list(filter(lambda x: x not in [N_DAYS_COLUMN, DAY_COLUMN], mi))

    def get_all_variable_ids(self) -> List[int]:
        all_ids = []
        for interval in self.get_available_intervals():
            ids = self.get_variable_ids(interval)
            all_ids.extend(ids)
        return all_ids

    def get_variables_df(self, interval: str) -> pd.DataFrame:
        df = self.get_all_results(interval)
        return df.columns.to_frame(index=False)

    def get_all_variables_df(self) -> pd.DataFrame:
        frames = []
        for interval in self.get_available_intervals():
            frames.append(self.get_variables_df(interval))
        return pd.concat(frames)

    def rename_variable(self, interval: str, id_, key_name, var_name) -> None:
        mi_df = self.tables[interval].columns.to_frame(index=False)
        mi_df.loc[mi_df.id == id_, ["key", "variable"]] = [key_name, var_name]
        self.tables[interval].columns = pd.MultiIndex.from_frame(mi_df)

    def add_variable(self, variable: Variable, array: Sequence) -> None:
        interval, key, variable, units = variable
        df_length = len(self.tables[interval].index)
        valid = len(array) == df_length

        if not valid:
            print(f"New variable contains {len(array)} values, df length is {df_length}!"
                  "\nVariable cannot be added.")
        else:
            all_ids = self.get_all_variable_ids()
            id_ = id_gen(all_ids)
            self.tables[interval][id_, interval, key, variable, units] = array

            return id_

    def update_variable(self, interval: str, id_: int, array: Sequence[float]):
        df_length = len(self.tables[interval].index)
        valid = len(array) == df_length

        if not valid:
            print(f"Variable contains {len(array)} values, df length is {df_length}!"
                  "\nVariable cannot be updated.")
        else:
            cond = self.tables[interval].columns.get_level_values("id") == id_
            self.tables[interval].loc[:, cond] = array

    def remove_variables(self, interval: str, ids: Sequence[int]) -> None:
        all_ids = self.tables[interval].columns.get_level_values("id")
        if not all(map(lambda x: x in all_ids, ids)):
            raise KeyError(f"Cannot remove ids: '{', '.join([str(id_) for id_ in ids])}',"
                           f"\nids {[str(id_) for id_ in ids if id_ not in all_ids]}"
                           f"are not included.")

        self.tables[interval].drop(columns=ids, inplace=True, level="id")

    def get_special_column(self, interval: str, name: str, start_date: datetime = None,
                           end_date: datetime = None) -> pd.Series:
        if name not in self.tables[interval].columns.get_level_values("id"):
            raise KeyError(f"'{name}' column is not available "
                           f"on the given data set.")

        col = slicer(self.tables[interval], name, start_date, end_date)

        if isinstance(col, pd.DataFrame):
            col = col.iloc[:, 0]

        return col

    def get_number_of_days(self, interval: str, start_date: datetime = None,
                           end_date: datetime = None) -> pd.Series:
        return self.get_special_column(interval, N_DAYS_COLUMN, start_date, end_date)

    def get_days_of_week(self, interval: str, start_date: datetime = None,
                         end_date: datetime = None) -> pd.Series:
        return self.get_special_column(interval, DAY_COLUMN, start_date, end_date)

    def get_all_results(self, interval: str) -> pd.DataFrame:
        mi = self.tables[interval].columns
        cond = mi.get_level_values("id").isin([N_DAYS_COLUMN, DAY_COLUMN])
        return self.tables[interval].loc[:, ~cond]

    def get_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                    end_date: datetime = None, include_day: bool = False) -> pd.DataFrame:
        df = slicer(self.tables[interval], ids, start_date=start_date, end_date=end_date)

        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        if include_day:
            try:
                days = self.get_days_of_week(interval, start_date, end_date)
                df[DAY_COLUMN] = days
                df.set_index(DAY_COLUMN, append=True, inplace=True)
            except KeyError:
                try:
                    df[DAY_COLUMN] = df.index.strftime("%A")
                    df.set_index(DAY_COLUMN, append=True, inplace=True)
                except AttributeError:
                    pass

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
