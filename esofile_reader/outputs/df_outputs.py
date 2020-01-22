import pandas as pd
import numpy as np
from datetime import datetime
from typing import Sequence, List, Dict, Union
from esofile_reader.constants import *
from esofile_reader.outputs.base_outputs import BaseOutputs
from esofile_reader.processing.interval_processor import parse_result_dt
from esofile_reader.utils.utils import id_gen
from esofile_reader.utils.mini_classes import Variable


def _merge_peak_outputs(timestamp_df, values_df):
    """ Group 'value' and 'timestamp' columns to be adjacent for each id. """
    df = pd.concat({TIMESTAMP_COLUMN: timestamp_df, VALUE_COLUMN: values_df}, axis=1)
    df.columns.set_names(names="data", level=0, inplace=True)

    # move data index to lowest level
    names = list(df.columns.names)
    names.append(names.pop(0))
    df.columns = df.columns.reorder_levels(names)

    # create order to group value and timestamp
    length = len(df.columns)
    order = list(range(1, length, 2)) + list(range(0, length, 2))
    levels = [df.columns.get_level_values(i) for i in range(df.columns.nlevels)]
    levels.insert(0, pd.Index(order))

    # sort DataFrame by order column, order name must be specified
    names = ["order", *df.columns.names]
    df.columns = pd.MultiIndex.from_arrays(levels, names=names)
    df.sort_values(by="order", inplace=True, axis=1)

    # drop order index
    df.columns = df.columns.droplevel("order")

    return df


def _local_peaks(df, val_ix=None, month_ix=None, day_ix=None,
                 hour_ix=None, end_min_ix=None):
    """ Return value and datetime of occurrence. """

    def get_timestamps(sr):
        def parse_vals(val):
            if val is not np.NaN:
                month = val[month_ix] if month_ix else None
                day = val[day_ix] if day_ix else None
                hour = val[hour_ix]
                end_min = val[end_min_ix]
                ts = parse_result_dt(date, month, day, hour, end_min)
                return ts
            else:
                return np.NaN

        date = sr.name
        sr = sr.apply(parse_vals)

        return sr

    vals = df.applymap(lambda x: x[val_ix] if x is not np.nan else np.nan)
    ixs = df.apply(get_timestamps, axis=1)

    df = _merge_peak_outputs(ixs, vals)

    return df


def create_peak_outputs(interval, df, max_=True):
    """ Create DataFrame for peak minimums. """

    max_indexes = {
        D: {"val_ix": 3, "hour_ix": 4, "end_min_ix": 5},
        M: {"val_ix": 4, "day_ix": 5, "hour_ix": 6, "end_min_ix": 7},
        A: {"val_ix": 5, "month_ix": 6, "day_ix": 7, "hour_ix": 8, "end_min_ix": 9},
        RP: {"val_ix": 5, "month_ix": 6, "day_ix": 7, "hour_ix": 8, "end_min_ix": 9}
    }
    min_indexes = {
        D: {"val_ix": 0, "hour_ix": 1, "end_min_ix": 2},
        M: {"val_ix": 0, "day_ix": 1, "hour_ix": 2, "end_min_ix": 3},
        A: {"val_ix": 0, "month_ix": 1, "day_ix": 2, "hour_ix": 3, "end_min_ix": 4},
        RP: {"val_ix": 0, "month_ix": 1, "day_ix": 2, "hour_ix": 3, "end_min_ix": 4},
    }
    indexes = max_indexes if max_ else min_indexes

    return _local_peaks(df, **indexes[interval])


def slicer(df, ids, start_date=None, end_date=None):
    """ Slice df using indeterminate range. """
    ids = ids if isinstance(ids, list) else [ids]
    cond = df.columns.get_level_values("id").isin(ids)
    try:
        if start_date and end_date:
            df = df.loc[start_date:end_date, cond]
        elif start_date:
            df = df.loc[start_date:, cond]
        elif end_date:
            df = df.loc[:end_date, cond]
        else:
            df = df.loc[:, cond]

    except KeyError:
        valid_ids = df.columns.get_level_values("id").intersection(ids)
        ids = [str(ids)] if not isinstance(ids, list) else [str(i) for i in ids]
        print(f"Cannot slice df using requested inputs:"
              f"ids: '{', '.join(ids)}', start date: '{start_date}', end date: "
              f"'{end_date}'.\nTrying to use ids: '{valid_ids}' with all rows.")

        if valid_ids.empty:
            raise KeyError("Any of given ids is not included!")

        df = df.loc[:, valid_ids]

    return df.copy()


class DFOutputs(BaseOutputs):
    def __init__(self):
        self.tables = {}

    def set_data(self, interval: str, df: pd.DataFrame):
        self.tables[interval] = df

    def get_only_numeric_data(self, interval: str) -> pd.DataFrame:
        mi = self.tables[interval].columns
        cond = mi.get_level_values("id").isin([N_DAYS_COLUMN, DAY_COLUMN])
        return self.tables[interval].loc[:, ~cond]

    def get_available_intervals(self) -> List[str]:
        return list(self.tables.keys())

    def get_variables(self, interval: str) -> Dict[int, Variable]:

        def create_variable(sr):
            return sr["id"], Variable(sr["interval"], sr["key"], sr["variable"], sr["units"])

        header_df = self.get_header_df(interval)
        var_df = header_df.apply(create_variable, axis=1, result_type="expand")
        var_df.set_index(0, inplace=True)

        return var_df.to_dict(orient="dict")[1]

    def get_variable_ids(self, interval: str) -> List[int]:
        mi = self.tables[interval].columns.get_level_values("id").tolist()
        return list(filter(lambda x: x not in [N_DAYS_COLUMN, DAY_COLUMN], mi))

    def get_all_variable_ids(self) -> List[int]:
        all_ids = []
        for interval in self.get_available_intervals():
            ids = self.get_variable_ids(interval)
            all_ids.extend(ids)
        return all_ids

    def get_header_df(self, interval: str) -> pd.DataFrame:
        df = self.get_only_numeric_data(interval)
        return df.columns.to_frame(index=False)

    def get_all_header_dfs(self) -> pd.DataFrame:
        frames = []
        for interval in self.get_available_intervals():
            frames.append(self.get_header_df(interval))
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

    def remove_variables(self, interval: str, ids: Sequence[int]) -> None:
        try:
            self.tables[interval].drop(columns=ids, inplace=True, level="id")
        except KeyError:
            print(f"Cannot remove ids: {', '.join([str(id_) for id_ in ids])}")
            raise KeyError

    def get_special_column(self, name: str, interval: str, start_date: datetime = None,
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
        return self.get_special_column(N_DAYS_COLUMN, interval, start_date, end_date)

    def get_days_of_week(self, interval: str, start_date: datetime = None,
                         end_date: datetime = None) -> pd.Series:
        return self.get_special_column(DAY_COLUMN, interval, start_date, end_date)

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

        df = _merge_peak_outputs(ixs, vals)
        df = df.iloc[[0]]  # report only first occurrence

        return df

    def get_global_max_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        return self._global_peak(interval, ids, start_date, end_date)

    def get_global_min_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        return self._global_peak(interval, ids, start_date, end_date, max_=False)
