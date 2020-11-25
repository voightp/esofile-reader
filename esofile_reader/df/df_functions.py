import logging
from datetime import datetime
from typing import Sequence, Optional, List

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.processing.eplus.esofile_time import combine_peak_result_datetime


def merge_peak_outputs(timestamp_df: pd.DataFrame, values_df: pd.DataFrame) -> pd.DataFrame:
    """ Group 'value' and 'timestamp' columns to be adjacent for each id. """
    df = pd.concat({TIMESTAMP_COLUMN: timestamp_df, VALUE_LEVEL: values_df}, axis=1)
    df.columns.set_names(names=DATA_LEVEL, level=0, inplace=True)

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


def _local_peaks(
    df: pd.DataFrame,
    val_ix: int = None,
    month_ix: int = None,
    day_ix: int = None,
    hour_ix: int = None,
    end_min_ix: int = None,
) -> pd.DataFrame:
    """ Return value and datetime of occurrence. """

    def get_timestamps(sr):
        def parse_vals(val):
            if isinstance(val, tuple):
                month = val[month_ix] if month_ix else None
                day = val[day_ix] if day_ix else None
                hour = val[hour_ix]
                end_min = val[end_min_ix]
                ts = combine_peak_result_datetime(date, month, day, hour, end_min)
                return ts
            else:
                return val

        date = sr.name
        sr = sr.apply(parse_vals)
        return sr

    vals = df.applymap(lambda x: x[val_ix] if isinstance(x, tuple) else x)
    ixs = df.apply(get_timestamps, axis=1)
    df = merge_peak_outputs(ixs, vals)
    return df


def create_peak_max_outputs(table: str, df: pd.DataFrame) -> pd.DataFrame:
    """ Create DataFrame for peak maximums. """
    max_indexes = {
        D: {"val_ix": 3, "hour_ix": 4, "end_min_ix": 5},
        M: {"val_ix": 4, "day_ix": 5, "hour_ix": 6, "end_min_ix": 7},
        A: {"val_ix": 5, "month_ix": 6, "day_ix": 7, "hour_ix": 8, "end_min_ix": 9},
        RP: {"val_ix": 5, "month_ix": 6, "day_ix": 7, "hour_ix": 8, "end_min_ix": 9},
    }
    return _local_peaks(df, **max_indexes[table])


def create_peak_min_outputs(table: str, df: pd.DataFrame) -> pd.DataFrame:
    """ Create DataFrame for peak minimums. """
    min_indexes = {
        D: {"val_ix": 0, "hour_ix": 1, "end_min_ix": 2},
        M: {"val_ix": 0, "day_ix": 1, "hour_ix": 2, "end_min_ix": 3},
        A: {"val_ix": 0, "month_ix": 1, "day_ix": 2, "hour_ix": 3, "end_min_ix": 4},
        RP: {"val_ix": 0, "month_ix": 1, "day_ix": 2, "hour_ix": 3, "end_min_ix": 4},
    }
    return _local_peaks(df, **min_indexes[table])


def sort_by_ids(df: pd.DataFrame, ids: List[int]):
    """ Return filtered DataFrame ordered as given ids. """
    # get iloc position for given list of ids, slices are required for non filtered levels
    all_ids = df.columns.get_level_values(ID_LEVEL).to_series()
    all_ids.reset_index(drop=True, inplace=True)
    indexes = []
    for id_ in ids:
        ix = all_ids[all_ids == id_].index
        if not ix.empty:
            indexes.extend(ix)
        else:
            logging.warning(f"Id {id_} is not included in given DataFrame.")
    return df.iloc[:, indexes]


def slice_df(
    df: pd.DataFrame,
    ids: Sequence[int],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """ Slice df using indeterminate range. """
    ids = ids if isinstance(ids, list) else [ids]
    all_ids = df.columns.get_level_values(ID_LEVEL)
    if not all(map(lambda x: x in all_ids, ids)):
        raise KeyError(
            f"Cannot slice df, ids: '{', '.join([str(id_) for id_ in ids])}',"
            f"\nids {[str(id_) for id_ in ids if id_ not in all_ids]}"
            f" are not included."
        )
    cond = df.columns.get_level_values(ID_LEVEL).isin(ids)
    if start_date and end_date:
        df = df.loc[start_date:end_date, cond]
    elif start_date:
        df = df.loc[start_date:, cond]
    elif end_date:
        df = df.loc[:end_date, cond]
    else:
        df = df.loc[:, cond]
    return sort_by_ids(df, ids)


def slice_df_by_datetime_index(
    df: pd.DataFrame, start_date: Optional[datetime], end_date: Optional[datetime]
) -> pd.DataFrame:
    """ Slice df by index. """
    if start_date and end_date:
        df = df.loc[start_date:end_date, :]
    elif start_date:
        df = df.loc[start_date:, :]
    elif end_date:
        df = df.loc[:end_date, :]
    return df


def slice_series_by_datetime_index(
    sr: pd.Series, start_date: Optional[datetime], end_date: Optional[datetime]
) -> pd.Series:
    """ Slice series by index. """
    if start_date and end_date:
        sr = sr.loc[start_date:end_date]
    elif start_date:
        sr = sr.loc[start_date:]
    elif end_date:
        sr = sr.loc[:end_date]
    return sr
