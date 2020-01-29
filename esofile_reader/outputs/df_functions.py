import numpy as np
import pandas as pd

from esofile_reader.constants import *
from esofile_reader.processing.interval_processor import parse_result_dt


def merge_peak_outputs(timestamp_df: pd.DataFrame, values_df: pd.DataFrame) -> pd.DataFrame:
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


def _local_peaks(df: pd.DataFrame, val_ix: int = None, month_ix: int = None, day_ix: int = None,
                 hour_ix: int = None, end_min_ix: int = None):
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

    df = merge_peak_outputs(ixs, vals)

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

    all_ids = df.columns.get_level_values("id")
    if not all(map(lambda x: x in all_ids, ids)):
        raise KeyError(f"Cannot slice df, ids: '{', '.join([str(id_) for id_ in ids])}',"
                       f"\nids {[str(id_) for id_ in ids if id_ not in all_ids]}"
                       f" are not included.")

    cond = df.columns.get_level_values("id").isin(ids)

    if start_date and end_date:
        df = df.loc[start_date:end_date, cond]
    elif start_date:
        df = df.loc[start_date:, cond]
    elif end_date:
        df = df.loc[:end_date, cond]
    else:
        df = df.loc[:, cond]

    return df.copy()


def df_dt_slicer(df, start_date, end_date):
    """ Slice df 'vertically'. """
    if start_date and end_date:
        df = df.loc[start_date:end_date, :]
    elif start_date:
        df = df.loc[start_date:, ]
    elif end_date:
        df = df.loc[:end_date, :]

    return df


def sr_dt_slicer(sr, start_date, end_date):
    """ Slice series. """
    if start_date and end_date:
        df = sr.loc[start_date:end_date]
    elif start_date:
        df = sr.loc[start_date:]
    elif end_date:
        df = sr.loc[:end_date]

    return sr
