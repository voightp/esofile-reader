import pandas as pd
import numpy as np
from esofile_reader.processing.interval_processor import parse_result_dt
from esofile_reader.constants import *


def _sort_peak_outputs(df):
    """ Group 'value' and 'timestamp' columns to be adjacent for each id. """
    length = len(df.columns)
    order = list(range(1, length, 2)) + list(range(0, length, 2))
    levels = [df.columns.get_level_values(i) for i in range(df.columns.nlevels)]
    levels.insert(0, pd.Index(order))

    df.columns = pd.MultiIndex.from_arrays(levels, names=["order", "id", "line"])
    df.sort_values(by="order", inplace=True, axis=1)
    df.columns = df.columns.droplevel(0)

    return df


def _local_peaks(df, val_ix=None, month_ix=None, day_ix=None,
                 hour_ix=None, end_min_ix=None):
    """ Return value and datetime of occurrence. """

    def get_timestamps(sr):
        def parse_vals(val):
            if val is not np.NaN:
                ts = parse_result_dt(date, val, month_ix, day_ix, hour_ix, end_min_ix)
                return ts
            else:
                return np.NaN

        date = sr.name
        sr = sr.apply(parse_vals)

        return sr

    vals = df.applymap(lambda x: x[val_ix] if x is not np.nan else np.nan)
    ixs = df.apply(get_timestamps, axis=1)

    df = pd.concat({"timestamp": ixs, "value": vals}, axis=1)
    df.columns = df.columns.swaplevel(0, 1)
    df = _sort_peak_outputs(df)

    return df


def create_peak_df(data, interval, index, max_=True):
    """ Create DataFrame for peak minimums. """
    df = PeakOutputs(data)
    df.index = index

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
    try:
        if start_date and end_date:
            return df.loc[start_date:end_date, ids]
        elif start_date:
            return df.loc[start_date:, ids]
        elif end_date:
            return df.loc[:end_date, ids]
        else:
            return df.loc[:, ids]

    except KeyError:
        valid_ids = df.columns.intersection(ids)
        ids = [str(ids)] if not isinstance(ids, list) else [str(i) for i in ids]
        print(f"Cannot slice df using requested inputs:"
              f"ids: '{', '.join(ids)}', start date: '{start_date}', end date: "
              f"'{end_date}'.\nTrying to use ids: '{valid_ids}' with all rows.")

        if valid_ids.empty:
            raise KeyError("Any of given ids is not included!")

        return df.loc[:, valid_ids]


class BaseOutputs(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super(BaseOutputs, self).__init__(*args, **kwargs)

    def get_results(self, ids, start_date=None, end_date=None):
        """ Find standard result. """
        df = slicer(self, ids, start_date=start_date, end_date=end_date)

        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        return df.copy()


class PeakOutputs(BaseOutputs):
    def __init__(self, *args, **kwargs):
        super(PeakOutputs, self).__init__(*args, **kwargs)


class Outputs(BaseOutputs):
    """
    A parent class to define all methods required for extracting
    specific E+ results.

    Only method shared by all subclasses is to find standard results.
    Maximum and minimum methods are in place to avoid non implementing
    all the required results for all subclasses.

    Local, global and timestep peak methods are base methods to
    pick up requested values from the lowest level result tuple.

    Parameters
    ----------
    data : dict like objects
        Dict of list-like objects of processed EnergyPlus results.
    **kwargs
        Key word arguments which are passed to super() pandas.DataFrame class.

    """

    def __init__(self, data, **kwargs):
        super(Outputs, self).__init__(data, **kwargs)

    def get_all_results(self, transposed=False, drop_special=True):
        """ Get df with only 'standard' outputs and 'num days'. """
        df = self.copy()

        if drop_special:
            for s in ["n days", "day"]:
                try:
                    df.drop(s, axis=1, inplace=True)
                except KeyError:
                    pass

        if transposed:
            df = df.T
            df.index = df.index.set_names(["id"])  # reapply the name as it gets lost when combining with 'num_days'

        return df

    def _validate(self, data):
        """ Validate if the line has required format. """
        length = len(data)
        df_length = len(self.index)
        valid = length == df_length

        if not valid:
            print(f"New variable contains {length} values, df length is {df_length}!"
                  "\n\t Variable will not be added to the file.")

        return valid

    def add_column(self, id_, array):
        """ Add output line. """
        is_valid = self._validate(array)

        if is_valid:
            self[id_] = array

        return is_valid

    def remove_columns(self, ids):
        """ Remove output line. """
        if not isinstance(ids, list):
            ids = [ids]
        try:
            self.drop(columns=ids, inplace=True)
        except KeyError:
            strids = ", ".join(ids)
            print(f"Cannot remove ids: {strids}")

        if len(self.columns) == 1:
            # df can only include one of identifiers below
            for s in ["n days", "day"]:
                try:
                    self.drop(s, axis=1, inplace=True)
                except KeyError:
                    pass

    def get_number_of_days(self, start_date=None, end_date=None):
        """ Return 'number of days' column. """
        if "n days" not in self.columns:
            raise AttributeError("'n days' column is not available"
                                 "on the given data set.")
        return slicer(self, "n days", start_date, end_date)

    def get_days_of_week(self, start_date=None, end_date=None):
        """ Return 'days of week' column. """
        if "days of week" not in self.columns:
            raise AttributeError("'day' column is not available"
                                 "on the given data set.")
        return slicer(self, "day", start_date, end_date)
