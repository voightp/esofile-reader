import pandas as pd
import numpy as np
from esofile_reader.processing.interval_processor import parse_result_dt
from esofile_reader.constants import *


class PeaksNotIncluded(Exception):
    """ Exception is raised when EsoFile has been processed without peaks. """
    # PeaksNotIncluded("Peak values are not included, it's required to "
    #                  "add kwarg 'ignore_peaks=False' when processing the file."
    #                  "\nNote that peak values are only applicable for"
    #                  "raw Eso files.")
    pass


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
    df = df.applymap(lambda x: x[val_ix] if x is not np.nan else np.nan)

    def get_timestamps(sr):
        def parse_vals(val):
            ts = parse_result_dt(date, val, month_ix, day_ix, hour_ix, end_min_ix)
            return ts

        date = sr.name
        sr = sr.apply(parse_vals)

        return sr

    vals = df.applymap(lambda x: x[val_ix])
    ixs = df.apply(get_timestamps, axis=1)

    df = pd.concat({"timestamp": ixs, "value": vals}, axis=1)
    df.columns = df.columns.swaplevel(0, 1)
    df = _sort_peak_outputs(df)

    return df


def get_mins(data, interval):
    """ Create DataFrame for peak minimums. """
    df = PeakOutputs(data)
    indexes = {
        D: {"val_ix": 0, "hour_ix": 1, "end_min_ix": 2},
        M: {"val_ix": 0, "day_ix": 1, "hour_ix": 2, "end_min_ix": 3},
        A: {"val_ix": 0, "month_ix": 1, "day_ix": 2, "hour_ix": 3, "end_min_ix": 4},
        RP: {"val_ix": 0, "month_ix": 1, "day_ix": 2, "hour_ix": 3, "end_min_ix": 4},
    }
    return _local_peaks(df, **indexes[interval])


def get_maxs(data, interval):
    """ Create DataFrame for peak minimums. """
    df = PeakOutputs(data)
    indexes = {
        D: {"val_ix": 3, "hour_ix": 4, "end_min_ix": 5},
        M: {"val_ix": 4, "day_ix": 5, "hour_ix": 6, "end_min_ix": 7},
        A: {"val_ix": 5, "month_ix": 6, "day_ix": 7, "hour_ix": 8, "end_min_ix": 9},
        RP: {"val_ix": 5, "month_ix": 6, "day_ix": 7, "hour_ix": 8, "end_min_ix": 9}
    }
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
        print("Cannot slice df using requested inputs:"
              "ids: '{}', start date: '{}', end date: '{}'.\n"
              "Trying to use ids: '{}' with all rows. ".format(", ".join(ids),
                                                               start_date, end_date,
                                                               valid_ids))
        if valid_ids.empty:
            raise KeyError("Any of given ids is not included!")

        return df.loc[:, valid_ids]


class PeakOutputs(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super(PeakOutputs, self).__init__(*args, **kwargs)


class Outputs(pd.DataFrame):
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

    def get_standard_results_only(self, transposed=False):
        """ Get df with only 'standard' outputs and 'num days'. """
        df = self.copy()

        for s in ["num days", "days of week"]:
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
            if self.columns == ["num days"]:
                self.drop(columns="num days", inplace=True)

    def get_results(self, ids, start_date=None, end_date=None):
        """ Find standard result. """
        df = slicer(self, ids, start_date=start_date, end_date=end_date)

        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        return df.copy()

    def get_number_of_days(self, start_date=None, end_date=None):
        """ Return 'number of days' column. """
        if "num days" not in self.columns:
            raise AttributeError("'number of days' column is not available"
                                 "on the given data set.")
        return slicer(self, "num days", start_date, end_date)

    def get_days_of_week(self, start_date=None, end_date=None):
        """ Return 'days of week' column. """
        if "days of week" not in self.columns:
            raise AttributeError("'days of week' column is not available"
                                 "on the given data set.")
        return slicer(self, "days of week", start_date, end_date)


class Hourly(Outputs):
    """
    Pandas.DataFrame like class to hold EnergyPlus results
    for Hourly interval.

    Local peak and timestep results are nor applicable for
    Hourly interval.

    Parameters
    ----------
    data : dict like objects
        Dict of list-like objects of processed EnergyPlus results.
    **kwargs
        Key word arguments which are passed to
        super() pandas.DataFrame class.

    """

    def __init__(self, data, **kwargs):
        super(Hourly, self).__init__(data, **kwargs)

    def global_max(self, ids, start_date=None, end_date=None):
        """ Return an interval maximum value and date of occurrence. """
        return self._global_peak(ids, start_date, end_date, val_ix=0)

    def global_min(self, ids, start_date=None, end_date=None):
        """ Return an interval minimum value and date of occurrence. """
        return self._global_peak(ids, start_date, end_date, val_ix=0, max_=False)

    def local_maxs(self, *args, **kwargs):
        """ Local maximum values are not applicable for Hourly interval. """
        pass

    def local_mins(self, *args, **kwargs):
        """ Local minimum values are not applicable for Hourly interval. """
        pass

    def timestep_min(self, *args, **kwargs):
        """ Timestep maximum value is not applicable for Hourly interval. """
        pass

    def timestep_max(self, *args, **kwargs):
        """ Timestep maximum value is not applicable for Hourly interval. """
        pass

    def get_number_of_days(self, *args, **kwargs):
        """ Number of days is not available for Hourly interval. """
        pass


class Timestep(Hourly):
    """
    Pandas.DataFrame like class to hold EnergyPlus results
    for Timestep interval.

    Local peak results are nor applicable for
    Hourly interval.

    Parameters
    ----------
    data : dict like objects
        Dict of list-like objects of processed EnergyPlus results.
    **kwargs
        Key word arguments which are passed to super() pandas.DataFrame class.

    """

    def __init__(self, data, **kwargs):
        super(Timestep, self).__init__(data, **kwargs)

    def get_n_steps(self):
        """ Get a number of timesteps in an hour (this is unique for ts interval). """
        timestamps = self.index
        timedelta = timestamps[1] - timestamps[0]
        return 3600 / timedelta.seconds

    def timestep_min(self, ids, start_date, end_date):
        """ Timestep minimum value is the same as global minimum for Timestep interval. """
        return self._global_peak(ids, start_date, end_date, val_ix=0, max_=False)

    def timestep_max(self, ids, start_date, end_date):
        """ Timestep maximum value is the same as global maximum for Timestep interval. """
        return self._global_peak(ids, start_date, end_date, val_ix=0)
