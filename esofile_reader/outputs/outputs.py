import pandas as pd
import numpy as np
from esofile_reader.processing.interval_processor import parse_result_dt


class PeaksNotIncluded(Exception):
    """ Exception is raised when EsoFile has been processed without peaks. """
    pass


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
    _min_peak = None
    _max_peak = None

    def __init__(self, data, **kwargs):
        super(Outputs, self).__init__(data, **kwargs)

    @staticmethod
    def fetch_outputs(df, val_ix):
        """ Extract results column from df. """

        frames = []
        tuples = list(map(lambda x: x == object, df.dtypes))

        df_a = df.loc[:, tuples]
        df_b = df.loc[:, [not t for t in tuples]]

        if (val_ix != 0 or val_ix == -1) and df_a.empty:
            raise PeaksNotIncluded("Peak values are not included, it's required to "
                                   "add kwarg 'ignore_peaks=False' when processing the file.")
        if val_ix == -1:
            # return a copy without any modification
            return df.copy()

        if not df_a.empty:
            # The data is stored as a tuple, value needs to be extracted
            df_a = df_a.applymap(lambda x: x[val_ix] if x is not np.nan else np.nan)
            frames.append(df_a)

        if not df_b.empty:
            # The data is stored as a single value, need to copy this
            # as subsequent actions could modify original data
            frames.append(df_b.copy())

        return pd.concat(frames, sort=False, axis=1)

    @staticmethod
    def _group_peak_outputs(df):
        """ Group 'value' and 'timestamp' columns to be adjacent for each id. """
        length = len(df.columns)
        order = list(range(1, length, 2)) + list(range(0, length, 2))
        levels = [df.columns.get_level_values(i) for i in range(df.columns.nlevels)]
        levels.insert(0, pd.Index(order))

        df.columns = pd.MultiIndex.from_arrays(levels, names=["order", "id", "data"])
        df.sort_values(by="order", inplace=True, axis=1)
        df.columns = df.columns.droplevel(0)

        return df

    def get_standard_results_only(self, transposed=False):
        """ Get df with only 'standard' outputs and 'num days'. """
        df = self.fetch_outputs(self, 0)

        try:
            df.drop("num days", axis=1, inplace=True)
        except KeyError:
            pass

        if transposed:
            df = df.T
            df.index = df.index.set_names(["id"])  # reapply the name as it gets lost when combining with 'num_days'

        return df

    def get_results_df(self, ids, val_ix, start_date, end_date):
        """ Get base output DataFrame. """
        df = slicer(self, ids, start_date=start_date, end_date=end_date)

        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        df = self.fetch_outputs(df, val_ix)

        return df

    def _validate(self, data):
        """ Validate if the data has required format. """
        length = len(data)
        df_length = len(self.index)
        valid = length == df_length

        if not valid:
            print(f"New variable contains {length} values, df length is {df_length}!"
                  "\n\t Variable will not be added to the file.")

        return valid

    def add_column(self, id_, array):
        """ Add output data. """
        is_valid = self._validate(array)

        if is_valid:
            self[id_] = array

        return is_valid

    def remove_columns(self, ids):
        """ Remove output data. """
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

    def standard_results(self, ids, start_date=None, end_date=None):
        """ Find standard result. """
        val_ix = 0
        df = self.get_results_df(ids, val_ix, start_date, end_date)
        return df

    def local_maxs(self, ids, start_date=None, end_date=None):
        """ Find local interval maxima. """
        return self._local_peaks(ids, start_date, end_date, **self._min_peak)

    def global_max(self, ids, start_date=None, end_date=None):
        val_ix = self._max_peak["val_ix"]
        return self._global_peak(ids, start_date, end_date, val_ix=val_ix)

    def timestep_max(self, ids, start_date=None, end_date=None):
        return self._timestep_peak(ids, start_date, end_date, **self._max_peak)

    def local_mins(self, ids, start_date=None, end_date=None):
        return self._local_peaks(ids, start_date, end_date, **self._min_peak)

    def global_min(self, ids, start_date=None, end_date=None):
        val_ix = self._min_peak["val_ix"]
        return self._global_peak(ids, start_date, end_date, val_ix=val_ix, max_=False)

    def timestep_min(self, ids, start_date=None, end_date=None):
        return self._timestep_peak(ids, start_date, end_date, max_=False, **self._min_peak)

    def _global_peak(self, ids, start_date, end_date, val_ix=None, max_=True):
        """ Return maximum or minimum value and datetime of occurrence. """

        df = self.get_results_df(ids, val_ix, start_date, end_date)

        vals = df.max() if max_ else df.min()
        ixs = df.idxmax() if max_ else df.idxmin()

        vals = pd.DataFrame(vals)
        ixs = pd.DataFrame(ixs)

        df = pd.concat({"timestamp": ixs.T, "value": vals.T}, axis=1)
        df = df.iloc[[0]]  # report only first occurrence
        df.columns = df.columns.swaplevel(0, 1)

        df = self._group_peak_outputs(df)
        return df

    def _local_peaks(
            self, ids, start_date, end_date, val_ix=None,
            month_ix=None, day_ix=None, hour_ix=None, end_min_ix=None,
    ):
        """
        Return value and datetime of occurrence.
        """
        val_ix = -1  # this makes sure that the original df is requested
        df = self.get_results_df(ids, val_ix, start_date, end_date)

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

        df = self._group_peak_outputs(df)
        return df

    def _timestep_peak(
            self, ids, start_date, end_date, val_ix=None, month_ix=None,
            day_ix=None, hour_ix=None, end_min_ix=None, max_=True
    ):
        """
        Return maximum or minimum hourly value and datetime of occurrence.
        """
        df = self._local_peaks(
            ids, start_date, end_date, val_ix=val_ix, hour_ix=hour_ix,
            end_min_ix=end_min_ix, day_ix=day_ix, month_ix=month_ix,
        )

        def get_peak(d):
            c = d.iloc[:, 0] == (d.iloc[:, 0].max() if max_ else d.iloc[:, 0].min())
            out = d.loc[c]
            out.reset_index(inplace=True, drop=True)
            return out

        grouped = df.groupby(axis=1, level=0, group_keys=False, sort=False)
        df = grouped.apply(get_peak).iloc[[0]]
        return df

    def get_number_of_days(self, start_date=None, end_date=None):
        """ Return 'number of days' column. """
        return slicer(self, "num days", start_date, end_date)


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


class Daily(Outputs):
    """
    Pandas.DataFrame like class to hold EnergyPlus results
    for Daily interval.

    Parameters
    ----------
    data : dict like objects
        Dict of list-like objects of processed EnergyPlus results.
    **kwargs
        Key word arguments which are passed to
        super() pandas.DataFrame class.

    Class Attributes
    ----------------
    _min_peak : dict of {str : int}
        A dictionary which holds an information on min
        peak index in output tuple.
    _max_peak : dict of {str : int}
        A dictionary which holds an information on max
        peak index in output tuple.

    """
    _min_peak = {"val_ix": 1, "hour_ix": 2, "end_min_ix": 3}
    _max_peak = {"val_ix": 4, "hour_ix": 5, "end_min_ix": 6}

    def __init__(self, data, **kwargs):
        super(Daily, self).__init__(data, **kwargs)

    def get_number_of_days(self, *args, **kwargs):
        """ Number of days is not available for Hourly interval. """
        pass


class Monthly(Outputs):
    """
    Pandas.DataFrame like class to hold EnergyPlus results
    for Monthly interval.


    Parameters
    ----------
    data : dict like objects
        Dict of list-like objects of processed EnergyPlus results.
    **kwargs
        Key word arguments which are passed to
        super() pandas.DataFrame class.

    Class Attributes
    ----------------
    _min_peak : dict of {str : int}
        A dictionary which holds an information on min
        peak index in output tuple.
    _max_peak : dict of {str : int}
        A dictionary which holds an information on max
        peak index in output tuple.

    """
    _min_peak = {"val_ix": 1, "day_ix": 2, "hour_ix": 3, "end_min_ix": 4}
    _max_peak = {"val_ix": 5, "day_ix": 6, "hour_ix": 7, "end_min_ix": 8}

    def __init__(self, data, **kwargs):
        super(Monthly, self).__init__(data, **kwargs)


class Runperiod(Outputs):
    _min_peak = {"val_ix": 1, "month_ix": 2, "day_ix": 3, "hour_ix": 4, "end_min_ix": 5}
    _max_peak = {"val_ix": 6, "month_ix": 7, "day_ix": 8, "hour_ix": 9, "end_min_ix": 10}

    def __init__(self, data, **kwargs):
        super(Runperiod, self).__init__(data, **kwargs)


class Annual(Runperiod):
    def __init__(self, data, **kwargs):
        super(Annual, self).__init__(data, **kwargs)
