import pandas as pd
import numpy as np
from eso_reader.interval_processor import parse_result_dt


class PeaksNotIncluded(Exception):
    """ Exception is raised when EsoFile has been processed without peaks. """
    pass


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
    def fetch_outputs(df, index):
        """ Extract results column from df. """

        if index == -1:
            # return a copy without any modification
            return df.copy()

        frames = []
        tuples = list(map(lambda x: x == object, df.dtypes))

        df_a = df.loc[:, tuples]
        df_b = df.loc[:, [not t for t in tuples]]

        if (index != 0 or index == -1) and df_a.empty:
            raise PeaksNotIncluded("Peak values are not included, it's required to "
                                   "add kwarg 'ignore_peaks=False' when processing the file.")

        if not df_a.empty:
            # The data is stored as a tuple, value needs to be extracted
            df_a = df_a.applymap(lambda x: x[index] if x is not np.nan else np.nan)
            frames.append(df_a)

        if not df_b.empty:
            # The data is stored as a single value, need to copy this
            # as subsequent actions could modify original data
            frames.append(df_b.copy())

        return pd.concat(frames, sort=False)

    def get_results_df(self, ids, index, start_date, end_date):
        """ Get base output DataFrame. """
        try:
            if start_date and end_date:
                df = self.loc[start_date:end_date, ids]
            elif start_date:
                df = self.loc[start_date:, ids]
            elif end_date:
                df = self.loc[:end_date, ids]
            else:
                df = self.loc[:, ids]

        except KeyError:
            # TODO catch specific exceptions
            print("KEY ERROR")
            raise Exception("FOO")

        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        df = self.fetch_outputs(df, index)

        return df

    def num_of_rows(self):
        return len(self.index)

    def _validate(self, data):
        """ Validate if the data has required format. """
        # At the moment, just length is being checked
        valid = False
        length = len(data)
        df_length = self.num_of_rows()

        if length == df_length:
            valid = True

        else:
            print("Warning: new variable contains {} values, df length is {}!".format(length, df_length))
            print("\t Variable will not be added to the file. ")

        return valid

    def add_output(self, id, array):
        """ Add output data. """
        is_valid = self._validate(array)

        if is_valid:
            self[id] = array

        return is_valid

    def standard_results(self, ids, start_date, end_date):
        """ Find standard result. """
        index = 0
        df = self.get_results_df(ids, index, start_date, end_date)
        return df

    def local_maxs(self, ids, start_date, end_date):
        """ Find local interval maxima. """
        return self._local_peaks(ids, start_date, end_date, **self._min_peak)

    def global_max(self, ids, start_date, end_date):
        val_ix = self._max_peak["val_ix"]
        return self._global_peak(ids, start_date, end_date, val_ix=val_ix)

    def timestep_max(self, ids, start_date, end_date):
        return self._timestep_peak(ids, start_date, end_date, **self._max_peak)

    def local_mins(self, ids, start_date, end_date):
        return self._local_peaks(ids, start_date, end_date, **self._min_peak)

    def global_min(self, ids, start_date, end_date):
        val_ix = self._min_peak["val_ix"]
        return self._global_peak(ids, start_date, end_date, val_ix=val_ix, max_=False)

    def timestep_min(self, ids, start_date, end_date):
        return self._timestep_peak(ids, start_date, end_date, maximum=False, **self._min_peak)

    @staticmethod
    def _ashrae_peak(timestamp):
        """ Generate peak in format required for ASHRAE 140. """
        return timestamp.strftime("%d-%b %H").split()

    def _global_peak(self, ids, start_date, end_date, val_ix=None, max_=True):
        """ Return maximum or minimum value and datetime of occurrence. """

        df = self.get_results_df(ids, val_ix, start_date, end_date)

        vals = df.max() if max_ else df.min()
        ixs = df.idxmax() if max_ else df.idxmin()

        out = pd.concat([vals, ixs], keys=["value", "timestamp"], names=["data", "id"])
        out = pd.DataFrame(out)

        out.reset_index(inplace=True)
        out.sort_values(by="id", inplace=True)
        out.set_index(["id", "data"], inplace=True)

        # if tmstmp_frm.lower() == "ashrae": #TODO postprocess this elsewhere
        #     date, time = self._ashrae_peak(timestamp)
        #     return pd.DataFrame([(peak, date, time)])

        return out.T

    def _local_peaks(
            self, ids, start_date, end_date, val_ix=None,
            month_ix=None, day_ix=None, hour_ix=None, end_min_ix=None,
    ):
        """
        Return value and datetime of occurrence.
        """
        index = -1  # this makes sure that the original df is requested
        df = self.get_results_df(ids, index, start_date, end_date)

        def get_timestamps(sr):
            def parse_vals(val):
                ts = parse_result_dt(date, val, month_ix, day_ix, hour_ix, end_min_ix)
                return ts

            date = sr.name
            sr = sr.apply(parse_vals)

            return sr

        timestamps = df.apply(get_timestamps, axis=1)
        results = df.applymap(lambda x: x[val_ix])

        out = pd.concat([results.T, timestamps.T], keys=["value", "timestamp"], names=["data", "id"])
        out.reset_index(inplace=True)
        out.sort_values(by="id", inplace=True)
        out.set_index(["id", "data"], inplace=True)

        out = out.T
        #TODO get original dtype
        return out.T

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
        print(df.dtypes)

        df_vals = df.loc[:, df.columns.get_level_values(1) == "value"]
        df_vals = df_vals.droplevel(1, axis=1)
        df_ixs = df.loc[:, df.columns.get_level_values(1) == "timestamp"]

        group = df.groupby(axis=1, level=0)

        for n, a in group:
            gr = a.iloc[:, 0]
            # print(a.dtypes)
            # print(a)
            # ix = gr.idxmax() if max_ else gr.idxmin()
            # print(n, ix)

        vals = df_vals.max() if max_ else df_vals.min()

        # out = pd.concat([vals, ixs], keys=["value", "timestamp"], names=["data", "id"])
        # out = pd.DataFrame(out)
        #
        # out.reset_index(inplace=True)
        # out.sort_values(by="id", inplace=True)
        # out.set_index(["id", "data"], inplace=True)

        # if tmstmp_frm.lower() == "ashrae": # TODO postprocess this elsewhere
        #     date, time = self._ashrae_peak(timestamp)
        #     return pd.DataFrame([(peak, date, time)])

        return

    # @staticmethod
    # def gen_column_index(ids, peak=False, tmstmp_frm="default"):
    #     """ Generate column multi index. """
    #     if peak:
    #         if tmstmp_frm.lower() == "ashrae":
    #             return pd.MultiIndex(
    #                 levels=[[ids], [header[0]], [header[1]], [header[2]], ["value", "date", "time"]],
    #                 codes=[[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 1, 2]],
    #                 names=["id", "key", "variable", "units", "data"])
    #         else:
    #             return pd.MultiIndex(
    #                 levels=[[ids], [header[0]], [header[1]], [header[2]], ["value", "timestamp"]],
    #                 codes=[[0, 0], [0, 0], [0, 0], [0, 0], [0, 1]],
    #                 names=["id", "key", "variable", "units", "data"]
    #             )
    #     else:
    #         return pd.MultiIndex(
    #             levels=[[ids], [header[0]], [header[1]], [header[2]]],
    #             codes=[[0], [0], [0], [0]],
    #             names=["id", "key", "variable", "units"]
    #         )


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

    def global_max(self, ids, start_date, end_date):
        """ Return an interval maximum value and date of occurrence. """
        return self._global_peak(ids, start_date, end_date, val_ix=0)

    def global_min(self, ids, start_date, end_date):
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
