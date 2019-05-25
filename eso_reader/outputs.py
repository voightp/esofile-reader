import pandas as pd
import numpy as np
from interval_processor import parse_result_dt


class Outputs(pd.DataFrame):
    """
    A parent class to define all methods required for extracting
    specific E+ results.

    Only method shared by all subclasses is to find standard results.
    Maximum and minimum methods are in place to avoid non implementing
    all the required results for all subclasses.

    Local, global and hourly peak methods are base methods to
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
    def fetch_results(data, index):
        """ Extract results column from df. """
        if data.dtype == object:
            # The data is stored as a tuple, value needs to be extracted
            return data.apply(lambda x: x[index] if x is not np.nan else np.nan)

        # The data is stored as a single value
        return data.copy()

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

    def standard_results(self, var_id, start_date, end_date, header):
        """ Find standard result. """
        data = self[var_id]
        results = self.fetch_results(data, 0)
        res = results.values
        ix = results.index

        col_ix = self.gen_column_index(var_id, header)
        res = pd.DataFrame(res, index=ix, columns=col_ix)
        return res[start_date:end_date]

    def local_maxima(self, var_id, start_date, end_date, header):
        """ Find local interval maxima. """
        return self._local_peaks(
            var_id, start_date, end_date, header, **self._min_peak,
        )

    def global_maximum(self, var_id, start_date, end_date, header, tmstmp_frm="default"):
        val_ix = self._max_peak["val_ix"]
        return self._global_peak(
            var_id, start_date, end_date,
            header, tmstmp_frm=tmstmp_frm,
            val_ix=val_ix
        )

    def timestep_maximum(self, var_id, start_date, end_date, header, tmstmp_frm="default"):
        return self._timestep_peak(
            var_id, start_date, end_date,
            header, tmstmp_frm=tmstmp_frm,
            **self._max_peak
        )

    def local_minima(self, var_id, start_date, end_date, header):
        return self._local_peaks(
            var_id, start_date, end_date,
            header, **self._min_peak
        )

    def global_minimum(self, var_id, start_date, end_date, header, tmstmp_frm="default"):
        val_ix = self._min_peak["val_ix"]
        return self._global_peak(
            var_id, start_date, end_date,
            header, tmstmp_frm=tmstmp_frm,
            val_ix=val_ix, maximum=False
        )

    def timestep_minimum(self, var_id, start_date, end_date, header, tmstmp_frm="default"):
        return self._timestep_peak(
            var_id, start_date, end_date,
            header, maximum=False,
            tmstmp_frm=tmstmp_frm,
            **self._min_peak,
        )

    @staticmethod
    def _ashrae_peak(timestamp):
        """ Generate peak in format required for ASHRAE 140. """
        return timestamp.strftime("%d-%b %H").split()

    def _global_peak(self, var_id, start_date, end_date, header, val_ix=None, maximum=True, tmstmp_frm="default"):
        """ Return maximum or minimum value and datetime of occurrence. """
        data = self[var_id][start_date:end_date]
        results = self.fetch_results(data, val_ix)

        timestamp, peak = (results.idxmax(), results.max()) if maximum else (results.idxmin(), results.min())

        col_ix = self.gen_column_index(var_id, header, peak=True, tmstmp_frm=tmstmp_frm)
        if tmstmp_frm.lower() == "ashrae":
            date, time = self._ashrae_peak(timestamp)
            return pd.DataFrame([(peak, date, time)], columns=col_ix)

        return pd.DataFrame([(peak, timestamp)], columns=col_ix)

    def _local_peaks(
            self, var_id, start_date, end_date, header, val_ix=None,
            month_ix=None, day_ix=None, hour_ix=None, end_min_ix=None,
    ):
        """
        Return value and datetime of occurrence.
        """
        var = self[var_id][start_date:end_date]

        peak_dts = []
        for row in var.iteritems():
            index, value = row
            peak_dts.append(parse_result_dt(index, value, month_ix, day_ix, hour_ix, end_min_ix))

        var.index = peak_dts
        results = var.apply(lambda x: x[val_ix])

        res = results.values
        ix = results.index
        ix.name = "timestamp"

        col_ix = self.gen_column_index(var_id, header)
        res = pd.DataFrame(res, index=ix, columns=col_ix)
        return res[start_date:end_date]

    def _timestep_peak(
            self, var_id, start_date, end_date, header, val_ix=None, month_ix=None,
            day_ix=None, hour_ix=None, end_min_ix=None, maximum=True, tmstmp_frm="default"
    ):
        """
        Return maximum or minimum hourly value and datetime of occurrence.
        """
        data = self._local_peaks(
            var_id, start_date, end_date, header, val_ix=val_ix, hour_ix=hour_ix,
            end_min_ix=end_min_ix, day_ix=day_ix, month_ix=month_ix,
        )

        timestamp, peak = (data.idxmax(), data.max()) if maximum else (data.idxmin(), data.min())

        peak = peak.iloc[0]
        timestamp = timestamp.iloc[0]

        col_ix = self.gen_column_index(var_id, header, peak=True, tmstmp_frm=tmstmp_frm)

        if tmstmp_frm.lower() == "ashrae":
            date, time = self._ashrae_peak(timestamp)
            return pd.DataFrame([(peak, date, time)], columns=col_ix)

        return pd.DataFrame([(peak, timestamp)], columns=col_ix)

    @staticmethod
    def gen_column_index(var_id, header, peak=False, tmstmp_frm="default"):
        """ Generate column multi index. """
        if peak:
            if tmstmp_frm.lower() == "ashrae":
                return pd.MultiIndex(
                    levels=[[var_id], [header[0]], [header[1]], [header[2]], ["value", "date", "time"]],
                    labels=[[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 1, 2]],
                    names=["id", "key", "variable", "units", "data"])
            else:
                return pd.MultiIndex(
                    levels=[[var_id], [header[0]], [header[1]], [header[2]], ["value", "timestamp"]],
                    labels=[[0, 0], [0, 0], [0, 0], [0, 0], [0, 1]],
                    names=["id", "key", "variable", "units", "data"]
                )
        else:
            return pd.MultiIndex(
                levels=[[var_id], [header[0]], [header[1]], [header[2]]],
                labels=[[0], [0], [0], [0]],
                names=["id", "key", "variable", "units"]
            )


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

    def global_maximum(self, var_id, start_date, end_date, header, tmstmp_frm="default"):
        """ Return an interval maximum value and date of occurrence. """
        return self._global_peak(var_id, start_date, end_date, header, val_ix=0, tmstmp_frm=tmstmp_frm)

    def global_minimum(self, var_id, start_date, end_date, header, tmstmp_frm="default"):
        """ Return an interval minimum value and date of occurrence. """
        return self._global_peak(var_id, start_date, end_date, header, val_ix=0, maximum=False, tmstmp_frm=tmstmp_frm)

    def local_maxima(self, *args, **kwargs):
        """ Local maximum values are not applicable for Hourly interval. """
        pass

    def local_minima(self, *args, **kwargs):
        """ Local minimum values are not applicable for Hourly interval. """
        pass

    def timestep_minimum(self, *args, **kwargs):
        """ Timestep maximum value is not applicable for Hourly interval. """
        pass

    def timestep_maximum(self, *args, **kwargs):
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

    def timestep_minimum(self, var_id, start_date, end_date, header, tmstmp_frm="default"):
        """ Timestep minimum value is the same as global minimum for Timestep interval. """
        return self._global_peak(var_id, start_date, end_date, header, val_ix=0, maximum=False, tmstmp_frm=tmstmp_frm)

    def timestep_maximum(self, var_id, start_date, end_date, header, tmstmp_frm="default"):
        """ Timestep maximum value is the same as global maximum for Timestep interval. """
        return self._global_peak(var_id, start_date, end_date, header, val_ix=0, tmstmp_frm=tmstmp_frm)


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
