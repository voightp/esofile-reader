import pandas as pd
import numpy as np
import traceback
from esofile_reader.processing.interval_processor import parse_result_dt
from esofile_reader.constants import *
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

    return PeakOutputs(df)


def create_peak_outputs(df, interval, max_=True):
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
        """ Return standard result. """
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

    @property
    def only_numeric(self):
        cond = self.columns.get_level_values("id").isin([N_DAYS_COLUMN, DAY_COLUMN])
        return self.loc[:, ~cond]

    @property
    def header_df(self):
        """ Get columns as pd.DataFrame. """
        return self.only_numeric.columns.to_frame(index=False)

    @property
    def header_variables_dct(self):
        """ Get a list of all header variables. """

        def create_variable(sr):
            return sr["id"], Variable(sr["interval"], sr["key"], sr["variable"], sr["units"])

        var_df = self.header_df.apply(create_variable, axis=1, result_type="expand")
        var_df.set_index(0, inplace=True)

        return var_df.to_dict(orient="dict")[1]

    def get_ids(self):
        """ Get all variable ids. """
        return self.only_numeric.columns.get_level_values("id")

    def rename_variable(self, id_, key_name, variable_name):
        """ Rename variable. """
        mi_df = self.columns.to_frame(index=False)
        mi_df.loc[mi_df.id == id_, ["key", "variable"]] = [key_name, variable_name]
        self.columns = pd.MultiIndex.from_frame(mi_df)

    def get_all_results(self, transposed=False, drop_special=True, ignore_units=None):
        """ Get df with only 'standard' numeric outputs. """
        df = self.only_numeric if drop_special else self

        if ignore_units:
            cnd = df.columns.get_level_values("units").isin(ignore_units)
            df = df.loc[:, ~cnd]

        try:
            return df.T.copy() if transposed else df.copy()
        except MemoryError:
            raise MemoryError(f"Cannot create outputs set copy!"
                              f"\nRunning out of memory!{traceback.format_exc()}")

    def get_results(self, ids, start_date=None, end_date=None, include_day=False):
        """ Return standard result. """
        df = super().get_results(ids, start_date, end_date)

        if include_day:
            try:
                days = self.get_days_of_week(start_date, end_date)
                df[DAY_COLUMN] = days
                df.set_index(DAY_COLUMN, append=True, inplace=True)
            except KeyError:
                try:
                    df[DAY_COLUMN] = df.index.strftime("%A")
                    df.set_index(DAY_COLUMN, append=True, inplace=True)
                except AttributeError:
                    pass

        return df

    def _validate(self, data):
        """ Validate if the data has required format. """
        length = len(data)
        df_length = len(self.index)
        valid = length == df_length

        if not valid:
            print(f"New variable contains {length} values, df length is {df_length}!"
                  "\nVariable cannot be added.")

        return valid

    def add_variable(self, id_, variable, array):
        """ Add output variable. """
        is_valid = self._validate(array)
        interval, key, variable, units = variable

        if is_valid:
            self[id_, interval, key, variable, units] = array

        return is_valid

    def remove_variables(self, ids):
        """ Remove output variable. """
        if not isinstance(ids, list):
            ids = [ids]
        try:
            self.drop(columns=ids, inplace=True, level="id")
        except KeyError:
            strids = ", ".join(ids)
            print(f"Cannot remove ids: {strids}")

        if len(self.columns) == 1:
            # df can only include one of identifiers below
            for s in [N_DAYS_COLUMN, DAY_COLUMN]:
                try:
                    self.drop(s, axis=1, inplace=True)
                except KeyError:
                    pass

    def get_number_of_days(self, start_date=None, end_date=None):
        """ Return 'number of days' column. """
        if N_DAYS_COLUMN not in self.columns:
            raise KeyError(f"'{N_DAYS_COLUMN}' column is not available "
                           f"on the given data set.")
        return slicer(self, N_DAYS_COLUMN, start_date, end_date)

    def get_days_of_week(self, start_date=None, end_date=None):
        """ Return 'days of week' column. """
        if DAY_COLUMN not in self.columns:
            raise KeyError(f"'{DAY_COLUMN}' column is not available"
                           f"on the given data set.")
        return slicer(self, DAY_COLUMN, start_date, end_date)

    def _global_peak(self, ids, start_date, end_date, max_=True):
        """ Return maximum or minimum value and datetime of occurrence. """
        df = self.get_results(ids, start_date, end_date)

        vals = pd.DataFrame(df.max() if max_ else df.min()).T
        ixs = pd.DataFrame(df.idxmax() if max_ else df.idxmin()).T

        df = _merge_peak_outputs(ixs, vals)
        df = df.iloc[[0]]  # report only first occurrence

        return df

    def global_max(self, ids, start_date=None, end_date=None):
        return self._global_peak(ids, start_date, end_date)

    def global_min(self, ids, start_date=None, end_date=None):
        return self._global_peak(ids, start_date, end_date, max_=False)
