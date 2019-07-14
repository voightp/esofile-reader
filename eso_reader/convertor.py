from eso_reader.outputs import Timestep, Hourly, Daily
from eso_reader.outputs import slicer
from eso_reader.tables import energy_table, rate_table, si_to_ip

import pandas as pd


def apply_conversion(df, orig_units, new_units, conv_ratios):
    """ Convert values for columns using specified units. """
    for old, new, conv in zip(orig_units, new_units, conv_ratios):
        cnd = df.columns.get_level_values("units") == old
        if df.columns.nlevels == 4:
            cnd = cnd & df.columns.get_level_values("data") == "value"

        if isinstance(conv, (float, int)):
            df.loc[:, cnd] = df.loc[:, cnd] / conv
        elif callable(conv):
            df.loc[:, cnd] = df.loc[:, cnd].applymap(conv)
        else:
            df.loc[:, cnd] = df.loc[:, cnd].div(conv, axis=0)

    update_multiindex(df, "units", orig_units, new_units)
    return df


def convert_units(df, units_system, rate_units, energy_units):
    """ Convert raw E+ results to use requested units. """

    er_dct = {
        "W": rate_table(units_system, rate_units),
        "W/m2": rate_table(rate_units, per_area=True),
        "J": energy_table(units_system, energy_units),
        "J/m2": energy_table(energy_units, per_area=True),
    }

    conv_input = []

    for units in df.columns.levels[2]:
        out = None

        if units in er_dct:
            out = er_dct[units]
        elif units_system == "IP":
            out = si_to_ip(units_system)

        if out:
            conv_input.append(out)

    orig_units, new_units, conv_ratios = zip(*conv_input)
    df = apply_conversion(df, orig_units, new_units, conv_ratios)

    return df


def get_timestep_n(df):
    """ Get a number of timesteps in hour. """
    timestamps = df.index
    timedelta = timestamps[1] - timestamps[0]
    n_steps = 3600 / timedelta.seconds
    return n_steps


def update_multiindex(df, level, old_vals, new_vals, axis=1):
    """ Replace multiindex values on a specific level inplace. """

    def replace(val):
        if val in old_vals:
            return new_vals[old_vals.index(val)]
        else:
            return val

    mi = df.columns if axis == 1 else df.index

    if isinstance(level, str):
        # let 'Value Error' be raised when invalid
        level = mi.names.index(level)

    levels = [mi.get_level_values(i) for i in range(mi.nlevels)]

    new_level = [replace(val) for val in levels[level]]
    levels[level] = new_level
    new_mi = pd.MultiIndex.from_arrays(levels, names=mi.names)

    if axis == 1:
        df.columns = new_mi
    else:
        df.index = new_mi


def rate_to_energy(df, data_set, start_date, end_date):
    """ Convert 'rate' outputs to 'energy'. """
    if isinstance(data_set, Hourly):
        conv_ratio = 1 / 3600
    elif isinstance(data_set, Timestep):
        n_steps = get_timestep_n(data_set)
        conv_ratio = n_steps / 3600
    elif isinstance(data_set, Daily):
        conv_ratio = 1 / (24 * 3600)
    else:
        sr = slicer(data_set, "num days", start_date, end_date)
        conv_ratio = 1 / (sr * 24 * 3600)

    orig_units = ("W", "W/m2")
    new_units = ("J", "J/m2")
    conv_ratios = (conv_ratio, conv_ratio)  # ratios are the same

    return apply_conversion(df, orig_units, new_units, conv_ratios)
