from eso_reader.outputs import Timestep, Hourly, Daily
from eso_reader.conversion_tables import energy_table, rate_table, si_to_ip
from eso_reader.performance import perf

import pandas as pd


@perf
def apply_conversion(df, orig_units, new_units, conv_ratios):
    """ Convert values for columns using specified units. """
    for old, new, conv in zip(orig_units, new_units, conv_ratios):
        cnd = df.columns.get_level_values("units") == old

        if "data" in df.columns.names:
            cnd = cnd & (df.columns.get_level_values("data") == "value")

        if isinstance(conv, (float, int)):
            df.loc[:, cnd] = df.loc[:, cnd] / conv
        elif callable(conv):
            df.loc[:, cnd] = df.loc[:, cnd].applymap(conv)
        else:
            df.loc[:, cnd] = df.loc[:, cnd].div(conv, axis=0)

    update_multiindex(df, "units", orig_units, new_units)

    return df


@perf
def convert_units(df, units_system, rate_units, energy_units):
    """ Convert raw E+ results to use requested units. """

    conv_input = []

    for units in set(df.columns.get_level_values("units")):
        inp = None

        if units == "J" and energy_units != "J":
            inp = energy_table(energy_units)
        elif units == "J/m2" and energy_units != "J":
            inp = energy_table(energy_units, per_area=True)
        elif units == "W" and rate_units != "W":
            inp = rate_table(rate_units)
        elif units == "W/m2" and rate_units != "W":
            inp = rate_table(rate_units, per_area=True)
        elif units_system == "IP":
            inp = si_to_ip(units)

        if inp:
            assert units == inp[0], "Original units do not match!"  # TODO remove for distribution
            conv_input.append(inp)

    if not conv_input:
        # there's nothing to convert
        return df

    orig_units, new_units, conv_ratios = zip(*conv_input)

    return apply_conversion(df, orig_units, new_units, conv_ratios)


@perf
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


@perf
def verify_units(units):
    """ Check if the variables can be aggregated. """
    if all(map(lambda x: x == units[0], units)):
        # all units are the same
        return units[0]
    elif all(map(lambda x: x in ("J", "W"), units)):
        # rate will be converted to energy
        return units
    elif all(map(lambda x: x in ("J/m2", "W/m2"), units)):
        # rate will be converted to energy
        return units


@perf
def rate_to_energy(df, data_set, start_date=None, end_date=None):
    """ Convert 'rate' outputs to 'energy'. """
    if isinstance(data_set, Hourly):
        conv_ratio = 1 / 3600
    elif isinstance(data_set, Timestep):
        n_steps = data_set.get_n_steps()
        conv_ratio = n_steps / 3600
    elif isinstance(data_set, Daily):
        conv_ratio = 1 / (24 * 3600)
    else:
        sr = data_set.get_number_of_days(start_date=start_date, end_date=end_date)
        conv_ratio = 1 / (sr * 24 * 3600)

    orig_units = ("W", "W/m2")
    new_units = ("J", "J/m2")
    conv_ratios = (conv_ratio, conv_ratio)  # ratios are the same

    return apply_conversion(df, orig_units, new_units, conv_ratios)
