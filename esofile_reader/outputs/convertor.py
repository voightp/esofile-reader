from esofile_reader.outputs.conversion_tables import energy_table, rate_table, si_to_ip
from esofile_reader.constants import *
from typing import List, Union, Callable, Sequence

import pandas as pd


def apply_conversion(df: pd.DataFrame, orig_units: List[str], new_units: List[str],
                     conversion_ratios: List[Union[float, int, Callable, Sequence, pd.Series]]) -> pd.DataFrame:
    """ Convert values for columns using specified units. """
    for old, new, ratio in zip(orig_units, new_units, conversion_ratios):
        cnd = df.columns.get_level_values("units") == old
        if all(map(lambda x: not x, cnd)):
            # no applicable units
            continue

        if "data" in df.columns.names:
            cnd = cnd & (df.columns.get_level_values("data") == "value")

        if isinstance(ratio, (float, int)):
            df.loc[:, cnd] = df.loc[:, cnd] / ratio
        elif callable(ratio):
            df.loc[:, cnd] = df.loc[:, cnd].applymap(ratio)
        elif isinstance(ratio, pd.Series):
            df.loc[:, cnd] = df.loc[:, cnd].div(ratio.values, axis=0)
        else:
            df.loc[:, cnd] = df.loc[:, cnd].div(ratio, axis=0)

    update_multiindex(df, "units", orig_units, new_units)

    return df


def convert_units(df: pd.DataFrame, units_system: str, rate_units: str, energy_units) -> pd.DataFrame:
    """ Convert raw E+ results to use requested units. """
    conversion_inputs = []

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
            conversion_inputs.append(inp)

    if not conversion_inputs:
        # there's nothing to convert
        return df

    orig_units, new_units, conversion_ratios = zip(*conversion_inputs)

    return apply_conversion(df, orig_units, new_units, conversion_ratios)


def update_multiindex(df: pd.DataFrame, level: Union[str, int], old_vals: List[str],
                      new_vals: List[str], axis: int = 1) -> None:
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


def rate_and_energy_units(units: List[str]) -> bool:
    """ Check if all units are rate and energy. """
    return all(map(lambda x: x in ("J", "W"), units)) \
           or all(map(lambda x: x in ("J/m2", "W/m2"), units))


def get_n_steps(dt_index: pd.DatetimeIndex) -> float:
    """ Get a number of timesteps per hour. """
    timedelta = dt_index[1] - dt_index[0]
    return 3600 / timedelta.seconds


def convert_rate_to_energy(df: pd.DataFrame, interval: str, n_days: int = None) -> pd.DataFrame:
    """ Convert 'rate' outputs to 'energy'. """
    if interval == H or interval == TS:
        n_steps = get_n_steps(df.index)
        ratio = n_steps / 3600
    elif interval == D:
        ratio = 1 / (24 * 3600)
    else:
        ratio = 1 / (n_days * 24 * 3600)

    orig_units = ["W", "W/m2"]
    new_units = ["J", "J/m2"]

    # ratios are the same for standard and normalized units
    conversion_ratios = [ratio, ratio]

    return apply_conversion(df, orig_units, new_units, conversion_ratios)
