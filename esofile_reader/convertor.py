from typing import List, Union, Callable, Sequence

import numpy as np
import pandas as pd

from esofile_reader.constants import *
from esofile_reader.conversion_tables import energy_table, rate_table, si_to_ip


def apply_conversion(
    df: pd.DataFrame,
    orig_units: List[str],
    new_units: List[str],
    conversion_ratios: List[Union[float, int, Callable, Sequence, pd.Series]],
) -> pd.DataFrame:
    """ Convert values for columns using specified units. """
    for old, new, ratio in zip(orig_units, new_units, conversion_ratios):
        cnd = df.columns.get_level_values(UNITS_LEVEL) == old
        if all(map(lambda x: not x, cnd)):
            # no applicable units
            continue

        if DATA_LEVEL in df.columns.names:
            cnd = cnd & (df.columns.get_level_values(DATA_LEVEL) == VALUE_LEVEL)

        if isinstance(ratio, (float, int)):
            df.loc[:, cnd] = df.loc[:, cnd] / ratio
        elif callable(ratio):
            df.loc[:, cnd] = df.loc[:, cnd].applymap(ratio)
        elif isinstance(ratio, pd.Series):
            df.loc[:, cnd] = df.loc[:, cnd].div(ratio.values, axis=0)
        else:
            df.loc[:, cnd] = df.loc[:, cnd].div(ratio, axis=0)

    update_multiindex(df, UNITS_LEVEL, orig_units, new_units)

    return df


def convert_units(
    df: pd.DataFrame, units_system: str, rate_units: str, energy_units
) -> pd.DataFrame:
    """ Convert raw E+ results to use requested units. """
    conversion_inputs = []

    for units in set(df.columns.get_level_values(UNITS_LEVEL)):
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
            if units != inp[0]:
                # TODO remove for distribution
                raise AssertionError(
                    f"Original units '{units}' do not match " f"converted units '{inp[0]}'."
                )
            else:
                conversion_inputs.append(inp)

    if not conversion_inputs:
        # there's nothing to convert
        return df

    orig_units, new_units, conversion_ratios = zip(*conversion_inputs)

    return apply_conversion(df, orig_units, new_units, conversion_ratios)


def update_multiindex(
    df: pd.DataFrame,
    level: Union[str, int],
    old_vals: List[str],
    new_vals: List[str],
    axis: int = 1,
) -> None:
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


def is_rate_or_energy(units: List[str]) -> bool:
    """ Check if all units are rate and energy. """
    return all(map(lambda x: x in ("J", "W"), units)) or all(
        map(lambda x: x in ("J/m2", "W/m2"), units)
    )


def is_daily(index: pd.DatetimeIndex):
    """ Check if index represents daily interval. """
    return len(index) > 1 and all(map(lambda x: int(x) == 8.64e13, np.diff(index)))


def is_hourly(index: pd.DatetimeIndex):
    """ Check if index represents hourly interval. """
    return len(index) > 1 and all(map(lambda x: int(x) == 3.6e12, np.diff(index)))


def is_timestep(index: pd.DatetimeIndex):
    """ Check if index represents timestep interval. """
    unique_arr = np.unique(np.diff(index))
    return len(index) > 1 and len(unique_arr) == 1 and int(unique_arr[0]) < 3.6e12


def get_n_steps(dt_index: pd.DatetimeIndex) -> float:
    """ Get a number of timesteps per hour. """
    timedelta = dt_index[1] - dt_index[0]
    return 3600 / timedelta.seconds


def convert_rate_to_energy(df: pd.DataFrame, n_days: int = None) -> pd.DataFrame:
    """ Convert 'rate' outputs to 'energy'. """
    if is_hourly(df.index) or is_timestep(df.index):
        n_steps = get_n_steps(df.index)
        ratio = n_steps / 3600
    elif is_daily(df.index):
        ratio = 1 / (24 * 3600)
    else:
        ratio = 1 / (n_days * 24 * 3600)

    orig_units = ["W", "W/m2"]
    new_units = ["J", "J/m2"]

    # ratios are the same for standard and normalized units
    conversion_ratios = [ratio, ratio]

    return apply_conversion(df, orig_units, new_units, conversion_ratios)
