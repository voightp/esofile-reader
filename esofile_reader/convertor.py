import contextlib
from typing import List, Union, Tuple, Dict, Callable, Optional

import numpy as np
import pandas as pd

from esofile_reader.conversion_tables import (
    ENERGY_TABLE,
    ENERGY_TABLE_PER_AREA,
    RATE_TABLE,
    RATE_TABLE_PER_AREA,
    SI_TO_IP,
)
from esofile_reader.df.level_names import (
    N_DAYS_COLUMN,
    DATA_LEVEL,
    VALUE_LEVEL,
    UNITS_LEVEL,
    KEY_LEVEL,
)
from esofile_reader.typehints import ResultsFileType


def apply_conversion(
    df: pd.DataFrame, conversion_dict: Dict[str, Tuple[str, Union[Callable, float, int]]],
) -> pd.DataFrame:
    """ Convert values for columns using specified units. """
    for old, (new, factor) in conversion_dict.items():
        convert_arr = df.columns.get_level_values(UNITS_LEVEL) == old
        if DATA_LEVEL in df.columns.names:
            convert_arr = convert_arr & (df.columns.get_level_values(DATA_LEVEL) == VALUE_LEVEL)

        if isinstance(factor, (float, int)):
            df.loc[:, convert_arr] = df.loc[:, convert_arr] * factor
        elif callable(factor):
            df.loc[:, convert_arr] = df.loc[:, convert_arr].applymap(factor)
        elif isinstance(factor, pd.Series):
            df.loc[:, convert_arr] = df.loc[:, convert_arr].mul(factor.values, axis=0)
        else:
            df.loc[:, convert_arr] = df.loc[:, convert_arr].mul(factor, axis=0)

    units_lookup = {k: v[0] for k, v in conversion_dict.items()}
    df.columns = update_units_level(df.columns, units_lookup)
    return df


def create_conversion_dict(
    source_units: Union[pd.Series, pd.Index],
    units_system: str,
    rate_units: str,
    energy_units: str,
) -> Dict[str, Tuple[str, Union[Callable, float, int]]]:
    """ Get relevant converted units and conversion factors. """
    conversion_dict = {}
    for units in source_units.unique():
        if units == "J" and energy_units != "J":
            conversion_dict[units] = ENERGY_TABLE[energy_units]
        elif units == "J/m2" and energy_units != "J":
            conversion_dict[units] = ENERGY_TABLE_PER_AREA[energy_units]
        elif units == "W" and rate_units != "W":
            conversion_dict[units] = RATE_TABLE[rate_units]
        elif units == "W/m2" and rate_units != "W":
            conversion_dict[units] = RATE_TABLE_PER_AREA[rate_units]
        elif units_system == "IP":
            with contextlib.suppress(KeyError):
                conversion_dict[units] = SI_TO_IP[units]
    return conversion_dict


def convert_units(
    df: pd.DataFrame, units_system: str, rate_units: str, energy_units
) -> pd.DataFrame:
    """ Convert raw E+ results to use requested units. """
    conversion_dict = create_conversion_dict(
        df.columns.get_level_values(UNITS_LEVEL),
        units_system=units_system,
        rate_units=rate_units,
        energy_units=energy_units,
    )
    if conversion_dict:
        # there's nothing to convert
        return apply_conversion(df, conversion_dict)
    else:
        return df


def update_units_level(mi: pd.MultiIndex, units_dict: Dict[str, str],) -> pd.MultiIndex:
    """ Replace given units with converted ones. """

    def replace(val):
        try:
            updated = units_dict[val]
        except KeyError:
            updated = val
        return updated

    all_levels = [mi.get_level_values(i) for i in range(mi.nlevels)]
    units_level_index = mi.names.index(UNITS_LEVEL)
    updated_units_level = [replace(units) for units in all_levels[units_level_index]]
    all_levels[units_level_index] = updated_units_level
    return pd.MultiIndex.from_arrays(all_levels, names=mi.names)


def all_rate_or_energy(units: List[str]) -> bool:
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
    return 3600 / (dt_index[1] - dt_index[0]).seconds


def convert_rate_to_energy(
    df: pd.DataFrame, n_days: Optional[pd.Series] = None
) -> pd.DataFrame:
    """ Convert 'rate' outputs to 'energy'. """
    if is_hourly(df.index) or is_timestep(df.index):
        n_steps = get_n_steps(df.index)
        factor = 3600 / n_steps
    elif is_daily(df.index):
        factor = 24 * 3600
    else:
        factor = n_days * 24 * 3600
    # ratios are the same for standard and normalized units
    return apply_conversion(df, {"W": ("J", factor), "W/m2": ("J/m2", factor)})


def can_convert_rate_to_energy(results_file: ResultsFileType, table: str) -> bool:
    """ Check rate can be converted to energy on given table. """
    df = results_file.get_special_table(table)
    n_days_available = N_DAYS_COLUMN in df.columns.get_level_values(KEY_LEVEL)
    if not n_days_available:
        if isinstance(df.index, pd.DatetimeIndex):
            return is_daily(df.index) or is_hourly(df.index) or is_timestep(df.index)
        return False
    return n_days_available
