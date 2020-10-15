import logging
from datetime import datetime
from typing import Callable, Optional, Union, List, Tuple

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.convertor import all_rate_or_energy, convert_units, convert_rate_to_energy
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import VariableType, ResultsFileType
from esofile_reader.results_processing.table_formatter import TableFormatter


def get_n_days(
    results_file,
    table: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Optional[pd.Series]:
    """ Extract n_days column without raising KeyError when unavailable."""
    try:
        n_days = results_file.tables.get_special_column(
            table, N_DAYS_COLUMN, start_date, end_date
        )
    except KeyError:
        n_days = None
    return n_days


def get_processed_results(
    results_file: ResultsFileType,
    variables: Union[VariableType, List[VariableType], int, List[int]],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    output_type: str = "standard",
    part_match: bool = False,
    table_formatter: TableFormatter = None,
    units_system: str = "SI",
    rate_units: str = "W",
    energy_units: str = "J",
    rate_to_energy: bool = False,
) -> pd.DataFrame:
    """
    Return a pandas.DataFrame object with results for given variables.

    This function extracts requested set of outputs from the file
    and converts to specified units if requested.

    Parameters
    ----------
    results_file : ResultsFileType
        A file from which results are extracted.
    variables : VariableType or list of (VariableType)
        Requested variables.
    start_date : datetime like object, default None
        A start date for requested results.
    end_date : datetime like object, default None
        An end date for requested results.
    output_type : {'standard', global_max','global_min', 'local_min', 'local_max'}
        Requested type of results (local peaks are only included on .eso files.
    part_match : bool
        Only substring of the part of variable is enough
        to match when searching for variables if this is True.
    table_formatter : TableFormatter,
        Define output table index and column items.
    units_system : {'SI', 'IP'}
        Selected units type_ for requested outputs.
    rate_to_energy : bool
        Defines if 'rate' will be converted to energy.
    rate_units : {'W', 'kW', 'MW', 'Btu/h', 'kBtu/h'}
        Convert default 'Rate' outputs to requested units.
    energy_units : {'J', 'kJ', 'MJ', 'GJ', 'Btu', 'kWh', 'MWh'}
        Convert default 'Energy' outputs to requested units

    Returns
    -------
    pandas.DataFrame
        Results for requested variables.

    """

    def standard():
        return results_file.tables.get_results_df(
            table, ids, start_date, end_date, include_day=table_formatter.include_day
        )

    def global_max():
        return results_file.tables.get_global_max_results_df(table, ids, start_date, end_date)

    def global_min():
        return results_file.tables.get_global_min_results_df(table, ids, start_date, end_date)

    def local_peak():
        try:
            return results_file.peak_tables[output_type].get_results_df(
                table, ids, start_date, end_date
            )
        except TypeError:
            raise PeaksNotIncluded(
                "Local peak outputs are not included, only Eso files with "
                "kwarg 'ignore_peaks=False' includes local peak outputs."
            )
        except KeyError:
            raise PeaksNotIncluded(
                f"Local peak outputs '{output_type}' are not available for table '{table}'."
            )

    switch = {
        "standard": standard,
        "global_max": global_max,
        "global_min": global_min,
        "local_max": local_peak,
        "local_min": local_peak,
    }

    if output_type not in switch:
        raise InvalidOutputType(
            f"Invalid output type_ '{output_type}' "
            f"requested.\n'output_type' kwarg must be"
            f" one of '{', '.join(switch.keys())}'."
        )

    if units_system not in ["SI", "IP"]:
        raise InvalidUnitsSystem(
            f"Invalid units system '{units_system}' "
            f"requested.\n'output_type' kwarg must be"
            f" one of '[SI, IP]'."
        )

    if table_formatter is None:
        table_formatter = TableFormatter()

    frames = []
    groups = results_file.find_table_id_map(variables, part_match=part_match)
    for table, ids in groups.items():
        df = switch[output_type]()
        if (
            output_type == "standard"
            and rate_to_energy
            and results_file.can_convert_rate_to_energy(table)
        ):
            n_days = get_n_days(results_file, table, start_date, end_date)
            df = convert_rate_to_energy(df, n_days)

        if units_system != "SI" or rate_units != "W" or energy_units != "J":
            df = convert_units(df, units_system, rate_units, energy_units)

        frames.append(df)

    if frames:
        df = pd.concat(frames, axis=1, sort=False)
        return table_formatter.format_table(df, results_file.file_name)
    else:
        logging.warning(
            f"Any of requested variables is not "
            f"included in the results file '{results_file.file_name}'."
        )


def get_table_for_aggregation(
    results_file: ResultsFileType, table: [str], ids: List[int]
) -> pd.DataFrame:
    """ Get results table with unified rate and energy units. """
    df = results_file.tables.get_results_df(table, ids)
    units = df.columns.get_level_values(UNITS_LEVEL)
    is_rate_and_energy = all_rate_or_energy(units) and len(units) > 1
    if is_rate_and_energy and results_file.can_convert_rate_to_energy(table):
        n_days = get_n_days(results_file, table)
        df = convert_rate_to_energy(df, n_days)
    return df


def create_default_name(
    df: pd.DataFrame, func_name: str, new_key: str, new_type: str,
) -> Tuple[str, Optional[str]]:
    """ Modify type and key if passed as default. """
    if TYPE_LEVEL in df.columns.names:
        all_types = df.columns.get_level_values(TYPE_LEVEL).tolist()
        if new_type == "Custom Type" and all(map(lambda x: x == all_types[0], all_types)):
            new_type = all_types[0]
    else:
        new_type = None
    if new_key == "Custom Key":
        new_key += f" - {func_name}"
    return new_key, new_type


def aggregate_variables(
    results_file: ResultsFileType,
    variables: Union[VariableType, List[VariableType]],
    func: Union[str, Callable],
    new_key: str = "Custom Key",
    new_type: str = "Custom Type",
    part_match: bool = False,
) -> Optional[Tuple[int, VariableType]]:
    """
    Aggregate given variables using given function.

    A new 'Variable' with specified key and variable names
    will be added into the file.

    Parameters
    ----------
    results_file : ResultsFileType
        A file from to add a new aggregated variable.
    variables : list of Variable
        A list of 'Variable' named tuples.
    func: func, func name
        Function to use for aggregating the data.
        It can be specified as np.mean, 'mean', 'sum', etc.
    new_key: str, default 'Custom Key'
        Specific key for a new variable. If this would not be
        unique, unique number is added automatically.
    new_type: str, default 'Custom Variable'
        Specific variable name for a new variable. If all the
        input 'Variables' share the same variable name, this
        will be used if nto specified otherwise.
    part_match : bool
        Only substring of the part of variable is enough
        to match when searching for variables if this is True.

    Returns
    -------
    int, Variable or None
        A numeric id of the new added variable. If the variable
        could not be added, None is returned.

    """
    table_id_map = results_file.find_table_id_map(variables, part_match=part_match)
    if not table_id_map:
        raise CannotAggregateVariables("Cannot find variables!")

    if len(table_id_map.keys()) > 1:
        raise CannotAggregateVariables("Cannot aggregate variables from multiple tables!")

    table, ids = list(table_id_map.items())[0]
    df = get_table_for_aggregation(results_file, table, ids)
    if len(ids) < 2:
        raise CannotAggregateVariables("Cannot aggregate single variable!")

    units = df.columns.get_level_values(UNITS_LEVEL).unique()
    if len(units) > 1:
        raise CannotAggregateVariables("Cannot aggregate variables with multiple units!")

    sr = df.aggregate(func, axis=1)

    func_name = func.__name__ if callable(func) else func
    new_key, new_type = create_default_name(df, func_name, new_key, new_type)
    new_units = units[0]

    # return value can be either tuple (id, Variable) or None
    out = results_file.insert_variable(table, new_key, new_units, sr, type_=new_type)

    return out
