from typing import Union, List, Callable, Optional, Tuple

import pandas as pd

from esofile_reader.df.level_names import TYPE_LEVEL, UNITS_LEVEL
from esofile_reader.convertor import (
    all_rate_or_energy,
    convert_rate_to_energy,
    can_convert_rate_to_energy,
)
from esofile_reader.exceptions import CannotAggregateVariables
from esofile_reader.typehints import ResultsFileType, VariableType
from esofile_reader.results_processing.process_results import get_n_days


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


def get_table_for_aggregation(
    results_file: ResultsFileType, table: str, ids: List[int]
) -> pd.DataFrame:
    """ Get results table with unified rate and energy units. """
    df = results_file.tables.get_results_df(table, ids)
    units = df.columns.get_level_values(UNITS_LEVEL).unique()
    is_rate_and_energy = all_rate_or_energy(units) and len(units) > 1
    if is_rate_and_energy and can_convert_rate_to_energy(results_file, table):
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
