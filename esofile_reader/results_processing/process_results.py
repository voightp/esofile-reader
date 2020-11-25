import logging
from datetime import datetime
from typing import Optional, Union, List

import pandas as pd

from esofile_reader.convertor import convert_units, convert_rate_to_energy
from esofile_reader.df.level_names import N_DAYS_COLUMN
from esofile_reader.exceptions import *
from esofile_reader.typehints import ResultsFileType, VariableType
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
        except (TypeError, AttributeError):
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
