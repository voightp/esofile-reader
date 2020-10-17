import logging
from datetime import datetime
from typing import Union, List, Optional

import pandas as pd

from esofile_reader import base_file, ResultsFile
from esofile_reader.mini_classes import VariableType, ResultsFileType, PathLike
from esofile_reader.results_processing.process_results import get_processed_results
from esofile_reader.results_processing.table_formatter import TableFormatter


def get_results_from_single_file(
    file: Union[ResultsFile, PathLike],
    variables: Union[VariableType, List[VariableType]],
    **kwargs,
) -> Optional[pd.DataFrame]:
    """ Load eso file and return requested results. """
    if issubclass(type(file), base_file.BaseFile):
        results_file = file
    else:
        results_file = ResultsFile.from_path(file)
    return get_processed_results(results_file, variables, **kwargs)


def get_results_from_multiple_files(
    file_list: List[Union[ResultsFile, PathLike]],
    variables: Union[VariableType, List[VariableType]],
    **kwargs,
) -> Optional[pd.DataFrame]:
    """ Extract results from multiple files. """
    frames = []
    for file in file_list:
        df = get_results_from_single_file(file, variables, **kwargs)
        if df is not None:
            frames.append(df)
    try:
        return pd.concat(frames, axis=1, sort=False)
    except ValueError:
        # joined_variables = ", ".join(variables) if isinstance(variables, list) else variables
        logging.warning(f"Any of requested variables: '[{variables}]' was not found!")


def get_results(
    files: Union[List[Union[ResultsFileType, PathLike]], Union[ResultsFileType, PathLike]],
    variables: Union[VariableType, List[VariableType]],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    output_type: str = "standard",
    part_match: bool = False,
    table_formatter: TableFormatter = None,
    units_system: str = "SI",
    rate_units: str = "W",
    energy_units: str = "J",
    rate_to_energy: bool = False,
) -> Optional[pd.DataFrame]:
    """
     Return a pandas.DataFrame object with outputs for specified request.

     Eso files can be specified using 'path' like objects or an instantiated
     'EsoFile' class object. This function accepts either single file or multiple
     files in a list.

     Multiple options are available to transform the original 'Eso' file outputs.

     Parameters
     ----------
         files : {str, EsoFile} or list of ({str, EsoFile})
            Eso files defined as 'EsoFile' objects or using path like objects.
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
    kwargs = {
        "start_date": start_date,
        "end_date": end_date,
        "output_type": output_type,
        "table_formatter": table_formatter,
        "units_system": units_system,
        "rate_to_energy": rate_to_energy,
        "rate_units": rate_units,
        "energy_units": energy_units,
        "part_match": part_match,
    }
    if isinstance(files, list):
        return get_results_from_multiple_files(files, variables, **kwargs)
    return get_results_from_single_file(files, variables, **kwargs)
