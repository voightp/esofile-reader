import logging
from datetime import datetime
from typing import Union, List, Dict, Optional

import pandas as pd

from esofile_reader.base_file import BaseFile
from esofile_reader.constants import *
from esofile_reader.eso_file import EsoFile
from esofile_reader.mini_classes import Variable


def get_results(
        files,
        variables: Union[Variable, List[Variable]],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        output_type: str = "standard",
        add_file_name: str = "row",
        include_interval: bool = False,
        include_day: bool = False,
        include_id: bool = False,
        part_match: bool = False,
        units_system: str = "SI",
        rate_units: str = "W",
        energy_units: str = "J",
        timestamp_format: str = "default",
    rate_to_energy_dct: Dict[str, bool] = RATE_TO_ENERGY_DCT,
    ignore_peaks: bool = True,
):
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
         variables : Variable or list of (Variable)
            Requested variables..
         start_date : datetime like object, default None
            A start date for requested results.
         end_date : datetime like object, default None
            An end date for requested results.
         output_type : {
                'standard', 'local_max',' global_max', 'timestep_max',
                'local_min', 'global_min', 'timestep_min'
                }
            Requested type_ of results.
         add_file_name : ('row','column',None)
            Specify if file name should be added into results df.
         include_interval : bool
            Decide if 'interval' information should be included on
            the results df.
         include_id : bool
            Decide if variable 'id' should be included on the results df.
         include_day : bool
            Add day of week into index, this is applicable only for 'timestep',
            'hourly' and 'daily' outputs.
         part_match : bool
            Only substring of the part of variable is enough
            to match when searching for variables if this is True.
         units_system : {'SI', 'IP'}
            Selected units type_ for requested outputs.
         rate_to_energy_dct : dct
            Defines if 'rate' will be converted to energy.
         rate_units : {'W', 'kW', 'MW', 'Btu/h', 'kBtu/h'}
            Convert default 'Rate' outputs to requested units.
         energy_units : {'J', 'kJ', 'MJ', 'GJ', 'Btu', 'kWh', 'MWh'}
            Convert default 'Energy' outputs to requested units
         timestamp_format : str
            Specified str format of a datetime timestamp.
         ignore_peaks : bool, default: True
            Ignore peak values from 'Daily'+ intervals.

     Returns
     -------
     pandas.DataFrame
         Results for requested variables.
    """
    kwargs = {
        "start_date": start_date,
        "end_date": end_date,
        "output_type": output_type,
        "add_file_name": add_file_name,
        "include_interval": include_interval,
        "include_id": include_id,
        "include_day": include_day,
        "units_system": units_system,
        "rate_to_energy_dct": rate_to_energy_dct,
        "rate_units": rate_units,
        "energy_units": energy_units,
        "timestamp_format": timestamp_format,
        "part_match": part_match,
        "ignore_peaks": ignore_peaks,
    }

    if isinstance(files, list):
        return _get_results_multiple_files(files, variables, **kwargs)

    return _get_results(files, variables, **kwargs)


def _get_results(file, variables, **kwargs):
    """ Load eso file and return requested results. """
    ignore_peaks = kwargs.pop("ignore_peaks")

    if issubclass(file.__class__, BaseFile):
        eso_file = file
    else:
        eso_file = EsoFile(file, ignore_peaks=ignore_peaks)

    return eso_file.get_results(variables, **kwargs)


def _get_results_multiple_files(file_list, variables, **kwargs):
    """ Extract results from multiple files. """
    frames = []
    for file in file_list:
        df = _get_results(file, variables, **kwargs)
        if df is not None:
            frames.append(df)
    try:
        res = pd.concat(frames, axis=1, sort=False)

    except ValueError:
        if isinstance(variables, list):
            rstr = ", ".join(["'{} {} {} {}'".format(*tup) for tup in variables])
        else:
            rstr = variables

        logging.warning(
            f"Any of requested variables was not found!\n" f"Requested variables: [{rstr}]"
        )
        return

    return res
