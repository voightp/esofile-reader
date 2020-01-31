import os
from datetime import datetime
from typing import Type, List

import pandas as pd

from esofile_reader.base_file import BaseFile, IncompleteFile
from esofile_reader.diff_file import DiffFile
from esofile_reader.processing.monitor import DefaultMonitor
from esofile_reader.totals_file import TotalsFile
from esofile_reader.utils.mini_classes import Variable, ResultsFile

try:
    from esofile_reader.processing.esofile_processor import read_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install()
    from esofile_reader.processing.esofile_processor import read_file


class PeaksNotIncluded(Exception):
    """ Exception is raised when EsoFile has been processed without peaks. """
    pass


class EsoFile(BaseFile):
    """
    The ESO class holds processed EnergyPlus output ESO file data.

    A structure for data bins is works as for 'BaseFile'.

    Attributes
    ----------
    file_path : str
        A full path of the ESO file.
    file_created : datetime.datetime
        Time and date when the ESO file has been generated (extracted from original Eso file).
    storage : {DFOutputs, SQLOutputs}
        A class to store resutls data
        {period : {ID : (key name, variable name, units)}}

    Parameters
    ----------
    file_path : path like object
        A full path of the ESO file
    report_progress : bool, default True
        Processing progress is reported in terminal when set as 'True'.
    ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.

    Raises
    ------
    IncompleteFile

    """

    def __init__(self, file_path: str, monitor: Type[DefaultMonitor] = None,
                 report_progress=True, ignore_peaks: bool = True, year: int = 2002):
        super().__init__()
        self.file_path = file_path
        self.peak_outputs = None
        self.populate_content(monitor=monitor,
                              report_progress=report_progress,
                              ignore_peaks=ignore_peaks,
                              year=year)

    def populate_content(self, monitor: Type[DefaultMonitor] = None, report_progress: bool = True,
                         ignore_peaks: bool = True, year: int = 2002):
        """ Process the eso file to populate attributes. """
        self.file_name = os.path.splitext(os.path.basename(self.file_path))[0]
        self.file_created = datetime.utcfromtimestamp(os.path.getctime(self.file_path))

        content = read_file(
            self.file_path,
            monitor=monitor,
            report_progress=report_progress,
            ignore_peaks=ignore_peaks,
            year=year
        )

        if content:
            (
                self.storage,
                self.peak_outputs,
                self._search_tree,
            ) = content

        else:
            raise IncompleteFile(f"Unexpected end of the file reached!\n"
                                 f"File '{self.file_path}' is not complete.")

    def _get_peak_results(self, variables: List[Variable], output_type: str,
                          start_date: datetime = None, end_date: datetime = None,
                          add_file_name: str = "row", include_interval: bool = False,
                          include_id: bool = False, part_match: bool = False,
                          timestamp_format: str = "default") -> pd.DataFrame:
        """ Return local peak results. """
        frames = []
        groups = self._find_pairs(variables, part_match=part_match)

        for interval, ids in groups.items():
            try:
                df = self.peak_outputs[output_type].get_results(interval, ids, start_date, end_date)
            except KeyError:
                print(f"There are no peak outputs stored for interval: '{interval}'.")
                continue

            if not include_id:
                df.columns = df.columns.droplevel("id")

            if not include_interval:
                df.columns = df.columns.droplevel("interval")

            frames.append(df)

        return self._merge_frame(frames, timestamp_format, add_file_name)

    def get_results(self, variables: List[Variable], output_type: str = "standard",
                    **kwargs) -> pd.DataFrame:
        """
        Return a pandas.DataFrame object with results for given variables.

        This function extracts requested set of outputs from the file
        and converts to specified units if requested.

        Parameters
        ----------
        variables : Variable or list of (Variable)
            Requested variables..
        output_type : {'standard', global_max','global_min', 'local_max', 'local_min'}
                Requested type of results.

        **kwargs
            start_date : datetime like object, default None
                A start date for requested results.
            end_date : datetime like object, default None
                An end date for requested results.
            add_file_name : ('row','column',None)
                Specify if file name should be added into results df.
            include_interval : bool
                Decide if 'interval' information should be included on
                the results df.
            include_day : bool
                Add day of week into index, this is applicable only for 'timestep',
                'hourly' and 'daily' outputs.
            include_id : bool
                Decide if variable 'id' should be included on the results df.
            part_match : bool
                Only substring of the part of variable is enough
                to match when searching for variables if this is True.
            units_system : {'SI', 'IP'}
                Selected units type for requested outputs.
            rate_to_energy_dct : dct
                Defines if 'rate' will be converted to energy.
            rate_units : {'W', 'kW', 'MW', 'Btu/h', 'kBtu/h'}
                Convert default 'Rate' outputs to requested units.
            energy_units : {'J', 'kJ', 'MJ', 'GJ', 'Btu', 'kWh', 'MWh'}
                Convert default 'Energy' outputs to requested units
            timestamp_format : str
                Specified str format of a datetime timestamp.

        Returns
        -------
        pandas.DataFrame
            Results for requested variables.

        """
        if output_type in ["local_max", "local_min"]:
            if self.peak_outputs:
                ignore = ["units_system", "rate_to_energy_dct",
                          "rate_units", "energy_units", "include_day"]
                kwargs = {k: v for k, v in kwargs.items() if k not in ignore}
                df = self._get_peak_results(variables, output_type, **kwargs)

            else:
                raise PeaksNotIncluded("Peak values are not included, it's "
                                       "required to add kwarg 'ignore_peaks=False' "
                                       "when processing the file.")
        else:
            df = super().get_results(variables, output_type=output_type, **kwargs)

        return df

    def generate_totals(self):
        """ Generate 'Totals' file. """
        return TotalsFile(self)

    def generate_diff(self, other_file: ResultsFile):
        """ Generate 'Diff' file. """
        return DiffFile(self, other_file)
