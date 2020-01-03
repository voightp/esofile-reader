import pandas as pd
import os

from esofile_reader.base_file import BaseResultsFile, InvalidOutputType
from esofile_reader.outputs.convertor import rate_to_energy, convert_units
from esofile_reader.processing.esofile_processor import read_file
from esofile_reader.constants import RATE_TO_ENERGY_DCT
from esofile_reader.totals_file import TotalsFile


class NoResults(Exception):
    """ Exception raised when results are requested from an incomplete file. """
    pass


class IncompleteFile(Exception):
    """ Exception raised when the file is not complete. """
    pass


class PeaksNotIncluded(Exception):
    """ Exception is raised when EsoFile has been processed without peaks. """
    # PeaksNotIncluded("Peak values are not included, it's required to "
    #                  "add kwarg 'ignore_peaks=False' when processing the file."
    #                  "\nNote that peak values are only applicable for"
    #                  "raw Eso files.")
    pass


def get_results(files, variables, start_date=None, end_date=None, output_type="standard",
                add_file_name="row", include_interval=False, include_id=False,
                units_system="SI", rate_to_energy_dct=RATE_TO_ENERGY_DCT, rate_units="W",
                energy_units="J", timestamp_format="default", report_progress=True,
                part_match=False, ignore_peaks=True, suppress_errors=False):
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
        Requested type of results.
     add_file_name : ('row','column',None)
        Specify if file name should be added into results df.
     include_interval : bool
        Decide if 'interval' information should be included on
        the results df.
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
     report_progress : bool, default True
        Processing progress is reported in terminal when set as 'True'.
     ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.
     suppress_errors: bool, default False
        Do not raise IncompleteFile exceptions when processing fails

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
        "units_system": units_system,
        "rate_to_energy_dct": rate_to_energy_dct,
        "rate_units": rate_units,
        "energy_units": energy_units,
        "timestamp_format": timestamp_format,
        "report_progress": report_progress,
        "part_match": part_match,
        "ignore_peaks": ignore_peaks,
        "suppress_errors": suppress_errors,
    }

    if isinstance(files, list):
        return _get_results_multiple_files(files, variables, **kwargs)

    return _get_results(files, variables, **kwargs)


def _get_results(file, variables, **kwargs):
    """ Load eso file and return requested results. """
    report_progress = kwargs.pop("report_progress")
    ignore_peaks = kwargs.pop("ignore_peaks")
    suppress_errors = kwargs.pop("suppress_errors")

    if issubclass(file.__class__, BaseResultsFile):
        eso_file = file
    else:
        eso_file = EsoFile(file,
                           ignore_peaks=ignore_peaks,
                           report_progress=report_progress,
                           suppress_errors=suppress_errors)

    if not eso_file.complete:
        msg = f"Cannot load results!\nFile '{eso_file.file_name}' is not complete."
        if not suppress_errors:
            raise NoResults(msg)
        else:
            print(msg)
            return

    return eso_file.results_df(variables, **kwargs)


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
            lst = ["'{} - {} {} {}'".format(*tup) for tup in variables]
            request_str = ", ".join(lst)
        else:
            request_str = variables

        print("Any of requested variables was not found!\n"
              "Requested variables: '{}'\n"
              "Files: '{}'".format(request_str, ", ".join(file_list)))
        return

    return res


class EsoFile(BaseResultsFile):
    """
    The ESO class holds processed EnergyPlus output ESO file data.

    The results are stored in a dictionary using string interval identifiers
    as keys and pandas.DataFrame like classes as values.

    A structure for line bins is as follows:
    header_dict = {
        TS : {(int)ID : ('Key','Variable','Units')},
        H : {(int)ID : ('Key','Variable','Units')},
        D : {(int)ID : ('Key','Variable','Units')},
        M : {(int)ID : ('Key','Variable','Units')},
        A : {(int)ID : ('Key','Variable','Units')},
        RP : {(int)ID : ('Key','Variable','Units')},
    }

    outputs = {
        TS : outputs.Timestep,
        H : outputs.Hourly,
        D : outputs.Daily,
        M : outputs.Monthly,
        A : outputs.Annual,
        RP : outputs.Runperiod,
    }

    Attributes
    ----------
    file_path : str
        A full path of the ESO file.
    file_timestamp : datetime.datetime
        Time and date when the ESO file has been generated (extracted from original Eso file).
    header : dict of {str : dict of {int : list of str}}
        A dictionary to store E+ header line
        {period : {ID : (key name, variable name, units)}}
    outputs : dict of {str : Outputs subclass}
        A dictionary holding categorized outputs using pandas.DataFrame like classes.

    Parameters
    ----------
    file_path : path like object
        A full path of the ESO file
    report_progress : bool, default True
        Processing progress is reported in terminal when set as 'True'.
    ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.
    suppress_errors: bool, default False
        Do not raise IncompleteFile exceptions when processing fails

    Raises
    ------
    IncompleteFile
    InvalidOutputType

    """

    def __init__(self, file_path, monitor=None, report_progress=True,
                 ignore_peaks=True, suppress_errors=False):
        super().__init__()
        self.file_path = file_path
        self.peak_outputs = None
        self.populate_content(monitor=monitor,
                              report_progress=report_progress,
                              ignore_peaks=ignore_peaks,
                              suppress_errors=suppress_errors)

    def populate_content(self, monitor=None, report_progress=True,
                         ignore_peaks=True, suppress_errors=False):
        """ Process the eso file to populate attributes. """
        self.file_name = os.path.splitext(os.path.basename(self.file_path))[0]

        content = read_file(
            self.file_path,
            monitor=monitor,
            report_progress=report_progress,
            ignore_peaks=ignore_peaks,
            suppress_errors=suppress_errors
        )

        if content:
            self._complete = True
            (
                self.file_timestamp,
                self.environments,
                self.header,
                self.outputs,
                self.peak_outputs,
                self.header_tree,
            ) = content

        else:
            if not suppress_errors:
                raise IncompleteFile(f"Unexpected end of the file reached!\n"
                                     f"File '{self.file_path}' is not complete.")

    def get_totals(self):
        """ Generate a new 'Building' eso file. """
        if self.complete:
            return TotalsFile(self)
        else:
            raise IncompleteFile(f"Cannot generate building totals, "
                                 f"file {self.file_path} is not complete!")
