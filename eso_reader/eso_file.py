import pandas as pd

from eso_reader.performance import perf
from eso_reader.base_eso_file import BaseEsoFile
from eso_reader.convertor import rate_to_energy, convert_units
from eso_reader.eso_processor import read_file
from eso_reader.constants import RATE_TO_ENERGY_DCT
from eso_reader.building_eso_file import BuildingEsoFile


class NoResults(Exception):
    """ Exception raised when results are requested from an incomplete file. """
    pass


class IncompleteFile(Exception):
    """ Exception raised when the file is not complete. """
    pass


class InvalidOutputType(Exception):
    """ Exception raised when the output time is invalid. """
    pass


def get_results(files, variables, start_date=None, end_date=None, output_type="standard",
                add_file_name="row", include_interval=False, units_system="SI",
                rate_to_energy_dct=RATE_TO_ENERGY_DCT, rate_units="W", energy_units="J",
                timestamp_format="default", report_progress=True, exclude_intervals=None,
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
     exclude_intervals : list of {TS, H, D, M, A, RP}
        A list of interval identifiers which will be ignored. This can
        be used to avoid processing hourly, sub-hourly intervals.
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
        "units_system": units_system,
        "rate_to_energy_dct": rate_to_energy_dct,
        "rate_units": rate_units,
        "energy_units": energy_units,
        "timestamp_format": timestamp_format,
        "report_progress": report_progress,
        "exclude_intervals": exclude_intervals,
        "part_match": part_match,
        "ignore_peaks": ignore_peaks,
        "suppress_errors": suppress_errors,
    }

    if isinstance(files, list):
        return _get_results_multiple_files(files, variables, **kwargs)

    return _get_results(files, variables, **kwargs)


def _get_results(file, variables, **kwargs):
    """ Load eso file and return requested results. """
    excl = kwargs.pop("exclude_intervals")
    report_progress = kwargs.pop("report_progress")
    ignore_peaks = kwargs.pop("ignore_peaks")
    suppress_errors = kwargs.pop("suppress_errors")

    if isinstance(file, EsoFile):
        eso_file = file
    else:
        eso_file = EsoFile(file, exclude_intervals=excl,
                           ignore_peaks=ignore_peaks,
                           report_progress=report_progress,
                           suppress_errors=suppress_errors)

    if not eso_file.complete:
        raise NoResults("Cannot load results!\n"
                        "File '{}' is not complete.".format(eso_file.file_name))

    df = eso_file.results_df(variables, **kwargs)

    return df


def _get_results_multiple_files(file_list, variables, **kwargs):
    """ Extract results from multiple files. """
    frames = []
    for file in file_list:
        df = _get_results(file, variables, **kwargs)
        if df is not None:
            frames.append(df)
    try:
        res = pd.concat(frames, sort=False)

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


class EsoFile(BaseEsoFile):
    """
    The ESO class holds processed EnergyPlus output ESO file data.

    The results are stored in a dictionary using string interval identifiers
    as keys and pandas.DataFrame like classes as values.

    A structure for data bins is as follows:
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
    header_dct : dict of {str : dict of {int : list of str}}
        A dictionary to store E+ header data
        {period : {ID : (key name, variable name, units)}}
    outputs_dct : dict of {str : Outputs subclass}
        A dictionary holding categorized outputs using pandas.DataFrame like classes.

    Parameters
    ----------
    file_path : path like object
        A full path of the ESO file
    exclude_intervals : list of {TS, H, D, M, A, RP}
        A list of interval identifiers which will be ignored. This can
        be used to avoid processing hourly, sub-hourly intervals.
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

    def __init__(self, file_path, exclude_intervals=None, monitor=None, report_progress=True,
                 ignore_peaks=True, suppress_errors=False):
        super().__init__()
        self.file_path = file_path
        self.populate_content(exclude_intervals=exclude_intervals,
                              monitor=monitor,
                              report_progress=report_progress,
                              ignore_peaks=ignore_peaks,
                              suppress_errors=suppress_errors)

    def populate_content(self, exclude_intervals=None, monitor=None, report_progress=True,
                         ignore_peaks=True, suppress_errors=False):
        """ Process the eso file to populate attributes. """
        content = read_file(
            self.file_path,
            exclude_intervals=exclude_intervals,
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
                self.header_dct,
                self.outputs_dct,
                self.header_tree,
            ) = content

        else:
            if not suppress_errors:
                raise IncompleteFile("Unexpected end of the file reached!\n"
                                     "File '{}' is not complete.".format(self.file_path))

    @perf
    def results_df(
            self, variables, start_date=None, end_date=None,
            output_type="standard", add_file_name="row", include_interval=False, part_match=False,
            units_system="SI", rate_to_energy_dct=RATE_TO_ENERGY_DCT, rate_units="W",
            energy_units="J", timestamp_format="default"
    ):
        """
        Return a pandas.DataFrame object with results for given variables.

        This function extracts requested set of outputs from the eso file
        and converts to specified units if requested.

        Parameters
        ----------
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

        def standard():
            return data_set.standard_results(*f_args)

        def local_maxs():
            return data_set.local_maxs(*f_args)

        def global_max():
            return data_set.global_max(*f_args)

        def timestep_max():
            return data_set.timestep_max(*f_args)

        def local_mins():
            return data_set.local_mins(*f_args)

        def global_min():
            return data_set.global_min(*f_args)

        def timestep_min():
            return data_set.timestep_min(*f_args)

        res = {
            "standard": standard,
            "local_max": local_maxs,
            "global_max": global_max,
            "timestep_max": timestep_max,
            "local_min": local_mins,
            "global_min": global_min,
            "timestep_min": timestep_min,
        }

        if output_type not in res:
            msg = "Invalid output type '{}' requested.\n'output_type'" \
                  "kwarg must be one of '{}'.".format(output_type, ", ".join(res.keys()))
            raise InvalidOutputType(msg)

        frames = []
        ids = self.find_ids(variables, part_match=part_match)
        groups = self.categorize_ids(ids)

        for interval, ids in groups.items():
            data_set = self.outputs_dct[interval]

            # Extract specified set of results
            f_args = (ids, start_date, end_date)

            df = res[output_type]()

            if df is None:
                print("Results type '{}' is not applicable for '{}' interval."
                      "\n\tignoring the request...".format(type, interval))
                continue

            df = self.add_header_data(interval, df)

            # convert 'rate' or 'energy' when standard results are requested
            if output_type == "standard" and rate_to_energy_dct:
                is_energy = rate_to_energy_dct[interval]
                if is_energy:
                    # 'energy' is requested for current output
                    df = rate_to_energy(df, data_set, start_date, end_date)

            if units_system != "SI" or rate_units != "W" or energy_units != "J":
                df = convert_units(df, units_system, rate_units, energy_units)

            if include_interval:
                df = pd.concat([df], axis=1, keys=[interval], names=["interval"])

            frames.append(df)

        # Catch empty frames exception
        try:
            # Merge dfs
            df = pd.concat(frames, axis=1, sort=False)
            # Add file name to the index
            if timestamp_format != "default":
                df = self.update_dt_format(df, output_type, timestamp_format)
            if add_file_name:
                df = self.add_file_name(df, add_file_name)
            return df

        except ValueError:
            # raise ValueError("Any of requested variables is not included in the Eso file.")
            print("Any of requested variables is not "
                  "included in the Eso file '{}'.".format(self.file_name))

    def get_building_totals(self):
        """ Generate a new 'Building' eso file. """
        return BuildingEsoFile(self)
