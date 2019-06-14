import pandas as pd
import os
import time

from random import randint

from eso_reader.convertor import rate_to_energy, energy_to_rate, convert
from eso_reader.eso_processor import read_file
from eso_reader.mini_classes import HeaderVariable
from eso_reader.constants import MIN_DATE, MAX_DATE, RATE_TO_ENERGY_DCT


class VariableNotFound(Exception):
    """ Exception raised when requested variable id is not available. """
    pass


class NoResults(Exception):
    """ Exception raised when results are requested from an incomplete file. """
    pass


class InvalidFileNamePosition(Exception):
    """ Exception raised when incorrect file position on results df is requested. """
    pass


class IncompleteFile(Exception):
    """ Exception raised when the file is not complete. """
    pass


def load_eso_file(path, monitor=None, report_progress=True, suppress_errors=False):
    """ A wrapper to safely handle a file. """
    eso_file = EsoFile(path,
                       monitor=monitor,
                       report_progress=report_progress,
                       suppress_errors=suppress_errors)

    if eso_file.complete:
        return eso_file

    else:
        raise IncompleteFile("Unexpected end of the file reached!\n"
                             "File '{}' is not complete.".format(eso_file.file_name))


def get_results(files, request, start_date=MIN_DATE, end_date=MAX_DATE, type="standard",
                header=True, add_file_name="row", include_interval=False, units_system="SI",
                energy_rate_dct=RATE_TO_ENERGY_DCT, rate_units="W", energy_units="J",
                timestamp_format="default", report_progress=True, exclude_intervals=None,
                part_match=False):
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
     request : Variable or list of (Variable)
        Requested output variable or variables. A mini class  'Variable' needs
        to be used to find results.
     start_date : datetime like object, default 'MIN_DATE' constant
         A start date for requested results.
     end_date : datetime like object, default 'MAX_DATE' constant
         An end date for requested results.
     type : {'standard', 'local_max',' global_max', 'timestep_max',
             'local_min', 'global_min', 'timestep_min'}
         Requested type of results.
     header : bool
         Include full E+ header information in multi index if set to True.
     add_file_name : ('row','column',None)
         Specify if file name should be added into results df.
     include_interval : bool
         Decide if 'interval' information should be included on
         the results df column index.
     units_system : {'SI', 'IP'}
         Selected units type for requested outputs.
     energy_rate_dct : dct
         Defines if 'energy' or 'rate' will be reported for a specified interval
     rate_units : {'W', 'kW', 'MW', 'Btu/h', 'kBtu/h'}
         Convert default 'Rate' outputs to requested units.
     energy_units : {'J', 'kJ', 'MJ', 'GJ', 'Btu', 'kWh', 'MWh'}
         Convert default 'Energy' outputs to requested units
     timestamp_format : str
         A format of timestamp for peak results, currently only used for ASHRAE
         140 as these need separate date and time column
     report_progress : bool
         Processing progress will be reported in the terminal if set to 'True'.
     exclude_intervals : list of {TS, H, D, M, A, RP}
         A list of interval identifiers which will be ignored.
     part_match : bool
         Only substring of the part of variable is enough
         to match when searching for variables if this is True.

     Returns
     -------
     pandas.DataFrame
         Results for requested variables.
    """
    kwargs = {
        "start_date": start_date,
        "end_date": end_date,
        "type": type,
        "header": header,
        "add_file_name": add_file_name,
        "include_interval": include_interval,
        "units_system": units_system,
        "energy_rate_dct": energy_rate_dct,
        "rate_units": rate_units,
        "energy_units": energy_units,
        "timestamp_format": timestamp_format,
        "report_progress": report_progress,
        "exclude_intervals": exclude_intervals,
        "part_match": part_match
    }

    if isinstance(files, list):
        return _get_results_multiple_files(files, request, **kwargs)

    return _get_results(files, request, **kwargs)


def _get_results(file, request, **kwargs):
    """ Load eso file and return requested results. """
    excl = kwargs.pop("exclude_intervals")
    report_progress = kwargs.pop("report_progress")
    part_match = kwargs.pop("part_match")

    if isinstance(file, EsoFile):
        eso_file = file
    else:
        eso_file = EsoFile(file, exclude_intervals=excl, report_progress=report_progress)

    if not eso_file.complete:
        raise NoResults("Cannot load results!\n"
                        "File '{}' is not complete.".format(eso_file.file_name))

    ids = eso_file.find_ids(request, part_match=part_match)

    df = eso_file.results_df(*ids, **kwargs)
    return df


def _get_results_multiple_files(file_list, request, **kwargs):
    """ Extract results from multiple files. """
    frames = []
    for file in file_list:
        df = _get_results(file, request, **kwargs)

        if df is not None:
            frames.append(df)
    try:
        res = pd.concat(frames, sort=False)

    except ValueError:

        if isinstance(request, list):
            lst = ["'{} - {} {} {}'".format(*tup) for tup in request]
            request_str = ", ".join(lst)
        else:
            request_str = request

        print("Any of requested variables was not found!\n"
              "Requested variables: '{}'\n"
              "Files: '{}'".format(request_str, ", ".join(file_list)))
        return

    return res


class EsoFile:
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

    """

    def __init__(self, file_path, exclude_intervals=None, monitor=None, report_progress=True):
        self.file_path = file_path
        self._complete = False

        self.file_timestamp = None
        self.environments = None
        self.header_dct = None
        self.outputs_dct = None
        self.header_tree = None

        self.populate_content(exclude_intervals=exclude_intervals,
                              monitor=monitor,
                              report_progress=report_progress)

    def __repr__(self):
        human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.created))
        return "File: {}\n" \
               "Path: {}\n" \
               "Created: {}".format(self.file_name, self.file_path, human_time)

    @property
    def file_name(self):
        """ Return eso file name without suffix. """
        bsnm = os.path.basename(self.file_path)
        return os.path.splitext(bsnm)[0]

    @property
    def inverted_header_dct(self):
        """ Header dict where items have ID and (key name, variable name, units) switched. """
        return {key: {v: k for k, v in val.items()} for key, val in self.header_dct.items()}

    @property
    def available_intervals(self):
        """ Return a list of available intervals. """
        return self.header_dct.keys()

    @property
    def all_ids(self):
        """ Return a list of all ids (regardless the interval). """
        ids = []
        for interval, data in self.header_dct.items():
            keys = data.keys()
            ids.extend(keys)
        return ids

    @property
    def modified(self):
        """ Return a timestamp of the last file system modification. """
        path = self.file_path
        return os.path.getmtime(path)

    @property
    def created(self):
        """ Return a timestamp of the file system creation. """
        path = self.file_path
        return os.path.getctime(path)

    def populate_content(self, exclude_intervals=None, monitor=None, report_progress=True):
        """ Process the eso file to populate attributes. """
        content = read_file(
            self.file_path,
            exclude_intervals=exclude_intervals,
            monitor=monitor,
            report_progress=report_progress,
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

    def _search(self, var_id):
        """ Return an interval and data set containing the given variable. """
        for interval, data_set in self.outputs_dct.items():
            if var_id in data_set.columns:
                return interval, data_set
        else:
            VariableNotFound("Eso file does not contain variable id {}!".format(var_id))

    def _search_header(self, var_id):
        """ Return a data set containing given variable. """
        for data_set in self.header_dct.values():
            if var_id in data_set.keys():
                return data_set[var_id]
        else:
            VariableNotFound("Eso file does not contain variable id {}!".format(var_id))

    def results_df(
            self, *args, start_date=MIN_DATE, end_date=MAX_DATE,
            type="standard", header=True, add_file_name="row", include_interval=False,
            units_system="SI", rate_to_energy_dct=RATE_TO_ENERGY_DCT, rate_units="W",
            energy_units="J", timestamp_format="default"
    ):
        """
        Return a pandas.DataFrame object with results for given variables.

        This function extracts requested set of outputs from the eso file
        and converts to specified units if requested.

        Parameters
        ----------
        *args : int
            Variable ID or IDs.
        start_date : datetime like object, default 'MIN_DATE' constant
            A start date for requested results.
        end_date : datetime like object, default 'MAX_DATE' constant
            An end date for requested results.
        type : {
                'standard', 'local_max',' global_max', 'timestep_max',
                'local_min', 'global_min', 'timestep_min'
                }
            Requested type of results.
        header : bool
            Include full E+ header information in multi index if set to True.
        add_file_name : ('row','column',None)
            Specify if file name should be added into results df.
        include_interval : bool
            Decide if 'interval' information should be included on
            the results df.
        units_system : {'SI', 'IP'}
            Selected units type for requested outputs.
        rate_to_energy_dct : dct
            Defines if 'rate' will be converted to energy.
        rate_units : {'W', 'kW', 'MW', 'Btu/h', 'kBtu/h'}
            Convert default 'Rate' outputs to requested units.
        energy_units : {'J', 'kJ', 'MJ', 'GJ', 'Btu', 'kWh', 'MWh'}
            Convert default 'Energy' outputs to requested units
        timestamp_format : str
            A format of timestamp for peak results, currently only
            used for ASHRAE 140 as these need separate date and
            time column

        Returns
        -------
        pandas.DataFrame
            Results for requested variables.

        """
        frames = []

        for var_id in args:

            data_tup = self._search(var_id)

            if not data_tup:
                # Given 'id' was not found in the file
                continue

            interval, data_set = data_tup

            def standard():
                return data_set.standard_results(*args)

            def local_maxima():
                return data_set.local_maxima(*args)

            def global_maximum():
                return data_set.global_maximum(*args, tmstmp_frm=timestamp_format)

            def timestep_maximum():
                return data_set.timestep_maximum(*args, tmstmp_frm=timestamp_format)

            def local_minima():
                return data_set.local_minima(*args)

            def global_minimum():
                return data_set.global_minimum(*args, tmstmp_frm=timestamp_format)

            def timestep_minimum():
                return data_set.timestep_minimum(*args, tmstmp_frm=timestamp_format)

            res = {
                "standard": standard,
                "local_max": local_maxima,
                "global_max": global_maximum,
                "timestep_max": timestep_maximum,
                "local_min": local_minima,
                "global_min": global_minimum,
                "timestep_min": timestep_minimum,
            }

            # Find matching header information
            header_data = self._search_header(var_id)

            # Extract specified set of results
            args = var_id, start_date, end_date, header_data
            data = res[type]()

            # Some types of outputs are not applicable for
            # certain intervals so these will be ignored
            if data is None:
                continue

            # convert 'rate' or 'energy' when standard results are requested
            if type == "standard" and rate_to_energy_dct:
                is_energy = rate_to_energy_dct[interval]
                if is_energy:
                    # 'energy' is requested for current output
                    data = rate_to_energy(data, data_set, start_date, end_date)

            # Convert the data if units system, rate or energy
            # units are not default
            if units_system != "SI" or rate_units != "W" or energy_units != "J":
                data = convert(data, units_system, rate_units, energy_units)

            # Remove 'key', 'variable' and 'units' from multi index
            if not header:
                data.columns = self.drop_header_levels(data.columns)

            else:
                # Drop only variable id (as index is defined using key, var and units).
                data.columns = data.columns.droplevel(0)

                if include_interval:
                    data = pd.concat([data], axis=1, keys=[interval], names=["interval"])

            frames.append(data)

        # Catch empty frames exception
        try:
            # Merge dfs
            results = pd.concat(frames, axis=1)
            # Add file name to the index
            if add_file_name:
                results = self.add_file_name(results, add_file_name)
            return results

        except ValueError:
            # raise ValueError("Any of requested variables is not included in the Eso file.")
            print("Any of requested variables is not "
                  "included in the Eso file '{}'.".format(self.file_name))

    def add_file_name(self, results, name_position):
        """ Add file name to index. """
        if name_position not in {"row", "column", None}:
            raise InvalidFileNamePosition("Invalid name position!\n"
                                          "'add_file_name' kwarg must be one of: "
                                          "'row', 'column' or None.")

        axis = 0 if name_position == "row" else 1
        return pd.concat([results], axis=axis, keys=[self.file_name], names=["file"])

    @classmethod
    def drop_header_levels(cls, columns):
        """ Exclude key, var and units from column index. """
        end = 3 if columns.nlevels == 4 else 4
        for _ in range(end):
            columns = columns.droplevel(1)
        return columns

    def find_ids(self, request_lst, part_match=False):
        """
        Find variable ids for a list of 'Requests'.

        Parameters
        ----------
        request_lst : list of Request
            A list of 'Request' named tuples.
        part_match : bool
            Only substring of the part of variable is enough
            to match when searching for variables if this is True.
        """
        ids = []

        if not isinstance(request_lst, list):
            request_lst = [request_lst]

        for request in request_lst:
            interval, key, var, units = request
            ids.extend(self._find_ids(interval=interval, key_name=key, var_name=var,
                                      units=units, part_match=part_match))
        return ids

    def _find_ids(self, interval=None, key_name=None, var_name=None, units=None, part_match=False):
        """ Find variable id or ids for given identifiers. """
        ids = self.header_tree.search(interval=interval, key=key_name, var=var_name,
                                      units=units, part_match=part_match)
        if not ids:
            print("File: {}".format(self.file_path))

        return ids

    @staticmethod
    def _generate_id():
        """ ID generator. """
        while True:
            yield -randint(1, 999999)

    def _id_generator(self):
        """ Generate a unique id for custom variable. """
        gen = self._generate_id()
        while True:
            id = next(gen)
            if id not in self.all_ids:
                return id

    def _is_variable_unique(self, variable, interval):
        """ Check if the variable is included in a given interval. """
        is_unique = variable not in self.inverted_header_dct[interval]
        return is_unique

    @staticmethod
    def _add_underscore(variable):
        """ Create a new variable with added '_' in key. """
        key, variable, units = variable
        new_key = "_" + key
        return new_key, variable, units

    def _create_variable(self, interval, variable):
        """ Validate a new variable. """
        while True:
            # Underscores will be added into a 'key' until the variable name is unique
            is_unique = self._is_variable_unique(variable, interval)

            if is_unique:
                # Variable is unique, return standard header variable
                header_variable = HeaderVariable(*variable)
                return header_variable

            variable = self._add_underscore(variable)

    @staticmethod
    def remove_header_variable(id, header_dct):
        """ Remove header variable from header. """
        try:
            del header_dct[id]
        except KeyError:
            print("Cannot remove id: {} from header.\n"
                  "Given id is not valid.")

    def add_output(self, request_tuple, array):
        """ Add specified output variable to the file. """
        interval, key, var, units = request_tuple

        if interval not in self.available_intervals:
            print("Cannot add variable: '{} : {} : {}' into outputs.\n"
                  "Interval is not included in file '{}'".format(key, var, units, self.file_name))
            return

        variable = key, var, units
        header_dct = self.header_dct[interval]
        # Generate a unique identifier
        # Note that custom ids use '-' sign
        id = self._id_generator()

        # Add variable identifiers to the header
        header_variable = self._create_variable(interval, variable)
        header_dct[id] = header_variable

        # Add variable data to the output df
        outputs = self.outputs_dct[interval]
        is_valid = outputs.add_output(id, array)

        if is_valid:
            # Variable can be added, create a reference in the search tree
            self.header_tree.add_branch(interval, key, var, units, id)
        else:
            # Revert header dict in its original state
            self.remove_header_variable(id, header_dct)
