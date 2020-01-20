import pandas as pd

from random import randint
from datetime import datetime
from esofile_reader.outputs.convertor import rate_and_energy_units, convert_rate_to_energy, convert_units
from esofile_reader.outputs.outputs import Outputs
from esofile_reader.processing.interval_processor import update_dt_format
from esofile_reader.constants import *
from esofile_reader.utils.mini_classes import Variable
from typing import List, Dict, Union, Tuple, Sequence, Callable


class VariableNotFound(Exception):
    """ Exception raised when requested variable id is not available. """
    pass


class InvalidOutputType(Exception):
    """ Exception raised when the output time is invalid. """
    pass


class IncompleteFile(Exception):
    """ Exception raised when the file is not complete. """
    pass


class CannotAggregateVariables(Exception):
    """ Exception raised when variables cannot be aggregated. """
    pass


def gen_id(checklist, negative=True):
    """ ID generator. """
    while True:
        i = randint(1, 999999)
        i = -i if negative else i
        if i not in checklist:
            return -randint(1, 999999)


class BaseFile:
    """
    The AbstractEsoFile class works as a base for a 'physical' eso file and
    totals file.

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
        TS : outputs.Outputs,
        H : outputs.Outputs,
        D : outputs.Outputs,
        M : outputs.Outputs,
        A : outputs.Outputs,
        RP : outputs.Outputs,
    }

    Attributes
    ----------
    file_path : str
        A full path of the ESO file.
    file_timestamp : datetime.datetime
        Time and date when the ESO file has been generated (extracted from original Eso file).
    header : dict of {str : dict of {int : list of str}}
        A dictionary to store E+ header data
        {period : {ID : (key name, variable name, units)}}
    _outputs : dict of {str : Outputs subclass}
        A dictionary holding categorized outputs using pandas.DataFrame like classes.

    Notes
    -----
    This class cannot be used directly!

    Method 'populate_data' is only a signature method so attributes cannot be populated.

    """

    def __init__(self):
        self.file_path = None
        self.file_name = None
        self._complete = False
        self._outputs = None

        self.file_timestamp = None
        self._search_tree = None

    def __repr__(self):
        return f"File: {self.file_name}" \
            f"\nPath: {self.file_path}" \
            f"\nCreated: {self.created}"

    @property
    def available_intervals(self) -> List[str]:
        """ Return a list of available intervals. """
        return list(self._outputs.keys())

    @property
    def all_data_sets(self) -> List[Outputs]:
        """ Fetch all data sets. """
        return self._outputs.values()

    @property
    def all_ids(self) -> List[int]:
        """ Return a list of all ids (regardless the interval). """
        ids = []
        for data_set in self.all_data_sets:
            ids.extend(data_set.get_ids())
        return ids

    @property
    def created(self) -> datetime:
        """ Return a timestamp of the file system creation. """
        return datetime.fromtimestamp(self.file_timestamp)

    @property
    def complete(self) -> bool:
        """ Check if the file has been populated and complete. """
        return self._complete

    @property
    def header_df(self) -> pd.DataFrame:
        """ Get pd.DataFrame like header (index: mi(interval, id). """
        frames = []
        for data_set in self._outputs.values():
            frames.append(data_set.header_df)
        return pd.concat(frames)

    def data_set(self, interval: str) -> Outputs:
        """ Fetch data set for given interval. """
        try:
            return self._outputs[interval]
        except KeyError:
            raise KeyError

    def rename(self, name: str) -> None:
        """ Set a new file name. """
        self.file_name = name

    def populate_content(self, *args, **kwargs):
        """ Populate instance attributes. """
        pass

    def _add_file_name(self, df: pd.DataFrame, name_position: str) -> pd.DataFrame:
        """ Add file name to index. """
        pos = ["row", "column", "None"]  # 'None' is here only to inform
        if name_position not in pos:
            name_position = "row"
            print(f"Invalid name position!\n'add_file_name' kwarg must "
                  f"be one of: '{', '.join(pos)}'.\nSetting 'row'.")

        axis = 0 if name_position == "row" else 1

        return pd.concat([df], axis=axis, keys=[self.file_name], names=["file"])

    def _merge_frame(self, frames: List[pd.DataFrame], timestamp_format: str = "default",
                     add_file_name: str = "row") -> pd.DataFrame:
        """ Merge result DataFrames into a single one. """
        if frames:
            # Catch empty frames exception
            df = pd.concat(frames, axis=1, sort=False)

            if add_file_name:
                df = self._add_file_name(df, add_file_name)

            if timestamp_format != "default":
                df = update_dt_format(df, timestamp_format)

            return df
        else:
            print(f"Any of requested variables is not "
                  f"included in the Eso file '{self.file_name}'.")

    def find_ids(self, variables: Union[Variable, List[Variable]],
                 part_match: bool = False) -> List[int]:
        """ Find ids for a list of 'Variables'. """
        variables = variables if isinstance(variables, list) else [variables]
        out = []

        for request in variables:
            interval, key, var, units = [str(r) if isinstance(r, int) else r for r in request]
            ids = self._search_tree.get_ids(interval=interval, key=key, variable=var,
                                            units=units, part_match=part_match)
            if not ids:
                continue

            out.extend(ids)

        return out

    def _find_pairs(self, variables: Union[Variable, List[Variable]],
                    part_match: bool = False) -> Dict[str, List[int]]:
        """
        Find variable ids for a list of 'Variables'.

        Parameters
        ----------
        variables : list of Variable
            A list of 'Variable' named tuples.
        part_match : bool
            Only substring of the part of variable is enough
            to match when searching for variables if this is True.

        Returns
        -------
        dct
            A dictionary with 'intervals' as keys and lists of
            ids as values.

        """
        variables = variables if isinstance(variables, list) else [variables]
        out = {}

        for request in variables:
            interval, key, var, units = [str(r) if isinstance(r, int) else r for r in request]

            pairs = self._search_tree.get_pairs(interval=interval, key=key, variable=var,
                                                units=units, part_match=part_match)
            if not pairs:
                continue

            for k, v in pairs.items():
                if k in out.keys():
                    out[k].extend(pairs[k])
                else:
                    out[k] = pairs[k]

        return out

    def get_results(
            self, variables: Union[Variable, List[Variable]], start_date: datetime = None,
            end_date: datetime = None, output_type: str = "standard", add_file_name: str = "row",
            include_interval: bool = False, include_day: bool = False, include_id: bool = False,
            part_match: bool = False, units_system: str = "SI", rate_units: str = "W",
            energy_units: str = "J", timestamp_format: str = "default",
            rate_to_energy_dct: Dict[str, bool] = RATE_TO_ENERGY_DCT,
    ) -> pd.DataFrame:
        """
        Return a pandas.DataFrame object with results for given variables.

        This function extracts requested set of outputs from the file
        and converts to specified units if requested.

        Parameters
        ----------
        variables : Variable or list of (Variable)
            Requested variables..
        start_date : datetime like object, default None
            A start date for requested results.
        end_date : datetime like object, default None
            An end date for requested results.
        output_type : {'standard', global_max','global_min'}
            Requested type of results.
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

        def standard():
            return data_set.get_results(ids, start_date, end_date, include_day)

        def global_max():
            return data_set.global_max(ids, start_date, end_date)

        def global_min():
            return data_set.global_min(ids, start_date, end_date)

        res = {
            "standard": standard,
            "global_max": global_max,
            "global_min": global_min,
        }

        if output_type not in res:
            msg = f"Invalid output type '{output_type}' requested.\n'output_type'" \
                f"kwarg must be one of '{', '.join(res.keys())}'."
            raise InvalidOutputType(msg)

        frames = []
        groups = self._find_pairs(variables, part_match=part_match)

        for interval, ids in groups.items():
            data_set = self._outputs[interval]
            df = res[output_type]()

            if df is None:
                print(f"Results type '{output_type}' is not applicable for "
                      f"'{interval}' interval. \n\tignoring the request...")
                continue

            # convert 'rate' or 'energy' when standard results are requested
            if output_type == "standard" and rate_to_energy_dct[interval]:
                try:
                    n_days = data_set.get_number_of_days(start_date, end_date)
                except KeyError:
                    n_days = None

                df = convert_rate_to_energy(df, interval, n_days)

            if units_system != "SI" or rate_units != "W" or energy_units != "J":
                df = convert_units(df, units_system, rate_units, energy_units)

            if not include_id:
                df.columns = df.columns.droplevel("id")

            if not include_interval:
                df.columns = df.columns.droplevel("interval")

            frames.append(df)

        return self._merge_frame(frames, timestamp_format, add_file_name)

    def _new_header_variable(self, interval: str, key: str, var: str,
                             units: str) -> Variable:
        """ Create a unique header variable. """

        def add_num():
            new_key = f"{key} ({i})"
            return Variable(interval, new_key, var, units)

        variable = Variable(interval, key, var, units)

        i = 0

        while variable in self.data_set(interval).header_variables_dct.values():
            i += 1
            variable = add_num()

        return variable

    def rename_variable(self, variable: Variable, var_name: str = "",
                        key_name: str = "") -> Tuple[int, Variable]:
        """ Rename the given 'Variable' using given names. """
        ids = self.find_ids(variable)
        interval, key, var, units = variable

        if not var_name:
            var_name = var

        if not key_name:
            key_name = key

        if ids:
            # remove current item to avoid item duplicity
            self._search_tree.remove_variables([variable])

            # create new variable and add it into tree
            new_var = self._new_header_variable(interval, key_name, var_name, units)
            self._search_tree.add_variable(ids[0], new_var)

            # rename variable in data set
            self.data_set(interval).rename_variable(ids[0], new_var.key,
                                                    new_var.variable)
            return ids[0], new_var

    def add_output(self, interval: str, key_name: str, var_name: str, units: str,
                   array: Sequence) -> Tuple[int, Variable]:
        """ Add specified output variable to the file. """
        # generate a unique identifier, custom ids use '-' sign
        id_ = gen_id(self.all_ids, negative=True)

        # create new unique variable
        new_var = self._new_header_variable(interval, key_name, var_name, units)
        unique = self._search_tree.add_variable(id_, new_var)

        if unique:
            valid = self.data_set(interval).add_variable(id_, new_var, array)
            if valid:
                return id_, new_var
            else:
                self._search_tree.remove_variables(new_var)

    def aggregate_variables(self, variables: Union[Variable, List[Variable]],
                            func: Union[str, Callable], key_name: str = "Custom Key",
                            var_name: str = "Custom Variable",
                            part_match: bool = False) -> Tuple[int, Variable]:
        """
        Aggregate given variables using given function.

        A new 'Variable' with specified key and variable names
        will be added into the file.

        Parameters
        ----------
        variables : list of Variable
            A list of 'Variable' named tuples.
        func: func, func name
            Function to use for aggregating the data.
            It can be specified as np.mean, 'mean', 'sum', etc.
        key_name: str, default 'Custom Key'
            Specific key for a new variable. If this would not be
            unique, unique number is added automatically.
        var_name: str, default 'Custom Variable'
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
        groups = self._find_pairs(variables, part_match=part_match)

        if not groups or len(groups.keys()) > 1:
            raise CannotAggregateVariables("Cannot aggregate variables. "
                                           "Variables are not available or "
                                           "are from different intervals!")

        interval, ids = list(groups.items())[0]

        df = self.data_set(interval).get_results(ids)
        variables = df.columns.get_level_values("variable").tolist()
        units = df.columns.get_level_values("units").tolist()

        if len(set(units)) == 1:
            # no processing required
            units = units[0]

        elif rate_and_energy_units(units):
            # it's needed to assign multi index to convert energy
            try:
                n_days = self.data_set(interval).get_number_of_days()
            except KeyError:
                n_days = None
                if interval in [M, A, RP]:
                    raise CannotAggregateVariables(f"Cannot aggregate variables. "
                                                   f"'{N_DAYS_COLUMN}' is not available!")

            df = convert_rate_to_energy(df, interval, n_days)
            units = next(u for u in units if u in ("J", "J/m2"))

        else:
            raise CannotAggregateVariables("Cannot aggregate variables. "
                                           "Variables use different units!")

        sr = df.aggregate(func, axis=1)

        if var_name == "Custom Variable":
            if all(map(lambda x: x == variables[0], variables)):
                var_name = variables[0]

        if key_name == "Custom Key":
            func_name = func.__name__ if callable(func) else func
            key_name = f"{key_name} - {func_name}"

        # results can be either tuple (id, Variable) or None
        out = self.add_output(interval, key_name, var_name, units, sr)

        return out

    def remove_outputs(self, variables: Union[Variable, List[Variable]]) -> Dict[str, List[int]]:
        """ Remove given variables from the file. """
        variables = variables if isinstance(variables, list) else [variables]

        groups = self._find_pairs(variables)
        for interval, ids in groups.items():
            self.data_set(interval).remove_variables(ids)

            if self._outputs[interval].empty:
                del self._outputs[interval]

        # clean up the tree
        self._search_tree.remove_variables(variables)

        return groups

    def as_df(self, interval: str):
        """ Return the file as a single DataFrame. """
        try:
            df = self.data_set(interval).get_all_results(drop_special=True)

        except KeyError:
            raise KeyError(f"Cannot find interval: '{interval}'.")

        return df

    def save(self, path=None, directory=None, name=None):
        """ Save the file into filesystem. """
        pass
        # TODO implement
