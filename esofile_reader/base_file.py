import os
import time
import pandas as pd

from random import randint
from datetime import datetime
from esofile_reader.outputs.convertor import verify_units, rate_to_energy, convert_units
from esofile_reader.constants import *
from esofile_reader.utils.mini_classes import Variable


class VariableNotFound(Exception):
    """ Exception raised when requested variable id is not available. """
    pass


class InvalidOutputType(Exception):
    """ Exception raised when the output time is invalid. """
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
    outputs : dict of {str : Outputs subclass}
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

        self.file_timestamp = None
        self.environments = None
        self.header = None
        self.outputs = None
        self.header_tree = None

    def __repr__(self):
        return f"File: {self.file_name}" \
            f"\nPath: {self.file_path}" \
            f"\nCreated: {self.created}"

    @property
    def available_intervals(self):
        """ Return a list of available intervals. """
        return list(self.header.keys())

    @property
    def all_ids(self):
        """ Return a list of all ids (regardless the interval). """
        ids = []
        for interval, data in self.header.items():
            keys = data.keys()
            ids.extend(keys)
        return ids

    @property
    def modified(self):
        """ Return a timestamp of the last file system modification. """
        return datetime.fromtimestamp(os.path.getmtime(self.file_path))

    @property
    def created(self):
        """ Return a timestamp of the file system creation. """
        return datetime.fromtimestamp(os.path.getmtime(self.file_path))

    @property
    def complete(self):
        """ Check if the file has been populated and complete. """
        return self._complete

    @property
    def header_df(self):
        """ Get pd.DataFrame like header (index: mi(interval, id). """
        rows = []
        for interval, variables in self.header.items():
            for id_, var in variables.items():
                rows.append((id_, *var))
        df = pd.DataFrame(rows, columns=["id", "interval", "key", "variable", "units"])
        return df

    @classmethod
    def update_dt_format(cls, df, output_type, timestamp_format):
        """ Set specified 'datetime' str format. """
        if output_type in ["standard", "local_max", "local_min"]:
            df.index = df.index.strftime(timestamp_format)

        return df

    def rename(self, name):
        """ Set a new file name. """
        self.file_name = name

    def populate_content(self, *args, **kwargs):
        """ Populate instance attributes. """
        pass

    def get_results(
            self, variables, start_date=None, end_date=None,
            output_type="standard", add_file_name="row", include_interval=False,
            include_id=False, part_match=False, units_system="SI",
            rate_to_energy_dct=RATE_TO_ENERGY_DCT, rate_units="W",
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
        output_type : {'standard', global_max','global_min'}
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

        Returns
        -------
        pandas.DataFrame
            Results for requested variables.

        """

        def standard():
            return data_set.get_results(*f_args)

        def global_max():
            return data_set.global_max(*f_args)

        def global_min():
            return data_set.global_min(*f_args)

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
            # Extract specified set of results
            f_args = (ids, start_date, end_date)
            data_set = self.outputs[interval]

            df = res[output_type]()

            if df is None:
                print(f"Results type '{output_type}' is not applicable for "
                      f"'{interval}' interval. \n\tignoring the request...")
                continue

            df.columns = self._create_header_mi(interval, df.columns)

            # convert 'rate' or 'energy' when standard results are requested
            if output_type == "standard" and rate_to_energy_dct[interval]:
                try:
                    n_days = data_set.get_number_of_days(start_date, end_date)
                except AttributeError:
                    n_days = None

                df = rate_to_energy(df, interval, n_days)

            if units_system != "SI" or rate_units != "W" or energy_units != "J":
                df = convert_units(df, units_system, rate_units, energy_units)

            if not include_id:
                df.columns = df.columns.droplevel("id")

            if not include_interval:
                df.columns = df.columns.droplevel("interval")

            frames.append(df)

        try:
            # Catch empty frames exception
            df = pd.concat(frames, axis=1, sort=False)

            if timestamp_format != "default":
                df = self.update_dt_format(df, output_type, timestamp_format)
            if add_file_name:
                df = self._add_file_name(df, add_file_name)
            return df

        except ValueError:
            print(f"Any of requested variables is not "
                  f"included in the Eso file '{self.file_name}'.")

    def find_ids(self, variables, part_match=False):
        """ Find ids for a list of 'Variables'. """
        out = []

        if not isinstance(variables, list):
            variables = [variables]

        for request in variables:
            interval, key, var, units = [str(r) if isinstance(r, int) else r for r in request]

            ids = self.header_tree.get_ids(interval=interval, key=key, variable=var,
                                           units=units, part_match=part_match)
            if not ids:
                continue

            out.extend(ids)

        return out

    def _find_pairs(self, variables, part_match=False):
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
        out = {}

        if not isinstance(variables, list):
            variables = [variables]

        for request in variables:
            interval, key, var, units = [str(r) if isinstance(r, int) else r for r in request]

            pairs = self.header_tree.get_pairs(interval=interval, key=key, variable=var,
                                               units=units, part_match=part_match)
            if not pairs:
                continue

            for k, v in pairs.items():
                if k in out.keys():
                    out[k].extend(pairs[k])
                else:
                    out[k] = pairs[k]

        return out

    def _create_header_mi(self, interval, ids):
        """ Create a header pd.DataFrame for given ids and interval. """

        def fetch_var():
            try:
                return self.header[interval][id_]
            except KeyError:
                print(f"Id '{id_}' was not found in the header!")

        tuples = []
        names = ["id", "interval", "key", "variable", "units"]
        if isinstance(ids, pd.MultiIndex):
            names.append("data")
            for id_, data in ids:
                var = fetch_var()
                if var:
                    tuples.append((str(id_), interval, var.key,
                                   var.variable, var.units, data))
        else:
            for id_ in ids:
                var = fetch_var()
                if var:
                    tuples.append((str(id_), interval, var.key,
                                   var.variable, var.units))

        return pd.MultiIndex.from_tuples(tuples, names=names)

    def _add_file_name(self, results, name_position):
        """ Add file name to index. """
        pos = ["row", "column", "None"]  # 'None' is here only to inform
        if name_position not in pos:
            name_position = "row"
            print(f"Invalid name position!\n'add_file_name' kwarg must be one of: "
                  f"'{', '.join(pos)}'.\nSetting 'row'.")

        axis = 0 if name_position == "row" else 1
        return pd.concat([results], axis=axis, keys=[self.file_name], names=["file"])

    def _add_header_variable(self, id_, interval, key, var, units):
        """ Create a unique header variable. """

        def add_num():
            new_key = f"{key} ({i})"
            return Variable(interval, new_key, var, units)

        variable = Variable(interval, key, var, units)

        i = 0
        while variable in self.header[interval].values():
            i += 1
            variable = add_num()

        self.header[interval][id_] = variable
        self.header_tree.add_branch(*variable, id_)

        return variable

    def rename_variable(self, variable, var_nm="", key_nm=""):
        """ Rename the given 'Variable' using given names. """
        ids = self.find_ids(variable)
        interval, key, var, units = variable

        if not var_nm:
            var_nm = var

        if not key_nm:
            key_nm = key

        if ids:
            # remove current item to avoid item duplicity
            self._remove_header_variables(interval, ids)

            # create a new header variable
            new_var = self._add_header_variable(ids[0], interval,
                                                key_nm, var_nm, units)

            return ids[0], new_var

    def add_output(self, interval, key_nm, var_nm, units, array):
        """ Add specified output variable to the file. """
        # generate a unique identifier, custom ids use '-' sign
        id_ = gen_id(self.all_ids, negative=True)

        # add variable data to the output df
        try:
            is_valid = self.outputs[interval].add_column(id_, array)
        except KeyError:
            is_valid = False
            print(f"Cannot add variable: "
                  f"'{key_nm} : {var_nm} : {units}'into outputs."
                  f"\nInterval is not included in file '{self.file_name}'")

        if is_valid:
            new_var = self._add_header_variable(id_, interval,
                                                key_nm, var_nm, units)
            return id_, new_var

    def aggregate_variables(self, variables, func, key_nm="Custom Key",
                            var_nm="Custom Variable", part_match=False):
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
        key_nm: str, default 'Custom Key'
            Specific key for a new variable. If this would not be
            unique, unique number is added automatically.
        var_nm: str, default 'Custom Variable'
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

        if not groups:
            print("There are no variables to sum!")
            return

        if len(groups.keys()) > 1:
            print("Cannot sum variables from multiple intervals!")
            return

        interval, ids = list(groups.items())[0]

        mi = self._create_header_mi(interval, ids)
        variables = mi.get_level_values("variable").tolist()
        units = mi.get_level_values("units").tolist()

        units = verify_units(units)

        if not units:
            print("Cannot sum variables using different units!")
            return

        data_set = self.outputs[interval]
        df = data_set.get_results(ids)

        if isinstance(units, list):
            # it's needed to assign multi index to convert energy
            df.columns = mi

            try:
                n_days = data_set.get_number_of_days()
            except AttributeError:
                n_days = None
                if interval in [M, A, RP]:
                    print("Cannot sum rate and energy variables!"
                          "\nN days column is not available!")
                    return

            df = rate_to_energy(df, interval, n_days)
            units = next(u for u in units if u in ("J", "J/m2"))

        sr = df.aggregate(func, axis=1)

        if var_nm == "Custom Variable":
            if all(map(lambda x: x == variables[0], variables)):
                var_nm = variables[0]

        if key_nm == "Custom Key":
            func_name = func.__name__ if callable(func) else func
            key_nm = f"{key_nm} - {func_name}"

        # results can be either tuple (id, Variable) or None
        out = self.add_output(interval, key_nm, var_nm, units, sr)

        return out

    def _remove_output_variables(self, interval, ids):
        """ Remove output data from the file. """
        try:
            self.outputs[interval].remove_columns(ids)
            if self.outputs[interval].empty:
                del self.outputs[interval]
        except KeyError:
            print(f"Invalid interval: '{interval}' specified!")

    def _remove_header_variables(self, interval, ids):
        """ Remove header variable from header. """
        ids = ids if isinstance(ids, list) else [ids]

        for id_ in ids:
            self.header_tree.remove_variables([self.header[interval][id_]])
            try:
                del self.header[interval][id_]
                if not self.header[interval]:
                    del self.header[interval]
            except KeyError:
                print(f"Cannot remove id: {id_} from {interval}."
                      f"\nGiven id or interval is not valid.")

    def remove_outputs(self, variables):
        """ Remove given variables from the file. """
        groups = self._find_pairs(variables)

        for ivl, ids in groups.items():
            self._remove_output_variables(ivl, ids)
            self._remove_header_variables(ivl, ids)

        return groups

    def as_df(self, interval):
        """ Return the file as a single DataFrame. """
        try:
            df = self.outputs[interval].get_all_results(drop_special=True)
            df.columns = self._create_header_mi(interval, df.columns)

        except KeyError:
            raise KeyError(f"Cannot find interval: '{interval}'.")

        return df

    def diff(self, other_file, absolute=False):
        """ Calculate difference between this and other results file. """
        diff = {}

        for interval in self.available_intervals:
            df1 = self.as_df(interval)

            if interval not in other_file.available_intervals:
                continue

            df2 = other_file.as_df(interval)

            df1.columns = df1.columns.droplevel("id")
            df1.columns = df1.columns.droplevel("interval")

            df2.columns = df2.columns.droplevel("id")
            df2.columns = df2.columns.droplevel("interval")

            try:
                df = df1 - df2
                df.dropna(how="all", inplace=True, axis=1)

                if not df.empty:
                    if absolute:
                        df = df.abs()

                    diff[interval] = df
            except MemoryError:
                raise MemoryError("Cannot subtract output DataFrames!"
                                  "\nRunning out of memory!")

            for c in [N_DAYS_COLUMN, DAY_COLUMN]:
                try:
                    if other_file.outputs[interval][c].equals(self.outputs[interval][c]):
                        df.insert(0, c, self.outputs[interval][c])
                except KeyError:
                    pass

        return diff

    def save(self, path=None, directory=None, name=None):
        """ Save the file into filesystem. """
        pass
        # TODO implement
