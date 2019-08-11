import os
import time
import pandas as pd

from random import randint
from eso_reader.performance import perf
from eso_reader.mini_classes import Variable
from eso_reader.convertor import verify_units, rate_to_energy


class VariableNotFound(Exception):
    """ Exception raised when requested variable id is not available. """
    pass


class InvalidOutputType(Exception):
    """ Exception raised when the output time is invalid. """
    pass


def rand_id_gen():
    """ ID generator. """
    while True:
        yield -randint(1, 999999)


class BaseResultsFIle:
    """
    The AbstractEsoFile class works as a base for a 'physical' eso file and
    'building' totals file.

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

    Notes
    -----
    This class cannot be used directly!

    Method 'populate_data' is only a signature method so attributes cannot be populated.

    """

    def __init__(self):
        self.file_path = None
        self._complete = False

        self.file_timestamp = None
        self.environments = None
        self.header_dct = None
        self.outputs_dct = None
        self.header_tree = None

    def __repr__(self):
        human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.created))
        return f"File: {self.file_name}" \
            f"\nPath: {self.file_path}" \
            f"\nCreated: {human_time}"

    @property
    def file_name(self):
        """ Return eso file name without suffix. """
        bsnm = os.path.basename(self.file_path)
        return os.path.splitext(bsnm)[0]

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

    @property
    def complete(self):
        """ Check if the file has been populated and complete. """
        return self._complete

    @perf
    @property
    def header_df(self):
        """ Get pd.DataFrame like header (index: mi(interval, id). """
        rows = []
        for interval, variables in self.header_dct.items():
            for id_, var in variables.items():
                rows.append((interval, id_, *var))
        df = pd.DataFrame(rows, columns=["interval", "id", "key", "variable", "units"])
        return df

    @perf
    @classmethod
    def update_dt_format(cls, df, output_type, timestamp_format):
        """ Set specified 'datetime' str format. """
        if output_type in ["standard", "local_max", "local_min"]:
            df.index = df.index.strftime(timestamp_format)

        return df

    def populate_content(self, *args, **kwargs):
        """ Populate instance attributes. """
        pass

    def results_df(self, *args, **kwargs):
        """ Fetch results. """
        pass

    @perf
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

    @perf
    def find_pairs(self, variables, part_match=False):
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

            if interval in out:
                out[interval].extend(pairs[interval])
            else:
                out[interval] = pairs[interval]

        return out

    @perf
    def create_header_mi(self, interval, ids):
        """ Create a header pd.DataFrame for given ids and interval. """

        def fetch_var():
            try:
                return self.header_dct[interval][id_]
            except KeyError:
                print("Id '{}' was not found in the header!")

        tuples = []
        names = ["key", "variable", "units"]
        if isinstance(ids, pd.MultiIndex):
            names.append("data")
            for id_, data in ids:
                var = fetch_var()
                if var:
                    tuples.append((var.key, var.variable, var.units, data))
        else:
            for id_ in ids:
                var = fetch_var()
                if var:
                    tuples.append((var.key, var.variable, var.units,))

        return pd.MultiIndex.from_tuples(tuples, names=names)

    @perf
    def add_header_data(self, interval, df):
        """ Add variable 'key', 'variable' and 'units' data. """
        df.columns = self.create_header_mi(interval, df.columns)
        return df

    @perf
    def add_file_name(self, results, name_position):
        """ Add file name to index. """
        pos = ["row", "column", "None"]  # 'None' is here only to inform
        if name_position not in pos:
            name_position = "row"
            print("Invalid name position!\n'add_file_name' kwarg must be one of: "
                  "'{}'.\nSetting 'row'.".format(", ".join(pos)))

        axis = 0 if name_position == "row" else 1
        return pd.concat([results], axis=axis, keys=[self.file_name], names=["file"])

    @perf
    def generate_rand_id(self):
        """ Generate a unique id for custom variable. """
        gen = rand_id_gen()
        while True:
            id_ = next(gen)
            if id_ not in self.all_ids:
                return id_

    @perf
    def is_variable_unique(self, variable, interval):
        """ Check if the variable is included in a given interval. """
        return variable not in self.header_dct[interval].values()

    @perf
    def create_variable(self, interval, key, var, units):
        """ Create a unique header variable. """

        def add_num():
            new_key = f"{key} ({i})"
            return Variable(interval, new_key, var, units)

        variable = Variable(interval, key, var, units)
        is_unique = self.is_variable_unique(variable, interval)

        i = 0
        while not is_unique:
            i += 1
            variable = add_num()
            is_unique = self.is_variable_unique(variable, interval)

        return variable

    @perf
    def add_output(self, interval, key, var, units, array):
        """ Add specified output variable to the file. """

        if interval not in self.available_intervals:
            print(f"Cannot add variable: '{key} : {var} : {units}' into outputs.\n"
                  "Interval is not included in file '{self.file_name}'")
            return

        # generate a unique identifier, custom ids use '-' sign
        id_ = self.generate_rand_id()

        # add variable data to the output df
        is_valid = self.outputs_dct[interval].add_column(id_, array)

        if is_valid:
            # variable can be added, create a reference in the search tree
            new_var = self.create_variable(interval, key, var, units)
            self.header_dct[interval][id_] = new_var
            self.header_tree.add_branch(interval, key, var, units, id_)

            return id_, new_var

    @perf
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
        groups = self.find_pairs(variables, part_match=part_match)

        if not groups:
            print("There are no variables to sum!")
            return

        if len(groups.keys()) > 1:
            print("Cannot sum variables from multiple intervals!")
            return

        interval, ids = list(groups.items())[0]

        mi = self.create_header_mi(interval, ids)
        variables = mi.get_level_values("variable")
        units = mi.get_level_values("units")

        units = verify_units(units)

        if not units:
            print("Cannot sum variables using different units!")
            return

        data_set = self.outputs_dct[interval]
        df = data_set.standard_results(ids)

        if isinstance(units, list):
            # it's needed to convert rate to energy
            df.columns = mi
            df = rate_to_energy(df, data_set)
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

    @perf
    def remove_output_variables(self, interval, ids):
        """ Remove output data from the file. """
        try:
            out = self.outputs_dct[interval]
            out.remove_columns(ids)
            if out.empty:
                del self.outputs_dct[interval]
        except AttributeError:
            print(f"Invalid interval: '{interval}' specified!")

    @perf
    def remove_header_variables(self, interval, ids):
        """ Remove header variable from header. """
        ids = ids if isinstance(ids, list) else [ids]

        for id_ in ids:
            try:
                del self.header_dct[interval][id_]
                if not self.header_dct[interval]:
                    del self.header_dct[interval]
            except KeyError:
                print(f"Cannot remove id: {id_} from {interval}."
                      f"\nGiven id or interval is not valid.")

    @perf
    def remove_outputs(self, variables):
        """ Remove given variables from the file. """
        groups = self.find_pairs(variables)

        for ivl, ids in groups.items():
            self.remove_output_variables(ivl, ids)
            self.remove_header_variables(ivl, ids)

        self.header_tree.remove_variables(variables)

        return groups
