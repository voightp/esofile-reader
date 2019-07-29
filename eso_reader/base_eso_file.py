import os
import time
import pandas as pd

from random import randint
from eso_reader.performance import perf
from eso_reader.mini_classes import HeaderVariable


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


def add_underscore(variable):
    """ Create a new variable with added '_' in key. """
    key, variable, units = variable
    new_key = "_" + key
    return new_key, variable, units


class BaseEsoFile:
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
        return "File: {}\n" \
               "Path: {}\n" \
               "Created: {}".format(self.file_name, self.file_path, human_time)

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

    @staticmethod
    def remove_header_variable(id_, header_dct):
        """ Remove header variable from header. """
        try:
            del header_dct[id_]
        except KeyError:
            print("Cannot remove id: {} from header.\n"
                  "Given id is not valid.")

    @perf
    @staticmethod
    def update_dt_format(df, output_type, timestamp_format):
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
        """
        Find variable ids for a list of 'Variables'.

        Parameters
        ----------
        variables : list of Variable
            A list of 'Variable' named tuples.
        part_match : bool
            Only substring of the part of variable is enough
            to match when searching for variables if this is True.
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
    def header_variables_mi(self, interval, ids):
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
                    tuples.append((*var, data))
        else:
            for id_ in ids:
                var = fetch_var()
                if var:
                    tuples.append((*var,))

        return pd.MultiIndex.from_tuples(tuples, names=names)

    @perf
    def add_header_data(self, interval, df):
        """ Add variable 'key', 'variable' and 'units' data. """
        mi = self.header_variables_mi(interval, df.columns)
        df.columns = mi

        # if not isinstance(df.columns[0], int):
        #     df.columns = pd.to_datetime(df.columns)  # revert 'datetime' dtype

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
    def _is_variable_unique(self, variable, interval):
        """ Check if the variable is included in a given interval. """
        is_unique = variable not in self.header_dct[interval].values()
        return is_unique

    def _create_variable(self, interval, variable):
        """ Validate a new variable. """
        while True:
            # Underscores will be added into a 'key' until the variable name is unique
            is_unique = self._is_variable_unique(variable, interval)

            if is_unique:
                # Variable is unique, return standard header variable
                header_variable = HeaderVariable(*variable)
                return header_variable

            variable = add_underscore(variable)

    def add_output(self, request_tuple, array):
        """ Add specified output variable to the file. """
        interval, key, var, units = request_tuple

        if interval not in self.available_intervals:
            print("Cannot add variable: '{} : {} : {}' into outputs.\n"
                  "Interval is not included in file '{}'".format(key, var, units,
                                                                 self.file_name))
            return

        variable = key, var, units
        header_dct = self.header_dct[interval]
        # Generate a unique identifier
        # Note that custom ids use '-' sign
        id_ = self.generate_rand_id()

        # Add variable identifiers to the header
        header_variable = self._create_variable(interval, variable)
        header_dct[id_] = header_variable

        # Add variable data to the output df
        outputs = self.outputs_dct[interval]
        is_valid = outputs.add_output(id_, array)

        if is_valid:
            # Variable can be added, create a reference in the search tree
            self.header_tree.add_branch(interval, key, var, units, id_)
        else:
            # Revert header dict in its original state
            self.remove_header_variable(id_, header_dct)
