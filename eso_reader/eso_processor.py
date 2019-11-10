import re
import datetime as dt
import pandas as pd
import numpy as np

from functools import partial
from collections import defaultdict

from eso_reader.outputs import (Hourly, Daily, Monthly,
                                Annual, Runperiod, Timestep)
from eso_reader.interval_processor import interval_processor
from eso_reader.mini_classes import Variable, IntervalTuple
from eso_reader.constants import TS, H, D, M, A, RP
from eso_reader.tree import Tree
from eso_reader.monitor import DefaultMonitor


class InvalidLineSyntax(AttributeError):
    """ Exception raised for an unexpected line syntax. """
    pass


class BlankLineError(Exception):
    """ Exception raised when eso file contains blank line.  """
    pass


def _eso_file_version(raw_version):
    """ Return eso file version as an integer (i.e.: 860, 890). """
    version = raw_version.strip()
    start = version.index(" ")
    return int(version[(start + 1):(start + 6)].replace(".", ""))


def _dt_timestamp(timestamp):
    """ Return date and time of the eso file generation as a Datetime. """
    timestamp = timestamp.split("=")[1].strip()
    return dt.datetime.strptime(timestamp, "%Y.%m.%d %H:%M")


def _process_statement(line):
    """ Extract the version and time of the file generation. """
    _, _, raw_version, tmstmp = line.split(",")
    version = _eso_file_version(raw_version)
    timestamp = _dt_timestamp(tmstmp)
    return version, timestamp


def _process_header_line(line):
    """
    Process E+ dictionary line and populate period header dictionaries.

    The goal is to process line syntax:
        ID, number of results, key name - zone / environment, variable name [units] !timestamp [info]

    Parameters
    ----------
    line : str
        A raw eso file line.

    Returns
    -------
    tuple of (int, str, str, str, str)
        Processed line tuple (ID, key name, variable name, units, interval)

    """

    pattern = re.compile("^(\d+),(\d+),(.*?)(?:,(.*?) ?\[| ?\[)(.*?)\] !(\w*)")

    try:
        line_id, _, key, var, units, interval = pattern.search(line).groups()

    except AttributeError:
        raise InvalidLineSyntax("Unexpected header line syntax:" + line)

    # 'var' variable is 'None' for 'Meter' variable
    if var is None:
        var = key
        key = "Cumulative Meter" if "Cumulative" in key else "Meter"

    return int(line_id), key, var, units, interval.lower()


def create_variable(variables, interval, key, var, units):
    """ Create a unique header variable. """

    def is_unique():
        return variable not in variables

    def add_num():
        new_key = f"{key} ({i})"
        return Variable(interval, new_key, var, units)

    variable = Variable(interval, key, var, units)

    i = 0
    while not is_unique():
        i += 1
        variable = add_num()

    return variable


def read_header(eso_file, excl=None):
    """
    Read header dictionary of the eso file.

    The file is being read line by line until the 'End of Data Dictionary'
    is reached. Raw line is processed and the data is added as an item to
    the header_dict dictionary. The outputs dictionary is populated with
    dictionaries using output ids as keys and blank lists as values
    (to be populated later).

    Parameters
    ----------
    eso_file : EsoFile
        Opened EnergyPlus result file.
    excl : list of {'TS', 'H', 'D', 'M', 'A', 'RP'}
        A list of interval identifiers which will be ignored.

    Returns
    -------
    dict of {str : dict of {int : tuple)}
        A dictionary of eso file header data with populated values.
    dict of {str: dict of {int : []))
        A dictionary of expected eso file results with initialized lists.

    """

    if not excl:
        excl = []

    if not isinstance(excl, list):
        excl = list(excl)

    header_dct = defaultdict(partial(defaultdict))
    outputs = defaultdict(partial(defaultdict))

    while True:
        line = next(eso_file)

        # something is wrong when there is a blank line in the file
        if line == "":
            raise BlankLineError

        # Check if the end of data dictionary has been reached
        if "End of Data Dictionary" in line:
            break

        # Extract data from a raw line
        id_, key_nm, var_nm, units, interval = _process_header_line(line)

        # Block storing the data
        if interval in excl:
            continue

        # create a new item in header_dict for a given interval
        var = Variable(interval, key_nm, var_nm, units)

        # add variable into header dict and initialize
        # output item for a given frequency
        header_dct[interval][id_] = var
        outputs[interval][id_] = []

    return header_dct, outputs


def process_standard_lines(file):
    """ Process first few standard lines. """
    first_line = next(file)

    version, timestamp = _process_statement(first_line)
    last = _last_standard_item_id(version)

    # Skip standard reporting intervals
    _ = [next(file) for _ in range(last)]

    # Find the last item which defines reporting interval
    return last, timestamp


def _last_standard_item_id(version):
    """ Return last standard item id (6th item was added for E+ 8.9) """
    return 6 if version >= 890 else 5


def _process_raw_line(line):
    """ Return id and list of data without trailing whitespaces. """
    line_data = line.split(",")
    cleaned_line_data = [item.strip() for item in line_data]
    return int(line_data[0]), cleaned_line_data[1:]


def _process_interval_line(line_id, data):
    """
    Sort interval data into relevant period dictionaries.

    Note
    ----
    Each interval holds a specific piece of information i.e.:
        ts, hourly : [Day of Simulation, Month, Day of Month,
                        DST Indicator, Hour, StartMinute, EndMinute, DayType]
        daily : [Cumulative Day of Simulation, Month, Day of Month,DST Indicator, DayType]
        monthly : [Cumulative Day of Simulation, Month]
        annual : [Year] (only when custom weather file is used) otherwise [int]
        runperiod :  [Cumulative Day of Simulation]

    For annual and runperiod intervals, a dummy data is assigned (for
    runperiod only date information - cumulative days are known). This
    is processed later in interval processor module.

    Parameters
    ----------
    line_id : int
        An id of the interval.
    data : list of str
        Line data passed as a list of strings (without ID).

    Returns
    -------
    For timestep, hourly and daily results only interval
    identifier tuple of int to specify date and time information

    tuple of (str, tuple of (n*int))
        Interval identifier and numeric date time information.

    For monthly, annual and runperiod cumulative days
    of simulation info is included in the lowest level tuple.

    tuple of (str, tuple of (int, tuple of (int,int))
        Interval identifier, cumulative num of days and date information.
    """

    def new_environment():
        return None, None  # this will not be used

    def hourly_interval():
        """ Process TS or H interval entry and return interval identifier. """
        # omit day of week in conversion
        i = [int(float(item)) for item in data[:-1]]
        interval = IntervalTuple(i[1], i[2], i[4], i[6])

        # check if interval is timestep or hourly interval
        if i[5] == 0 and i[6] == 60:
            return H, interval
        else:
            return TS, interval

    def daily_interval():
        """ Populate D list and return identifier. """
        # omit day of week in in conversion
        i = [int(item) for item in data[:-1]]
        # (Month, Day of Month)
        interval = IntervalTuple(i[1], i[2], 0, 0)
        return D, interval

    def monthly_interval():
        """ Populate M list and return identifier. """
        interval = IntervalTuple(int(data[1]), 1, 0, 0)
        return M, (int(data[0]), interval)

    def runperiod_interval():
        """ Populate RP list and return identifier. """
        interval = IntervalTuple(1, 1, 0, 0)
        return RP, (int(data[0]), interval)

    def annual_interval():
        """ Populate A list and return identifier. """
        interval = IntervalTuple(1, 1, 0, 0)
        return A, (None, interval)

    # switcher to return data for a specific interval
    categories = {
        1: new_environment,
        2: hourly_interval,
        3: daily_interval,
        4: monthly_interval,
        5: runperiod_interval,
        6: annual_interval,
    }

    return categories[line_id]()


def _process_result_line(data, ignore_peaks):
    """ Convert items of result data list from string to float. """
    if len(data) == 1 or ignore_peaks:
        # Hourly and timestep results hold only a single value
        return np.float(data[0])

    # return tuple([np.float(item) for item in data])
    return tuple([np.float(i) if "." in i else np.int(i) for i in data])


def read_body(eso_file, highest_interval_id, outputs, ignore_peaks, monitor):
    """
    Read body of the eso file.

    The data from eso file is processed line by line until the
    'End of Data' is reached. Interval data is stored in the 'envs'
    list, where each item represents a single environment.
    Result data is stored in the 'outputs' dictionary.

    Index 1-5 for eso file generated prior to E+ 8.9 or 1-6 from E+ 8.9
    further, indicates that line is an interval.

    Parameters
    ----------
    eso_file : EsoFile
        Opened EnergyPlus result file.
    highest_interval_id : int
        A maximum index defining an interval (higher is considered a result)
    outputs : dict of {str: dict of {int : []))
        A dictionary of expected eso file results with initialized blank lists.
        This is generated by 'read_header' function.
    ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.
    monitor : DefaultMonitor, CustomMonitor
        A custom class to monitor processing progress

    Returns
    -------
    outputs: dict of {str: dict of {int : list of float or int))
        A processed outputs dictionary - processed outputs are stored in lists.
    envs: list of (list of (Timestamp))
        A nested list of raw environment information.

     """
    envs = {k: [] for k in outputs.keys()}
    identifier = None
    i = 0

    while True:
        i += 1
        line = next(eso_file)
        monitor.update_body_progress()

        # something is wrong when there is a blank line in the file
        if line == "":
            monitor.processing_failed("Blank line in file.")
            raise BlankLineError

        # Check if the end of data block is reached
        if "End of Data" in line:
            break

        line_id, data = _process_raw_line(line)

        # Decide if the current line represents
        # an interval or an output
        if line_id <= highest_interval_id:
            identifier, data = _process_interval_line(line_id, data)

            # New environment
            if line_id == 1:
                # Initialize new list to store environment data
                [value.append([]) for value in envs.values()]

            else:
                if identifier not in envs.keys():
                    # Skip when current interval is not requested
                    continue

                # Populate last environment list with interval data
                envs[identifier][-1].append(data)

                # Populate current step for all result ids with nan values.
                # This is in place to avoid issues for variables which are not
                # reported during current interval
                [v.append(np.nan) for v in outputs[identifier].values()]

        else:
            if identifier not in envs.keys():
                continue  # Skip when current interval is not requested

            # current line represents a result
            # replace nan values from the last step
            res = _process_result_line(data, ignore_peaks)
            try:
                outputs[identifier][line_id][-1] = res
            except KeyError:
                print(f"ignoring {line_id}")

    return outputs, envs


def _gen_output(data, index, interval, num_of_days):
    """ Handle class assignment for output data. """
    clmn_name = "num days"  # Additional column to store 'Number of days'
    index = pd.Index(index, name="timestamp")
    columns = pd.Index(data.keys(), name="id")

    def gen_ts():
        return Timestep(data, index=index, columns=columns)

    def gen_h():
        return Hourly(data, index=index, columns=columns)

    def gen_d():
        return Daily(data, index=index, columns=columns)

    def gen_m():
        out = Monthly(data, index=index, columns=columns)
        out.insert(0, clmn_name, num_of_days)
        return out

    def gen_a():
        out = Annual(data, index=index, columns=columns)
        out.insert(0, clmn_name, num_of_days)
        return out

    def gen_rp():
        out = Runperiod(data, index=index, columns=columns)
        out.insert(0, clmn_name, num_of_days)
        return out

    gen = {
        TS: gen_ts,
        H: gen_h,
        D: gen_d,
        M: gen_m,
        A: gen_a,
        RP: gen_rp,
    }

    return gen[interval]()


def generate_outputs(outputs_dct, envs_dct, num_of_days_dct):
    """ Transform processed output data into DataFrame like classes. """
    intervals = envs_dct.keys()

    # Transform outputs
    for interval in intervals:
        num_of_days = num_of_days_dct.get(interval, None)
        outputs_dct[interval] = _gen_output(outputs_dct[interval],
                                            envs_dct[interval],
                                            interval,
                                            num_of_days)
    return outputs_dct


def create_tree(header_dct):
    """ Generate a search tree. """
    tree = Tree()
    dup_ids = tree.populate_tree(header_dct)
    return tree, dup_ids


def remove_duplicates(ids, header_dct, outputs_dct):
    """ Remove duplicate outputs from results set. """
    intervals = header_dct.keys()
    for id_ in ids:
        for interval in intervals:
            try:
                del header_dct[interval][id_]
                del outputs_dct[interval][id_]
                print(f"Removing duplicate variable '{id_}'.")
            except KeyError:
                pass


def process_file(file, monitor, excl=None, ignore_peaks=True, ):
    """ Process raw EnergyPlus output file. """
    # process first few standard lines
    last_standard_item_id, timestamp = process_standard_lines(file)

    # Read header to obtain a header dictionary of EnergyPlus
    # outputs and initialize dictionary for output values
    try:
        header_dct, init_outputs = read_header(file, excl=excl)
        monitor.header_finished()
    except BlankLineError:
        msg = "Blank line in header."
        monitor.processing_failed(msg)
        raise BlankLineError(msg)

    # Read body to obtain outputs and environment dictionaries.
    # Intervals excluded in header are ignored
    outputs_dct, envs = read_body(file, last_standard_item_id,
                                  init_outputs, ignore_peaks, monitor)
    monitor.body_finished()

    # Sort interval data into relevant dictionaries
    environments, env_dict, num_of_days_dict = interval_processor(envs)
    monitor.intervals_finished()

    # transform standard dictionaries into DataFrame
    # like Output classes
    outputs_dct = generate_outputs(outputs_dct, env_dict, num_of_days_dict)
    monitor.output_cls_gen_finished()

    # Create a 'search tree' to allow searching for variables
    # using header data
    tree, dup_ids = create_tree(header_dct)
    monitor.header_tree_finished()

    if dup_ids:
        # remove duplicates from header and outputs
        remove_duplicates(dup_ids, header_dct, outputs_dct)

    monitor.processing_finished()
    return timestamp, environments, header_dct, outputs_dct, tree


def read_file(file_path, exclude_intervals=None, monitor=None,
              report_progress=False, ignore_peaks=True, suppress_errors=False):
    """ Open the eso file and trigger file processing. """
    if monitor is None:
        monitor = DefaultMonitor(file_path, print_report=report_progress)

    # Initially read the file to check if it's ok
    monitor.processing_started()
    complete = monitor.preprocess(suppress_errors)

    if not complete:
        # prevent reading the file when incomplete
        return

    try:
        with open(file_path, "r") as file:
            return process_file(file, monitor, excl=exclude_intervals, ignore_peaks=ignore_peaks)

    except IOError:
        print("IOError thrown when handling: " + file_path)
