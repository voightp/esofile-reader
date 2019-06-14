import re
import datetime as dt
import pandas as pd
import numpy as np

from functools import partial
from collections import defaultdict

from eso_reader.outputs import Hourly, Daily, Monthly, Annual, Runperiod, Timestep
from eso_reader.interval_processor import interval_processor
from eso_reader.mini_classes import HeaderVariable, IntervalTuple
from eso_reader.constants import TS, H, D, M, A, RP
from eso_reader.search import Tree
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


def _dt_timestamp(tmstmp):
    """ Return date and time of the eso file generation as a Datetime object. """
    timestamp = tmstmp.split("=")[1].strip()
    return dt.datetime.strptime(timestamp, "%Y.%m.%d %H:%M")


def _process_statement(file):
    """ Process the first line of eso file to extract the version and time when the file was generated. """
    _, _, raw_version, tmstmp = next(file).split(",")
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
        Processed line tuple (ID, key name, variable name, units, frequency)
    """

    pattern = re.compile("^(\d+),(\d+),(.*?)(?:,(.*?) ?\[| ?\[)(.*?)\] !(\w*)")

    try:
        line_id, _, key, var, units, frequency = pattern.search(line).groups()

    except InvalidLineSyntax:
        print("Unexpected header line syntax:" + line)
        raise

    # 'var' variable is 'None' for 'Meter' variable
    if var is None:
        var = key
        key = "Cumulative Meter" if "Cumulative" in key else "Meter"

    return int(line_id), key, var, units, frequency.lower()


def read_header(eso_file, monitor, excl=None):
    """
    Read header dictionary of the eso file.

    The file is being read line by line until the 'End of Data Dictionary'
    is reached. Raw line is processed and the data is added as an item to
    the header_dict dictionary. The outputs dictionary is populated with
    dictionaries using output ids as keys and blank lists as values (to be populated later).

    Parameters
    ----------
    eso_file : EsoFile
        Opened EnergyPlus result file.
    excl : list of {'TS', 'H', 'D', 'M', 'A', 'RP'}
        A list of interval identifiers which will be ignored.
    monitor : DefaultMonitor, CustomMonitor
        A custom class to monitor processing progress.

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

    header_dicts = defaultdict(partial(defaultdict))
    outputs = defaultdict(partial(defaultdict))

    while True:
        line = next(eso_file)

        # something is wrong when there is a blank line in the file
        if line == "":
            monitor.processing_failed("Blank line in header.")
            raise BlankLineError

        # Check if the end of data dictionary has been reached
        if "End of Data Dictionary" in line:
            break

        # Extract data from a raw line
        line_id, key_name, var_name, units, frequency = _process_header_line(line)

        # Block storing the data
        if frequency in excl:
            continue

        # Create a new item in header_dict for a given frequency
        header_dicts[frequency][line_id] = HeaderVariable(key_name, var_name, units)

        # Initialize output item for a given frequency
        outputs[frequency][line_id] = []

    return header_dicts, outputs


def process_standard_lines(file):
    """ Process first few standard lines. """
    version, timestamp = _process_statement(file)
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
        """ Process timestep or hourly interval entry and return identifier for following result lines. """
        i = [int(float(item)) for item in data[:-1]]  # omit day of week in conversion
        interval = IntervalTuple(i[1], i[2], i[4], i[6])

        # check if interval is timestep or hourly frequency
        if i[5] == 0 and i[6] == 60:
            return H, interval
        else:
            return TS, interval

    def daily_interval():
        """ Populate daily list and return identifier for following result lines. """
        i = [int(item) for item in data[:-1]]  # omit day of week in in conversion
        interval = IntervalTuple(i[1], i[2], 0, 0)  # (Month, Day of Month)
        return D, interval

    def monthly_interval():
        """ Populate monthly list and return identifier for following result lines. """
        interval = IntervalTuple(int(data[1]), 1, 0, 0)
        return M, (int(data[0]), interval)

    def runperiod_interval():
        """ Populate runperiod list and return identifier for following result lines. """
        interval = IntervalTuple(1, 1, 0, 0)
        return RP, (int(data[0]), interval)

    def annual_interval():
        """ Populate annual list and return identifier for following result lines. """
        interval = IntervalTuple(1, 1, 0, 0)
        return A, (None, interval)

    # switcher to return data for a specific frequency
    categories = {
        1: new_environment,
        2: hourly_interval,
        3: daily_interval,
        4: monthly_interval,
        5: runperiod_interval,
        6: annual_interval,
    }

    return categories[line_id]()


def _process_result_line(data):
    """ Convert items of result data list from string to float. """
    if len(data) == 1:
        # Hourly and timestep results hold only a single value
        return np.float(data[0])

    # return tuple([np.float(item) for item in data])
    return tuple([np.float(item) if "." in item else np.int(item) for item in data])


def read_body(eso_file, highest_interval_id, outputs, monitor):
    """
    Read body of the eso file.

    The data from eso file is processed line by line until the
    'End of Data' is reached. Interval data is stored in the 'envs'
    list, where each item represents a single environment.
    Result data is stored in the 'outputs' dictionary.

    Index 1-5 for eso file generated prior to E+ 8.9 or 1-6 from E+ 8.9 further,
    indicates that line is an interval.

    Parameters
    ----------
    eso_file : EsoFile
        Opened EnergyPlus result file.
    highest_interval_id : int
        A maximum index defining an interval (higher is considered a result)
    outputs : dict of {str: dict of {int : []))
        A dictionary of expected eso file results with initialized blank lists.
        This is generated by 'read_header' function.
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
            outputs[identifier][line_id][-1] = _process_result_line(data)

    return outputs, envs


def _gen_output(data, index, interval, num_of_days):
    """ Handle class assignment for output data. """
    clmn_name = "Number of days"  # Additional column to store 'Number of days'
    index = pd.Index(index, name="timestamp")

    def gen_ts():
        return Timestep(data, index=index)

    def gen_h():
        return Hourly(data, index=index)

    def gen_d():
        return Daily(data, index=index)

    def gen_m():
        out = Monthly(data, index=index)
        out.insert(0, clmn_name, num_of_days)
        return out

    def gen_a():
        out = Annual(data, index=index)
        out.insert(0, clmn_name, num_of_days)
        return out

    def gen_rp():
        out = Runperiod(data, index=index)
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
    return Tree(header_dct)


def process_file(file, monitor, excl=None):
    """ Process raw EnergyPlus output file. """
    # process first few standard lines
    last_standard_item_id, timestamp = process_standard_lines(file)

    # Read header to obtain a header dictionary of EnergyPlus outputs
    # and initialize dictionary for output values
    header_dicts, init_outputs = read_header(file, monitor, excl=excl)
    monitor.header_finished()

    # Read body to obtain outputs and environment dictionaries.
    # Intervals excluded in header are ignored
    outputs, envs = read_body(file, last_standard_item_id, init_outputs, monitor)
    monitor.body_finished()

    # Sort interval data into relevant dictionaries
    environments, env_dict, num_of_days_dict = interval_processor(envs)
    monitor.intervals_finished()

    # Transform standard dictionaries into DataFrame like Output classes
    outputs = generate_outputs(outputs, env_dict, num_of_days_dict)
    monitor.output_cls_gen_finished()

    # Create a 'search tree' to allow searching for variables using header data
    tree = create_tree(header_dicts)
    monitor.header_tree_finished()

    monitor.processing_finished()
    return timestamp, environments, header_dicts, outputs, tree


def read_file(file_path, exclude_intervals=None, monitor=None, report_progress=False, suppress_errors=False):
    """ Open the eso file and trigger file processing. """
    if monitor is None:
        monitor = DefaultMonitor(file_path, print_report=report_progress)

    # Initially read the file to check if it's ok
    complete = monitor.preprocess(suppress_errors)

    if not complete:
        # prevent reading the file when incomplete
        return

    try:
        with open(file_path, "r") as file:
            return process_file(file, monitor, excl=exclude_intervals)

    except IOError:
        print("IOError thrown when handling: " + file_path)
