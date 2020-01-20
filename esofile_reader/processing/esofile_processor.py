import re
import datetime as dt
import pandas as pd
import numpy as np

from functools import partial
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List

from esofile_reader.outputs.outputs import Outputs, create_peak_outputs
from esofile_reader.processing.interval_processor import interval_processor
from esofile_reader.utils.mini_classes import Variable, IntervalTuple
from esofile_reader.constants import *
from esofile_reader.utils.tree import Tree
from esofile_reader.processing.monitor import DefaultMonitor


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
        raise InvalidLineSyntax(f"Unexpected header line syntax: {line}")

    # 'var' variable is 'None' for 'Meter' variable
    if var is None:
        var = key
        key = "Cumulative Meter" if "Cumulative" in key else "Meter"

    return int(line_id), key, var, units, interval.lower()


def read_header(eso_file):
    """
    Read header dictionary of the eso file.

    The file is being read line by line until the 'End of Data Dictionary'
    is reached. Raw line is processed and the line is added as an item to
    the header_dict dictionary. The outputs dictionary is populated with
    dictionaries using output ids as keys and blank lists as values
    (to be populated later).

    Parameters
    ----------
    eso_file : EsoFile
        Opened EnergyPlus result file.

    Returns
    -------
    dict of {str : dict of {int : tuple)}
        A dictionary of eso file header line with populated values.
    dict of {str: dict of {int : []))
        A dictionary of expected eso file results with initialized lists.

    """

    header_dct = defaultdict(partial(defaultdict))
    outputs = defaultdict(partial(defaultdict))

    while True:
        line = next(eso_file)

        # Extract line from a raw line
        try:
            id_, key_nm, var_nm, units, interval = _process_header_line(line)

        except AttributeError:
            if "End of Data Dictionary" in line:
                break
            elif line == "":
                raise BlankLineError
            else:
                raise AttributeError

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
    """ Return id and list of line without trailing whitespaces. """
    split_line = line.split(",")
    return int(split_line[0]), split_line[1:]


def _process_interval_line(line_id, data):
    """
    Sort interval line into relevant period dictionaries.

    Note
    ----
    Each interval holds a specific piece of information i.e.:
        ts, hourly : [Day of Simulation, Month, Day of Month,
                        DST Indicator, Hour, StartMinute, EndMinute, DayType]
        daily : [Cumulative Day of Simulation, Month, Day of Month,DST Indicator, DayType]
        monthly : [Cumulative Day of Simulation, Month]
        annual : [Year] (only when custom weather file is used) otherwise [int]
        runperiod :  [Cumulative Day of Simulation]

    For annual and runperiod intervals, a dummy line is assigned (for
    runperiod only date information - cumulative days are known). This
    is processed later in interval processor module.

    Parameters
    ----------
    line_id : int
        An id of the interval.
    data : list of str
        Line line passed as a list of strings (without ID).

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

    def hourly_interval():
        """ Process TS or H interval entry and return interval identifier. """
        # omit day of week in conversion
        i = [int(float(item)) for item in data[:-1]]
        interval = IntervalTuple(i[1], i[2], i[4], i[6])

        # check if interval is timestep or hourly interval
        if i[5] == 0 and i[6] == 60:
            return H, interval, data[-1]
        else:
            return TS, interval, data[-1]

    def daily_interval():
        """ Populate D list and return identifier. """
        # omit day of week in in conversion
        i = [int(item) for item in data[:-1]]
        return D, IntervalTuple(i[1], i[2], 0, 0), data[-1]

    def monthly_interval():
        """ Populate M list and return identifier. """
        return M, IntervalTuple(int(data[1]), 1, 0, 0), int(data[0])

    def runperiod_interval():
        """ Populate RP list and return identifier. """
        return RP, IntervalTuple(1, 1, 0, 0), int(data[0])

    def annual_interval():
        """ Populate A list and return identifier. """
        return A, IntervalTuple(1, 1, 0, 0), None

    # switcher to return line for a specific interval
    categories = {
        2: hourly_interval,
        3: daily_interval,
        4: monthly_interval,
        5: runperiod_interval,
        6: annual_interval,
    }

    return categories[line_id]()


def _process_result_line(line, ignore_peaks):
    """ Convert items of result line list from string to float. """
    # first value is converted in batch when creating results DataFrame
    if ignore_peaks:
        return line[0], None
    else:
        return line[0], [np.float(i) if "." in i else np.int(i) for i in line[1:]]


def read_body(eso_file, highest_interval_id, outputs, ignore_peaks, monitor):
    """
    Read body of the eso file.

    The line from eso file is processed line by line until the
    'End of Data' is reached. Interval line is stored in the 'envs'
    list, where each item represents a single environment.
    Result line is stored in the 'outputs' dictionary.

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
    dates = {k: [] for k in outputs.keys()}
    cumulative_days = {k: [] for k in outputs.keys() if k in (M, A, RP)}
    days_of_week = defaultdict(list)
    interval = None

    if not ignore_peaks:
        peak_outputs = {k: v for k, v in deepcopy(outputs).items() if k in (D, M, A, RP)}
    else:
        peak_outputs = None

    while True:
        line = next(eso_file)
        monitor.update_body_progress()

        try:
            line_id, line = _process_raw_line(line)
        except ValueError:
            if "End of Data" in line:
                break
            elif line == "":
                raise BlankLineError
            else:
                raise ValueError

        if line_id <= highest_interval_id:
            # check if the current line represents
            # an interval or an output
            if line_id == 1:
                # Initialize new list to store environment line
                [value.append([]) for value in dates.values()]
                [value.append([]) for value in cumulative_days.values()]
            else:
                interval, date, other = _process_interval_line(line_id, line)

                if interval not in dates.keys():
                    # Skip when current interval is not requested
                    continue

                # Populate last environment list with interval line
                dates[interval][-1].append(date)

                # Populate current step for all result ids with nan values.
                # This is in place to avoid issues for variables which are not
                # reported during current interval
                [v.append(np.nan) for v in outputs[interval].values()]

                if line_id >= 3 and not ignore_peaks:
                    [v.append(np.nan) for v in peak_outputs[interval].values()]

                if line_id <= 3:
                    days_of_week[interval].append(other.strip())
                else:
                    cumulative_days[interval][-1].append(other)

        else:
            # current line represents a result
            # replace nan values from the last step
            res, peak_res = _process_result_line(line, ignore_peaks)
            try:
                outputs[interval][line_id][-1] = res
                if peak_res:
                    peak_outputs[interval][line_id][-1] = peak_res
            except KeyError:
                print(f"ignoring {line_id}")

    return outputs, peak_outputs, dates, cumulative_days, days_of_week


def create_values_df(outputs_dct: Dict[int, Variable], index_name: str) -> pd.DataFrame:
    """ Create a raw values pd.DataFrame for given interval. """
    df = pd.DataFrame(outputs_dct)
    df = df.T
    df.index.set_names(index_name, inplace=True)
    return df


def create_header_df(header_dct: Dict[int, Variable], interval: str,
                     index_name: str, columns: List[str]) -> pd.DataFrame:
    """ Create a raw header pd.DataFrame for given interval. """
    rows, index = [], []
    for id_, var in header_dct.items():
        rows.append([interval, var.key, var.variable, var.units])
        index.append(id_)

    return pd.DataFrame(rows, columns=columns, index=pd.Index(index, name=index_name))


def generate_peak_outputs(raw_peak_outputs, header, dates):
    """ Transform processed peak output data into DataFrame like classes. """
    peak_outputs = {"local_min": {}, "local_max": {}}
    column_names = ["id", "interval", "key", "variable", "units"]
    for interval, values in raw_peak_outputs.items():
        df_values = create_values_df(values, column_names[0])
        df_header = create_header_df(header[interval], interval, column_names[0],
                                     column_names[1:])

        df = pd.merge(df_header, df_values, sort=False,
                      left_index=True, right_index=True)

        df.set_index(keys=column_names[1:], append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        peak_outputs["local_min"][interval] = create_peak_outputs(df, interval, max_=False)
        peak_outputs["local_max"][interval] = create_peak_outputs(df, interval)

    return peak_outputs


def generate_outputs(raw_outputs, header, dates, other_data):
    """ Transform processed output data into DataFrame like classes. """
    outputs = {}
    column_names = ["id", "interval", "key", "variable", "units"]
    for interval, values in raw_outputs.items():
        df_values = create_values_df(values, column_names[0])
        df_header = create_header_df(header[interval], interval, column_names[0],
                                     column_names[1:])

        df = pd.merge(df_header, df_values, sort=False,
                      left_index=True, right_index=True)

        df.set_index(keys=column_names[1:], append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        out = Outputs(df, dtype=np.float)

        for k, v in other_data.items():
            try:
                column = v.pop(interval)
                out.insert(0, k, column)
            except KeyError:
                pass

        outputs[interval] = out

    return outputs


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
            for dct in [header_dct, outputs_dct]:
                try:
                    del dct[interval][id_]
                    print(f"Removing duplicate variable '{id_}'.")
                except KeyError:
                    pass


def process_file(file, monitor, year, ignore_peaks=True):
    """ Process raw EnergyPlus output file. """
    # process first few standard lines, ignore timestamp
    last_standard_item_id, _ = process_standard_lines(file)

    # Read header to obtain a header dictionary of EnergyPlus
    # outputs and initialize dictionary for output values
    header, init_outputs = read_header(file)
    monitor.header_finished()

    # Read body to obtain outputs and environment dictionaries.
    (raw_outputs, raw_peak_outputs, dates,
     cumulative_days, day_of_week) = read_body(file, last_standard_item_id,
                                               init_outputs, ignore_peaks, monitor)
    monitor.body_finished()

    # Sort interval line into relevant dictionaries
    dates, n_days = interval_processor(dates, cumulative_days, year)
    monitor.intervals_finished()

    # Create a 'search tree' to allow searching for variables
    tree, dup_ids = create_tree(header)
    monitor.header_tree_finished()

    if dup_ids:
        # remove duplicates from header and outputs
        remove_duplicates(dup_ids, header, raw_outputs)

    if not ignore_peaks:
        peak_outputs = generate_peak_outputs(raw_peak_outputs, header, dates)
    else:
        peak_outputs = None

    # transform standard dictionaries into DataFrame like Output classes
    other_data = {N_DAYS_COLUMN: n_days, DAY_COLUMN: day_of_week}
    outputs = generate_outputs(raw_outputs, header, dates, other_data)
    monitor.output_cls_gen_finished()

    monitor.processing_finished()

    return outputs, peak_outputs, tree


def read_file(file_path, monitor=None, report_progress=False,
              ignore_peaks=True, suppress_errors=False, year=2002):
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
            return process_file(file, monitor, year, ignore_peaks=ignore_peaks)

    except BlankLineError:
        msg = f"There's a blank line in file '{file_path}'."
        monitor.processing_failed(msg)
        raise BlankLineError(msg)

    except IOError:
        print(f"IOError thrown when handling: '{file_path}'")
