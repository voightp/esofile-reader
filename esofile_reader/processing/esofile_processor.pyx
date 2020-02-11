import datetime as dt
import re
from collections import defaultdict
from copy import deepcopy
from functools import partial
from typing import Dict, List

import numpy as np
import pandas as pd

from esofile_reader.base_file import IncompleteFile
from esofile_reader.constants import *
from esofile_reader.processing.interval_processor import interval_processor
from esofile_reader.processing.monitor import DefaultMonitor
from esofile_reader.data.df_functions import create_peak_outputs
from esofile_reader.data.df_data import DFData
from esofile_reader.utils.mini_classes import Variable, IntervalTuple
from esofile_reader.utils.search_tree import Tree


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
    if ignore_peaks:
        return float(line[0]), None
    else:
        return float(line[0]), [float(i) if "." in i else int(i) for i in line[1:]]


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
    # initialize storage for all outputs
    all_outputs = []
    all_peak_outputs = []
    environments = []

    # initialize datetime bins
    dates = []
    cumulative_days = []
    days_of_week = []

    while True:
        raw_line = next(eso_file)
        monitor.update_body_progress()

        try:
            line_id, line = _process_raw_line(raw_line)
        except ValueError:
            if "End of Data" in raw_line:
                break
            elif raw_line == "":
                raise BlankLineError
            else:
                raise ValueError

        if line_id <= highest_interval_id:
            if line_id == 1:
                environments.append(line[0].strip())

                # initialize outputs for the current environment
                all_outputs.append(deepcopy(outputs))
                if not ignore_peaks:
                    all_peak_outputs.append(deepcopy({k: v for k, v in outputs.items()
                                                      if k in (D, M, A, RP)}))
                else:
                    all_peak_outputs.append(None)

                # initialize date time data for the current environment
                dates.append(defaultdict(list))
                days_of_week.append(defaultdict(list))
                cumulative_days.append(defaultdict(list))
            else:
                interval, date, other = _process_interval_line(line_id, line)

                # Populate last environment list with interval line
                dates[-1][interval].append(date)

                # Populate current step for all result ids with nan values.
                # This is in place to avoid issues for variables which are not
                # reported during current interval
                [v.append(np.nan) for v in all_outputs[-1][interval].values()]

                if line_id >= 3 and not ignore_peaks:
                    [v.append(np.nan) for v in all_peak_outputs[-1][interval].values()]

                if line_id <= 3:
                    days_of_week[-1][interval].append(other.strip())
                else:
                    cumulative_days[-1][interval].append(other)

        else:
            # current line represents a result
            # replace nan values from the last step
            try:
                res, peak_res = _process_result_line(line, ignore_peaks)
            except ValueError:
                raise ValueError(f"Unexpected value on line {line_id}: "
                                 f"{raw_line}")

            try:
                all_outputs[-1][interval][line_id][-1] = res
                if peak_res:
                    all_peak_outputs[-1][interval][line_id][-1] = peak_res
            except KeyError:
                print(f"Ignoring {line_id}, variable is not included in header!")

    return environments, all_outputs, all_peak_outputs, \
           dates, cumulative_days, days_of_week


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
    column_names = ["id", "interval", "key", "variable", "units"]

    min_peaks = DFData()
    max_peaks = DFData()

    for interval, values in raw_peak_outputs.items():
        df_values = create_values_df(values, column_names[0])
        df_header = create_header_df(header[interval], interval, column_names[0],
                                     column_names[1:])

        df = pd.merge(df_header, df_values, sort=False,
                      left_index=True, right_index=True)

        df.set_index(keys=column_names[1:], append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        min_df = create_peak_outputs(interval, df, max_=False)
        min_peaks.populate_table(interval, min_df)

        max_df = create_peak_outputs(interval, df)
        max_peaks.populate_table(interval, max_df)

    peak_outputs = {
        "local_min": min_peaks,
        "local_max": max_peaks
    }

    return peak_outputs


def generate_outputs(raw_outputs, header, dates, other_data):
    """ Transform processed output data into DataFrame like classes. """
    column_names = ["id", "interval", "key", "variable", "units"]
    outputs = DFData()

    for interval, values in raw_outputs.items():
        df_values = create_values_df(values, column_names[0])
        df_header = create_header_df(header[interval], interval, column_names[0],
                                     column_names[1:])

        df = pd.merge(df_header, df_values, sort=False,
                      left_index=True, right_index=True)

        df.set_index(keys=column_names[1:], append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        # add other special columns
        for k, v in other_data.items():
            try:
                column = v.pop(interval)
                df.insert(0, k, column)
            except KeyError:
                pass
        outputs.populate_table(interval, df)

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
    all_outputs = []
    all_peak_outputs = []
    trees = []

    # process first few standard lines, ignore timestamp
    last_standard_item_id, _ = process_standard_lines(file)

    # Read header to obtain a header dictionary of EnergyPlus
    # outputs and initialize dictionary for output values
    orig_header, init_outputs = read_header(file)
    monitor.header_finished()

    # Read body to obtain outputs and environment dictionaries.
    content = read_body(file, last_standard_item_id, init_outputs, ignore_peaks, monitor)
    monitor.body_finished()

    for out, peak, dates, cumulative_days, days_of_week in zip(*content[1:]):
        # Sort interval line into relevant dictionaries
        dates, n_days = interval_processor(dates, cumulative_days, year)
        monitor.intervals_finished()

        # Create a 'search tree' to allow searching for variables
        header = deepcopy(orig_header)
        tree, dup_ids = create_tree(header)
        trees.append(tree)
        monitor.search_tree_finished()

        if dup_ids:
            # remove duplicates from header and outputs
            remove_duplicates(dup_ids, header, out)

        if not ignore_peaks:
            peak_outputs = generate_peak_outputs(peak, header, dates)
        else:
            peak_outputs = None
        all_peak_outputs.append(peak_outputs)

        # transform standard dictionaries into DataFrame like Output classes
        other_data = {N_DAYS_COLUMN: n_days, DAY_COLUMN: days_of_week}
        outputs = generate_outputs(out, header, dates, other_data)
        all_outputs.append(outputs)

        monitor.output_cls_gen_finished()

    monitor.processing_finished()

    # content[0] stores environment names
    return content[0], all_outputs, all_peak_outputs, trees


def read_file(file_path, monitor=None, ignore_peaks=True, year=2002):
    """ Open the eso file and trigger file processing. """
    if monitor is None:
        monitor = DefaultMonitor(file_path)

    # Initially read the file to check if it's ok
    monitor.processing_started()
    complete = monitor.preprocess()

    if not complete:
        # prevent reading the file when incomplete
        msg = f"File '{file_path} is not complete!'"
        monitor.processing_failed(msg)
        raise IncompleteFile
    try:
        with open(file_path, "r") as file:
            return process_file(file, monitor, year, ignore_peaks=ignore_peaks)

    except BlankLineError:
        msg = f"There's a blank line in file '{file_path}'."
        monitor.processing_failed(msg)
        raise BlankLineError(msg)
