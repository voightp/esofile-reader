import datetime as dt
import logging
import re
from collections import defaultdict
from copy import deepcopy
from functools import partial
from typing import Dict, List

import cython
import numpy as np
import pandas as pd

from esofile_reader.constants import *
from esofile_reader.data.df_data import DFData
from esofile_reader.data.df_functions import create_peak_outputs
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import Variable, IntervalTuple
from esofile_reader.processor.interval_processor import interval_processor
from esofile_reader.processor.monitor import DefaultMonitor
from esofile_reader.search_tree import Tree


def _eso_file_version(raw_version):
    """ Return eso file version as an integer (i.e.: 860, 890). """
    version = raw_version.strip()
    start = version.index(" ")
    return int(version[(start + 1): (start + 6)].replace(".", ""))


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

    # this raises attribute error when there's some unexpected line syntax
    line_id, _, key, type_, units, interval = pattern.search(line).groups()

    # 'type' variable is 'None' for 'Meter' variable
    if type_ is None:
        type_ = key
        key = "Cumulative Meter" if "Cumulative" in key else "Meter"

    return int(line_id), key, type_, units, interval.lower()


def read_header(eso_file, monitor):
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

    """
    # //@formatter:off
    cdef int chunk, counter, id_
    cdef str raw_line, key_nm, var_nm, units, interval
    # //@formatter:on

    header_dct = defaultdict(partial(defaultdict))

    chunk = monitor.chunk_size
    counter = monitor.counter

    while True:
        raw_line = next(eso_file)

        counter += 1
        if counter == chunk:
            monitor.update_progress()
            counter = 0

        try:
            id_, key_nm, var_nm, units, interval = _process_header_line(raw_line)
        except AttributeError:
            if "End of Data Dictionary" in raw_line:
                break
            elif raw_line == "":
                monitor.processing_failed("Empty line!")
                raise BlankLineError
            else:
                msg = f"Unexpected line syntax: '{raw_line}'!"
                monitor.processing_failed(msg)
                raise InvalidLineSyntax(msg)

        header_dct[interval][id_] = Variable(interval, key_nm, var_nm, units)

    return header_dct


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


@cython.boundscheck(False)
@cython.wraparound(True)
@cython.binding(True)
def read_body(eso_file, highest_interval_id, header_dct, ignore_peaks, monitor):
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
    header_dct : dict of {str: dict of {int : []))
        A dictionary of expected eso file results with initialized blank lists.
        This is generated by 'read_header' function.
    ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.
    monitor : DefaultMonitor, CustomMonitor
        A custom class to monitor processing progress

    Returns
    -------
    Processed ESO file data.

     """
    # //@formatter:off
    cdef int chunk, counter, line_id
    cdef list line, split_line
    cdef double res
    cdef list peak_res
    cdef str raw_line, interval

    # initialize bins for all outputs
    cdef list all_outputs = []
    cdef list all_peak_outputs = []
    cdef list all_environments = []
    cdef list all_dates = []
    cdef list all_cumulative_days = []
    cdef list all_days_of_week = []

    cdef dict outputs = {}
    cdef dict peak_outputs = {}
    cdef dict dates = {}
    cdef dict cumulative_days = {}
    cdef dict days_of_week = {}
    # //@formatter:on

    chunk = monitor.chunk_size
    counter = monitor.counter

    while True:
        raw_line = next(eso_file)

        counter += 1
        if counter == chunk:
            monitor.update_progress()
            counter = 0

        try:
            split_line = raw_line.split(",")
            line_id = int(split_line[0])
            line = split_line[1:]
        except ValueError:
            if "End of Data" in raw_line:
                break
            elif raw_line == "":
                monitor.processing_failed(f"Empty line!.")
                raise BlankLineError
            else:
                msg = f"Unexpected line syntax: '{raw_line}'!"
                monitor.processing_failed(msg)
                raise InvalidLineSyntax(msg)

        if line_id <= highest_interval_id:
            if line_id == 1:
                # initialize variables for current environment
                outputs = {}
                peak_outputs = {}
                dates = {}
                cumulative_days = {}
                days_of_week = {}

                # initialize bins for the current environment
                for interval, dct in header_dct.items():
                    if interval in (M, A, RP):
                        cumulative_days[interval] = []
                    else:
                        days_of_week[interval] = []
                    dates[interval] = []

                    outputs[interval] = {}
                    for k in dct.keys():
                        outputs[interval][k] = []
                    if not ignore_peaks:
                        if interval in (D, M, A, RP):
                            peak_outputs[interval] = {}
                            for k, v in dct.items():
                                peak_outputs[interval][k] = []
                    else:
                        peak_outputs = None

                # store current environment data
                all_environments.append(line[0].strip())
                all_outputs.append(outputs)
                all_peak_outputs.append(peak_outputs)
                all_dates.append(dates)
                all_days_of_week.append(days_of_week)
                all_cumulative_days.append(cumulative_days)

            else:
                try:
                    interval, date, other = _process_interval_line(line_id, line)
                except ValueError:
                    msg = f"Unexpected value in line '{raw_line}'."
                    monitor.processing_failed(msg)
                    raise InvalidLineSyntax(msg)

                # Populate last environment list with interval line
                dates[interval].append(date)

                # Populate current step for all result ids with nan values.
                # This is in place to avoid issues for variables which are not
                # reported during current interval
                for v in outputs[interval].values():
                    v.append(np.nan)

                if line_id >= 3 and not ignore_peaks:
                    for v in peak_outputs[interval].values():
                        v.append(np.nan)

                if line_id <= 3:
                    days_of_week[interval].append(other.strip())
                else:
                    cumulative_days[interval].append(other)

        else:
            # current line represents a result, replace nan values from the last step
            peak_res = None
            try:
                if ignore_peaks:
                    res = float(line[0])
                else:
                    res = float(line[0])
                    peak_res = [float(i) if "." in i else int(i) for i in line[1:]]
            except ValueError:
                msg = f"Unexpected value in line '{raw_line}'."
                monitor.processing_failed(msg)
                raise InvalidLineSyntax(msg)

            outputs[interval][line_id][-1] = res
            if peak_res:
                peak_outputs[interval][line_id][-1] = peak_res

    return (
        all_environments,
        all_outputs,
        all_peak_outputs,
        all_dates,
        all_cumulative_days,
        all_days_of_week,
    )


def create_values_df(outputs_dct: Dict[int, Variable], index_name: str) -> pd.DataFrame:
    """ Create a raw values pd.DataFrame for given interval. """
    df = pd.DataFrame(outputs_dct)
    df = df.T
    df.index.set_names(index_name, inplace=True)
    return df


def create_header_df(
        header_dct: Dict[int, Variable], interval: str, index_name: str, columns: List[str]
) -> pd.DataFrame:
    """ Create a raw header pd.DataFrame for given interval. """
    rows, index = [], []
    for id_, var in header_dct.items():
        rows.append([interval, var.key, var.type, var.units])
        index.append(id_)

    return pd.DataFrame(rows, columns=columns, index=pd.Index(index, name=index_name))


def generate_peak_outputs(raw_peak_outputs, header, dates, monitor, step):
    """ Transform processed peak output data into DataFrame like classes. """
    min_peaks = DFData()
    max_peaks = DFData()

    for interval, values in raw_peak_outputs.items():
        df_values = create_values_df(values, ID_LEVEL)
        df_header = create_header_df(
            header[interval], interval, ID_LEVEL, COLUMN_LEVELS[1:]
        )

        df = pd.merge(df_header, df_values, sort=False, left_index=True, right_index=True)

        df.set_index(keys=COLUMN_LEVELS[1:], append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        min_df = create_peak_outputs(interval, df, max_=False)
        min_peaks.populate_table(interval, min_df)

        max_df = create_peak_outputs(interval, df)
        max_peaks.populate_table(interval, max_df)

        monitor.update_progress(i=step)

    # Peak outputs are stored in dictionary to distinguish min and max
    peak_outputs = {"local_min": min_peaks, "local_max": max_peaks}

    return peak_outputs


def generate_outputs(raw_outputs, header, dates, other_data, monitor, step):
    """ Transform processed output data into DataFrame like classes. """
    outputs = DFData()

    for interval, values in raw_outputs.items():
        df_values = create_values_df(values, ID_LEVEL)
        df_header = create_header_df(
            header[interval], interval, ID_LEVEL, COLUMN_LEVELS[1:]
        )

        df = pd.merge(df_header, df_values, sort=False, left_index=True, right_index=True)

        df.set_index(keys=COLUMN_LEVELS[1:], append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        # add other special columns
        for k, v in other_data.items():
            try:
                # all special columns have SPECIAL as id
                column_index = (SPECIAL, interval, k, "", "")
                column = v.pop(interval)
                df.insert(0, column_index, column)
            except KeyError:
                pass

        # store the data in  DFData class
        outputs.populate_table(interval, df)
        monitor.update_progress(i=step)

    return outputs


def remove_duplicates(dup_ids, header_dct, outputs_dct):
    """ Remove duplicate outputs from results set. """
    for id_, v in dup_ids.items():
        logging.info(
            f"Duplicate variable found, removing variable id: '{id_}' - "
            f"{v.interval} | {v.key} | {v.type} | {v.units}."
        )
        for dct in [header_dct, outputs_dct]:
            try:
                del dct[v.interval][id_]
            except KeyError:
                pass


def process_file(file, monitor, year, ignore_peaks=True):
    """ Process raw EnergyPlus output file. """
    # //@formatter:off
    cdef int last_standard_item_id
    cdef list all_outputs = []
    cdef list all_peak_outputs = []
    cdef list trees = []
    # //@formatter:on

    # process first few standard lines, ignore timestamp
    version, timestamp = _process_statement(next(file))
    monitor.counter += 1
    last_standard_item_id = 6 if version >= 890 else 5

    # Skip standard reporting intervals
    for _ in range(last_standard_item_id):
        next(file)
        monitor.counter += 1

    # Read header to obtain a header dictionary of EnergyPlus
    # outputs and initialize dictionary for output values
    monitor.header_started()
    orig_header = read_header(file, monitor)

    # Read body to obtain outputs and environment dictionaries
    monitor.body_started()
    content = read_body(file, last_standard_item_id, orig_header, ignore_peaks, monitor)

    # Get a fraction for each table and tree generated
    environments = content[0]
    n_tables = len(orig_header.keys()) if ignore_peaks else len(orig_header.keys()) * 2
    n_steps = n_tables * len(environments) + len(environments)
    step = (monitor.max_progress * (1 - monitor.PROGRESS_FRACTION)) / n_steps

    for out, peak, dates, cumulative_days, days_of_week in zip(*content[1:]):
        # Generate datetime data
        monitor.intervals_started()
        dates, n_days = interval_processor(dates, cumulative_days, year)

        # Create a 'search tree' to allow searching for variables
        monitor.search_tree_started()
        header = deepcopy(orig_header)

        tree = Tree()
        dup_ids = tree.populate_tree(header)
        trees.append(tree)
        monitor.update_progress(step)

        if dup_ids:
            # remove duplicates from header and outputs
            remove_duplicates(dup_ids, header, out)

        monitor.peak_outputs_started(ignore_peaks)
        if not ignore_peaks:
            peak_outputs = generate_peak_outputs(peak, header, dates, monitor, step)
        else:
            peak_outputs = None
        all_peak_outputs.append(peak_outputs)

        # transform standard dictionaries into DataFrame like Output classes
        monitor.outputs_started()
        other_data = {N_DAYS_COLUMN: n_days, DAY_COLUMN: days_of_week}
        outputs = generate_outputs(out, header, dates, other_data, monitor, step)
        all_outputs.append(outputs)

    # update progress to compensate for reminder
    if monitor.progress != monitor.max_progress:
        monitor.update_progress()

    monitor.processing_finished()

    return environments, all_outputs, all_peak_outputs, trees


def read_file(file_path, monitor=None, ignore_peaks=True, year=2002):
    """ Open the eso file and trigger file processing. """
    if monitor is None:
        monitor = DefaultMonitor(file_path)
    monitor.processing_started()

    # count number of lines to report progress
    # //@formatter:off
    cdef int i
    # //@formatter:on
    with open(file_path, "rb") as f:
        i = 0
        for _ in f:
            i += 1
    monitor.set_chunk_size(n_lines=i + 1)

    try:
        with open(file_path, "r") as file:
            return process_file(file, monitor, year, ignore_peaks=ignore_peaks)

    except StopIteration:
        msg = f"File is not complete!"
        monitor.processing_failed(msg)
        raise IncompleteFile(msg)
