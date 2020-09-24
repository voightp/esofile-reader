import logging
import re
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Dict, List, Tuple, TextIO, Optional, Union

import cython
import numpy as np
import pandas as pd

from esofile_reader.constants import *
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import Variable, IntervalTuple
from esofile_reader.processing.esofile_intervals import process_raw_date_data
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.search_tree import Tree
from esofile_reader.tables.df_functions import create_peak_min_outputs, create_peak_max_outputs
from esofile_reader.tables.df_tables import DFTables


def get_eso_file_version(raw_version: str) -> int:
    """ Return eso file version as an integer (i.e.: 860, 890). """
    version = raw_version.strip()
    start = version.index(" ")
    return int(version[(start + 1): (start + 6)].replace(".", ""))


def get_eso_file_timestamp(timestamp: str) -> datetime:
    """ Return date and time of the eso file generation as a Datetime. """
    timestamp = timestamp.split("=")[1].strip()
    return datetime.strptime(timestamp, "%Y.%m.%d %H:%M")


def process_statement_line(line: str) -> Tuple[int, datetime]:
    """ Extract the version and time of the file generation. """
    _, _, raw_version, timestamp = line.split(",")
    version = get_eso_file_version(raw_version)
    timestamp = get_eso_file_timestamp(timestamp)
    return version, timestamp


def process_header_line(line: str) -> Tuple[int, str, str, str, str]:
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


def read_header(eso_file: TextIO, progress_logger: EsoFileProgressLogger) -> Dict[
    str, Dict[int, Variable]]:
    """
    Read header dictionary of the eso file.

    The file is being read line by line until the 'End of TableType Dictionary'
    is reached. Raw line is processed and the line is added as an item to
    the header_dict dictionary. The outputs dictionary is populated with
    dictionaries using output ids as keys and blank lists as values
    (to be populated later).

    Parameters
    ----------
    eso_file : EsoFile
        Opened EnergyPlus result file.
    progress_logger: EsoFileProgressLogger
        Watcher to report processing progress.

    Returns
    -------
    dict of {str : dict of {int : tuple)}
        A dictionary of eso file header line with populated values.

    """
    # //@formatter:off
    cdef int chunk_size, counter, id_
    cdef str raw_line, key, type_, units, interval
    # //@formatter:on

    header = defaultdict(partial(defaultdict))
    counter = progress_logger.line_counter % progress_logger.CHUNK_SIZE
    chunk_size = progress_logger.CHUNK_SIZE
    while True:
        raw_line = next(eso_file)

        counter += 1
        if counter == chunk_size:
            progress_logger.increment_progress()
            progress_logger.line_counter += counter
            counter = 0

        try:
            id_, key, type_, units, interval = process_header_line(raw_line)
        except AttributeError:
            if "End of Data Dictionary" in raw_line:
                progress_logger.line_counter += counter
                break
            elif raw_line == "":
                raise BlankLineError("Empty line!")
            else:
                raise InvalidLineSyntax(f"Unexpected line syntax: '{raw_line}'!")

        header[interval][id_] = Variable(interval, key, type_, units)

    return header


def process_sub_monthly_interval_lines(
    line_id: int, data: List[str]
) -> Tuple[str, IntervalTuple, str]:
    """
    Process sub-hourly, hourly and daily interval line.

    Parameters
    ----------
    line_id : int
        An id of the interval.
    data : list of str
        Line line passed as a list of strings (without ID).

    Note
    ----
    Data by given interval:
        timestep, hourly : [Day of Simulation, Month, Day of Month,
                        DST Indicator, Hour, StartMinute, EndMinute, DayType]
        daily : [Cumulative Day of Simulation, Month, Day of Month,DST Indicator, DayType]

    Returns
    -------
        Interval identifier and numeric date time information and day of week..

    """

    def hourly_interval():
        """ Process TS or H interval entry and return interval identifier. """
        # omit day of week in conversion
        i = [int(float(item)) for item in data[:-1]]
        interval = IntervalTuple(i[1], i[2], i[4], i[6])

        # check if interval is timestep or hourly interval
        if i[5] == 0 and i[6] == 60:
            return H, interval, data[-1].strip()
        else:
            return TS, interval, data[-1].strip()

    def daily_interval():
        """ Populate D list and return identifier. """
        # omit day of week in in conversion
        i = [int(item) for item in data[:-1]]
        return D, IntervalTuple(i[1], i[2], 0, 0), data[-1].strip()

    categories = {
        2: hourly_interval,
        3: daily_interval,
    }

    return categories[line_id]()


def process_monthly_plus_interval_lines(
    line_id: int, data: List[str]
) -> Tuple[str, IntervalTuple, Optional[int]]:
    """
    Process sub-hourly, hourly and daily interval line.

    Parameters
    ----------
    line_id : int
        An id of the interval.
    data : list of str
        Line line passed as a list of strings (without ID).

    Note
    ----
    Data by given interval:
        monthly : [Cumulative Day of Simulation, Month]
        annual : [Year] (only when custom weather file is used) otherwise [int]
        runperiod :  [Cumulative Day of Simulation]

    Returns
    -------
        Interval identifier and numeric date time information and day of week..

    """

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
        4: monthly_interval,
        5: runperiod_interval,
        6: annual_interval,
    }

    return categories[line_id]()


@cython.boundscheck(False)
@cython.wraparound(True)
@cython.binding(True)
def read_body(
    eso_file: TextIO,
    highest_interval_id: int,
    header: Dict[str, Dict[int, Variable]],
    ignore_peaks: bool,
    progress_logger: EsoFileProgressLogger
) -> Tuple[
    List[str],
    List[Dict[str, dict]],
    List[Optional[Dict[str, dict]]],
    List[Dict[str, list]],
    List[Dict[str, list]],
    List[Dict[str, list]],
    Dict[str, Dict[int, Variable]]
]:
    """
    Read body of the eso file.

    The line from eso file is processed line by line until the
    'End of TableType' is reached. Interval line is stored in the 'envs'
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
    header : dict of {str: dict of {int : []))
        A dictionary of expected eso file results with initialized blank lists.
        This is generated by 'read_header' function.
    ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.
    progress_logger : EsoFileProgressLogger
        A custom class to progress_logger processing progress

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

    counter = progress_logger.line_counter % progress_logger.CHUNK_SIZE
    chunk_size = progress_logger.CHUNK_SIZE
    while True:
        raw_line = next(eso_file)

        counter += 1
        if counter == chunk_size:
            progress_logger.increment_progress()
            progress_logger.line_counter += counter
            counter = 0

        try:
            split_line = raw_line.split(",")
            line_id = int(split_line[0])
            line = split_line[1:]
        except ValueError:
            if "End of Data" in raw_line:
                progress_logger.line_counter += counter
                break
            elif raw_line == "":
                raise BlankLineError("Empty line!")
            else:
                raise InvalidLineSyntax(f"Unexpected line syntax: '{raw_line}'!")

        if line_id <= highest_interval_id:
            if line_id == 1:
                # initialize variables for current environment
                outputs = {}
                peak_outputs = {}
                dates = {}
                cumulative_days = {}
                days_of_week = {}

                # initialize bins for the current environment
                for interval, dct in header.items():
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
                    if line_id > 3:
                        interval, date, n_days = process_monthly_plus_interval_lines(line_id,
                                                                                     line)
                        cumulative_days[interval].append(n_days)
                    else:
                        interval, date, day = process_sub_monthly_interval_lines(line_id, line)
                        days_of_week[interval].append(day)
                except ValueError:
                    raise InvalidLineSyntax(f"Unexpected value in line '{raw_line}'.")

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
                raise InvalidLineSyntax(f"Unexpected value in line '{raw_line}'.")

            outputs[interval][line_id][-1] = res
            if peak_res:
                peak_outputs[interval][line_id][-1] = peak_res

    # update progress to compensate for reminder
    if progress_logger.progress != progress_logger.max_progress:
        progress_logger.increment_progress()

    return (
        all_environments,
        all_outputs,
        all_peak_outputs,
        all_dates,
        all_cumulative_days,
        all_days_of_week,
        header,
    )


def create_values_df(outputs_dct: Dict[int, List[float]], index_name: str) -> pd.DataFrame:
    """ Create a raw values pd.DataFrame for given interval. """
    df = pd.DataFrame(outputs_dct)
    df = df.T
    df.index.set_names(index_name, inplace=True)
    return df


def create_header_df(
    header: Dict[int, Variable], interval: str, index_name: str, columns: List[str]
) -> pd.DataFrame:
    """ Create a raw header pd.DataFrame for given interval. """
    rows, index = [], []
    for id_, var in header.items():
        rows.append([interval, var.key, var.type, var.units])
        index.append(id_)

    return pd.DataFrame(rows, columns=columns, index=pd.Index(index, name=index_name))


def generate_peak_tables(
    raw_peak_outputs: Dict[str, Dict[int, List[float]]],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str,],
    progress_logger: EsoFileProgressLogger,
) -> Dict[str, DFTables]:
    """ Transform processed peak output data into DataFrame like classes. """
    min_peaks = DFTables()
    max_peaks = DFTables()
    for interval, values in raw_peak_outputs.items():
        df_values = create_values_df(values, ID_LEVEL)
        df_header = create_header_df(
            header[interval], interval, ID_LEVEL, COLUMN_LEVELS[1:]
        )
        df = pd.merge(df_header, df_values, sort=False, left_index=True, right_index=True)
        df.set_index(keys=list(COLUMN_LEVELS[1:]), append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        min_df = create_peak_min_outputs(interval, df)
        min_peaks[interval] = min_df

        max_df = create_peak_max_outputs(interval, df)
        max_peaks[interval] = max_df

        progress_logger.increment_progress()

    # Peak outputs are stored in dictionary to distinguish min and max
    peak_outputs = {"local_min": min_peaks, "local_max": max_peaks}

    return peak_outputs


def generate_df_tables(
    raw_outputs: Dict[str, Dict[int, List[float]]],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str,],
    other_data: Dict[str, Dict[str, list]],
    progress_logger: EsoFileProgressLogger,
) -> DFTables:
    """ Transform processed output data into DataFrame like classes. """
    tables = DFTables()
    for interval, values in raw_outputs.items():
        df_values = create_values_df(values, ID_LEVEL)
        df_header = create_header_df(
            header[interval], interval, ID_LEVEL, COLUMN_LEVELS[1:]
        )
        df = pd.merge(df_header, df_values, sort=False, left_index=True, right_index=True)
        df.set_index(keys=list(COLUMN_LEVELS[1:]), append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        # store the data in  DFTables class
        tables[interval] = df

        progress_logger.increment_progress()

    # add other special columns, structure is {KEY: {INTERVAL: ARRAY}}
    for key, dict in other_data.items():
        for interval, arr in dict.items():
            tables.insert_special_column(interval, key, arr)

    return tables


def remove_duplicates(
    duplicates: Dict[int, Variable],
    header: Dict[str, Dict[int, Variable]],
    outputs: Dict[str, Dict[int, List[float]]]
) -> None:
    """ Remove duplicate outputs from results set. """
    for id_, v in duplicates.items():
        logging.info(f"Duplicate variable found, removing variable: '{id_} - {v}'.")
        for dct in [header, outputs]:
            try:
                del dct[v.table][id_]
            except KeyError:
                pass


def count_tables(outputs: List[Optional[Dict[str, dict]]]) -> int:
    return sum([sum([len(dct.keys()) for dct in outputs if dct is not None])])


def process_file_content(
    all_outputs: List[Dict[str, dict]],
    all_peak_outputs: List[Optional[Dict[str, dict]]],
    all_dates: List[Dict[str, list]],
    all_cumulative_days: List[Dict[str, list]],
    all_days_of_week: List[Dict[str, list]],
    original_header: Dict[str, Dict[int, Variable]],
    year: int,
    progress_logger: EsoFileProgressLogger
) -> Tuple[List[DFTables], List[Optional[Dict[str, DFTables]]], List[Tree]]:
    # //@formatter:off
    cdef list all_df_tables = []
    cdef list all_peak_df_tables = []
    cdef list all_trees = []
    # //@formatter:on
    zipped_args = zip(
        all_outputs, all_peak_outputs, all_dates, all_cumulative_days, all_days_of_week
    )
    n_tables = count_tables(all_outputs) + count_tables(all_peak_outputs)
    n_trees = len(all_outputs)
    progress_logger.set_new_maximum_progress(n_tables + n_trees)
    for outputs, peak_outputs, dates, cumulative_days, days_of_week in zipped_args:
        # Generate datetime data
        progress_logger.log_section("processing dates!")
        dates, n_days = process_raw_date_data(dates, cumulative_days, year)

        # Create a 'search tree' to allow searching for variables
        progress_logger.log_section("generating search tree!")
        header = deepcopy(original_header)

        try:
            tree = Tree.from_header_dict(header)
        except DuplicateVariable as e:
            tree = e.clean_tree
            remove_duplicates(e.duplicates, header, outputs)

        all_trees.append(tree)
        progress_logger.increment_progress()

        if peak_outputs is None:
            peak_df_tables = None
        else:
            progress_logger.log_section("generating peak tables!")
            peak_df_tables = generate_peak_tables(peak_outputs, header, dates, progress_logger)

        all_peak_df_tables.append(peak_df_tables)

        # transform standard dictionaries into DataFrame like Output classes
        progress_logger.log_section("generating tables!")
        other_data = {N_DAYS_COLUMN: n_days, DAY_COLUMN: days_of_week}
        df_tables = generate_df_tables(outputs, header, dates, other_data, progress_logger)
        all_df_tables.append(df_tables)

    progress_logger.log_section("creating class instance!")

    return all_df_tables, all_peak_df_tables, all_trees


def read_file(
    file: TextIO, progress_logger: EsoFileProgressLogger, ignore_peaks: bool = True
) -> Tuple[
    List[str],
    List[Dict[str, dict]],
    List[Optional[Dict[str, dict]]],
    List[Dict[str, list]],
    List[Dict[str, list]],
    List[Dict[str, list]],
    Dict[str, Dict[int, Variable]]
]:
    """ Read raw EnergyPlus output file. """
    # //@formatter:off
    cdef int last_standard_item_id
    # //@formatter:on

    # process first few standard lines, ignore timestamp
    version, timestamp = process_statement_line(next(file))
    progress_logger.line_counter += 1
    last_standard_item_id = 6 if version >= 890 else 5

    # Skip standard reporting intervals
    for _ in range(last_standard_item_id):
        next(file)
        progress_logger.line_counter += 1

    # Read header to obtain a header dictionary of EnergyPlus
    # outputs and initialize dictionary for output values
    progress_logger.log_section("processing data dictionary!")
    header = read_header(file, progress_logger)

    # Read body to obtain outputs and environment dictionaries
    progress_logger.log_section("processing data!")

    return read_body(file, last_standard_item_id, header, ignore_peaks, progress_logger)


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.binding(True)
def count_lines(file_path: Union[str, Path]):
    # //@formatter:off
    cdef int i
    # //@formatter:on
    with open(file_path, "rb") as f:
        i = 0
        for _ in f:
            i += 1
    i += 1
    return i


def preprocess_file(
    file_path: Union[str, Path], progress_logger: EsoFileProgressLogger
) -> None:
    """ Set maximum progress for eso file processing. """
    progress_logger.log_section("pre-processing!")
    n_lines = count_lines(file_path)
    maximum = n_lines // progress_logger.CHUNK_SIZE
    progress_logger.set_new_maximum_progress(maximum)


def process_eso_file(
    file_path: Union[str, Path],
    progress_logger: EsoFileProgressLogger,
    ignore_peaks: bool = True,
    year: int = 2002
) -> Tuple[List[str], List[DFTables], List[Optional[Dict[str, DFTables]]], List[Tree]]:
    """ Open the eso file and trigger file processing. """
    preprocess_file(file_path, progress_logger)
    try:
        with open(file_path, "r") as file:
            content = read_file(file, progress_logger, ignore_peaks=ignore_peaks)
    except StopIteration:
        raise IncompleteFile(f"File is not complete!")
    (
        all_outputs,
        all_peak_outputs,
        all_trees
    ) = process_file_content(*content[1:], year, progress_logger)
    all_environments = content[0]
    return all_environments, all_outputs, all_peak_outputs, all_trees
