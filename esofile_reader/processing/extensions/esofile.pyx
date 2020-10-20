import re
from collections import defaultdict
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Dict, List, Tuple, TextIO, Optional, Union
import cython

from esofile_reader.constants import *
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import Variable, IntervalTuple
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.processing.raw_outputs import RawOutputs, RawDFOutputs
from esofile_reader.search_tree import Tree
from esofile_reader.df.df_tables import DFTables

ENVIRONMENT_LINE = 1
TIMESTEP_OR_HOURLY_LINE = 2
DAILY_LINE = 3
MONTHLY_LINE = 4
RUNPERIOD_LINE = 5
ANNUAL_LINE = 6


cpdef int get_eso_file_version(str raw_version):
    """ Return eso file version as an integer (i.e.: 860, 890). """
    version = raw_version.strip()
    start = version.index(" ")
    return int(version[(start + 1): (start + 6)].replace(".", ""))


cpdef object get_eso_file_timestamp(str timestamp):
    """ Return date and time of the eso file generation as a Datetime. """
    timestamp = timestamp.split("=")[1].strip()
    return datetime.strptime(timestamp, "%Y.%m.%d %H:%M")


cpdef tuple process_statement_line(str line):
    """ Extract the version and time of the file generation. """
    _, _, raw_version, timestamp = line.split(",")
    version = get_eso_file_version(raw_version)
    timestamp = get_eso_file_timestamp(timestamp)
    return version, timestamp


cpdef tuple process_header_line(str line):
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
    # //@formatter:off
    cdef str raw_line_id, _, key, type_, units, interval
    cdef int line_id
    # //@formatter:on

    pattern = re.compile("^(\d+),(\d+),(.*?)(?:,(.*?) ?\[| ?\[)(.*?)\] !(\w*)")

    # this raises attribute error when there's some unexpected line syntax
    raw_line_id, _, key, type_, units, interval = pattern.search(line).groups()
    line_id = int(raw_line_id)

    # 'type' variable is 'None' for 'Meter' variable
    if type_ is None:
        type_ = key
        key = "Cumulative Meter" if "Cumulative" in key else "Meter"

    return line_id, key, type_, units, interval.lower()


@cython.boundscheck(False)
@cython.wraparound(True)
@cython.binding(True)
cpdef object read_header(object eso_file, object progress_logger):
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
    cdef int chunk_size, counter, line_id
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
            line_id, key, type_, units, interval = process_header_line(raw_line)
        except AttributeError:
            if "End of Data Dictionary" in raw_line:
                progress_logger.line_counter += counter
                break
            elif raw_line == "\n":
                raise BlankLineError("Empty line!")
            else:
                raise InvalidLineSyntax(f"Unexpected line syntax: '{raw_line}'!")

        header[interval][line_id] = Variable(interval, key, type_, units)

    return header


def process_sub_monthly_interval_line(
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
    cdef list items

    def parse_timestep_or_hourly_interval():
        """ Process TS or H interval entry and return interval identifier. """
        # omit day of week in conversion
        items = [int(float(item)) for item in data[:-1]]
        interval = IntervalTuple(items[1], items[2], items[4], items[6])

        # check if interval is timestep or hourly interval
        if items[5] == 0 and items[6] == 60:
            return H, interval, data[-1].strip()
        else:
            return TS, interval, data[-1].strip()

    def parse_daily_interval():
        """ Populate D list and return identifier. """
        # omit day of week in in conversion
        i = [int(item) for item in data[:-1]]
        return D, IntervalTuple(i[1], i[2], 0, 0), data[-1].strip()

    categories = {
        TIMESTEP_OR_HOURLY_LINE: parse_timestep_or_hourly_interval,
        DAILY_LINE: parse_daily_interval,
    }

    return categories[line_id]()


def process_monthly_plus_interval_line(
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

    def parse_monthly_interval():
        """ Populate M list and return identifier. """
        return M, IntervalTuple(int(data[1]), 1, 0, 0), int(data[0])

    def parse_runperiod_interval():
        """ Populate RP list and return identifier. """
        return RP, IntervalTuple(1, 1, 0, 0), int(data[0])

    def parse_annual_interval():
        """ Populate A list and return identifier. """
        return A, IntervalTuple(1, 1, 0, 0), None

    categories = {
        MONTHLY_LINE: parse_monthly_interval,
        RUNPERIOD_LINE: parse_runperiod_interval,
        ANNUAL_LINE: parse_annual_interval,
    }
    return categories[line_id]()


@cython.boundscheck(False)
@cython.wraparound(True)
@cython.binding(True)
cpdef list read_body(
    object eso_file,
    int highest_interval_id,
    object header,
    object ignore_peaks,
    object progress_logger
):
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
    cdef list all_raw_outputs = []
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

        # process raw line, leave this in while loop to avoid function call overhead
        try:
            split_line = raw_line.split(",")
            line_id = int(split_line[0])
            line = split_line[1:]
        except ValueError:
            if "End of Data" in raw_line:
                progress_logger.line_counter += counter
                break
            elif raw_line == "\n":
                raise BlankLineError("Empty line!")
            else:
                raise InvalidLineSyntax(f"Unexpected line syntax: '{raw_line}'!")

        # distribute outputs into relevant bins
        if line_id <= highest_interval_id:
            if line_id == ENVIRONMENT_LINE:
                # initialize variables for current environment
                environment_name = line[0].strip()
                raw_outputs = RawOutputs(environment_name, header, ignore_peaks)
                all_raw_outputs.append(raw_outputs)
            else:
                try:
                    if line_id > DAILY_LINE:
                        interval, date, n_days = process_monthly_plus_interval_line(line_id,
                                                                                    line)
                        raw_outputs.cumulative_days[interval].append(n_days)
                    else:
                        interval, date, day = process_sub_monthly_interval_line(line_id, line)
                        raw_outputs.days_of_week[interval].append(day)
                except ValueError:
                    raise InvalidLineSyntax(f"Unexpected value in line '{raw_line}'.")

                # Populate last environment list with interval line
                raw_outputs.dates[interval].append(date)

                # Populate current step for all result ids with nan values.
                # This is in place to avoid issues for variables which are not
                # reported during current interval
                raw_outputs.initialize_next_outputs_step(interval)
                if not ignore_peaks and line_id >= DAILY_LINE:
                    raw_outputs.initialize_next_peak_outputs_step(interval)
        else:
            # current line represents a result, replace nan values from the last step
            try:
                res = float(line[0])
                raw_outputs.outputs[interval][line_id][-1] = res
                if not ignore_peaks and interval in {D, M, A, RP}:
                    peak_res = [float(i) if "." in i else int(i) for i in line[1:]]
                    raw_outputs.peak_outputs[interval][line_id][-1] = peak_res
            except ValueError:
                raise InvalidLineSyntax(f"Unexpected value in line '{raw_line}'.")

    # update progress to compensate for reminder
    if progress_logger.progress != progress_logger.max_progress:
        progress_logger.increment_progress()

    return all_raw_outputs


def count_tables(all_raw_outputs: List[RawOutputs]) -> int:
    return sum(raw_outputs.get_n_tables() for raw_outputs in all_raw_outputs)


def process_raw_file_content(
    all_raw_outputs: List[RawOutputs],
    year: int,
    progress_logger: EsoFileProgressLogger
) -> List[RawDFOutputs]:
    all_raw_df_outputs = []
    n_tables = count_tables(all_raw_outputs)
    n_trees = len(all_raw_outputs)
    progress_logger.set_new_maximum_progress(n_tables + n_trees)
    for raw_outputs in all_raw_outputs:
        raw_df_outputs = RawDFOutputs.from_raw_outputs(raw_outputs, progress_logger, year)
        all_raw_df_outputs.append(raw_df_outputs)
    return all_raw_df_outputs


cpdef read_file(
    object file,object progress_logger,object ignore_peaks = True
):
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
cpdef count_lines(object file_path):
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
    progress_logger.n_lines = n_lines
    progress_logger.set_new_maximum_progress(maximum)


cpdef process_eso_file(
    object file_path,
    object progress_logger,
    object ignore_peaks,
    int year = 2002
):
    """ Open the eso file and trigger file processing. """
    preprocess_file(file_path, progress_logger)
    try:
        with open(file_path, "r") as file:
            all_raw_outputs = read_file(file, progress_logger, ignore_peaks=ignore_peaks)
    except StopIteration:
        raise IncompleteFile(f"File is not complete!")
    return process_raw_file_content(all_raw_outputs, year, progress_logger)
