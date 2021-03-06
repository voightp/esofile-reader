import re
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from functools import partial
from typing import Tuple

import cython

from esofile_reader.exceptions import *
from esofile_reader.typehints import Variable
from esofile_reader.processing.eplus.esofile_time import EsoTimestamp
from esofile_reader.processing.eplus import TS, H, D, M, A, RP
from esofile_reader.processing.eplus.raw_data import RawEsoData

ENVIRONMENT_LINE = 1
TIMESTEP_OR_HOURLY_LINE = 2
DAILY_LINE = 3
MONTHLY_LINE = 4
RUNPERIOD_LINE = 5
ANNUAL_LINE = 6


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
    # //@formatter:off
    cdef str raw_line_id, _, key, type_, units, interval
    cdef int line_id
    # //@formatter:on

    pattern = re.compile(
        "^(\d+),(\d+),(.*?)(?:,(.*?) ?\[| ?\[)(.*?)\] !(\w*(?: \w+)?).*$"
    )

    # this raises attribute error when there's some unexpected line syntax
    raw_line_id, _, key, type_, units, interval = pattern.search(line).groups()
    line_id = int(raw_line_id)

    # 'type' variable is 'None' for 'Meter' variable
    if type_ is None:
        type_ = key
        key = "Cumulative Meter" if "Cumulative" in key else "Meter"

    # regex matches only 'Each' from 'Each Call', since it's reported in TimeStep
    # put it into the same bin, if this would duplicate other variable, it will be deleted
    if interval == "Each Call":
        type_ = "System - " + type_
        interval = "TimeStep"

    return line_id, key, type_, units, interval.lower()


@cython.boundscheck(False)
@cython.wraparound(True)
@cython.binding(True)
cpdef object read_header(object eso_file, object logger):
    """
    Read header dictionary of the eso file.

    The file is being read line by line until the 'End of TableType Dictionary'
    is reached. Raw line is processed and the line is added as an item to
    the header_dict dictionary. 

    Parameters
    ----------
    eso_file : EsoFile
        Opened EnergyPlus result file.
    logger: BaseLogger
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
    counter = logger.line_counter % logger.CHUNK_SIZE
    chunk_size = logger.CHUNK_SIZE
    while True:
        raw_line = next(eso_file)

        counter += 1
        if counter == chunk_size:
            logger.increment_progress()
            logger.line_counter += counter
            counter = 0

        try:
            line_id, key, type_, units, interval = process_header_line(raw_line)
        except AttributeError:
            if "End of Data Dictionary" in raw_line:
                logger.line_counter += counter
                break
            elif raw_line == "\n":
                raise BlankLineError("Empty line!")
            else:
                raise InvalidLineSyntax(f"Unexpected line syntax: '{raw_line}'!")

        header[interval][line_id] = Variable(interval, key, type_, units)

    return header


def process_sub_monthly_interval_line(
    line_id: int, data: List[str]
) -> Tuple[str, EsoTimestamp, str]:
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
        end_minute = int(Decimal(data[6]).to_integral(rounding=ROUND_HALF_UP))
        interval = EsoTimestamp(
            int(data[1]), int(data[2]), int(data[4]), end_minute
        )
        if float(data[5]) == 0 and end_minute == 60:
            return H, interval, data[-1].strip()
        else:
            return TS, interval, data[-1].strip()

    def parse_daily_interval():
        """ Populate D list and return identifier. """
        return D, EsoTimestamp(int(data[1]), int(data[2]), 0, 0), data[-1].strip()

    categories = {
        TIMESTEP_OR_HOURLY_LINE: parse_timestep_or_hourly_interval,
        DAILY_LINE: parse_daily_interval,
    }

    return categories[line_id]()


def process_monthly_plus_interval_line(
    line_id: int, data: List[str]
) -> Tuple[str, EsoTimestamp, Optional[int]]:
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
        return M, EsoTimestamp(int(data[1]), 1, 0, 0), int(data[0])

    def parse_runperiod_interval():
        """ Populate RP list and return identifier. """
        return RP, EsoTimestamp(1, 1, 0, 0), int(data[0])

    def parse_annual_interval():
        """ Populate A list and return identifier. """
        return A, EsoTimestamp(1, 1, 0, 0), None

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
    object logger
):
    """
    Read body of the eso file.

    The line from eso file is processed line by line until the
    'End of Data' is reached. Outputs, dates, days of week are
    distributed into relevant bins in RawEsoData container.

    Index 1-5 for eso file generated prior to E+ 8.9 or 1-6 from E+ 8.9
    further, indicates that line is an interval.

    Parameters
    ----------
    eso_file : EsoFile
        Opened EnergyPlus result file.
    highest_interval_id : int
        A maximum index defining an interval (higher is considered a result)
    header : dict of {str: dict of {int : Variable))
        A dictionary of expected eso file results with initialized blank lists.
        This is generated by 'read_header' function.
    ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.
    logger : BaseLogger
        A custom class to logger processing progress

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
    cdef list all_raw_data = []
    # //@formatter:on

    counter = logger.line_counter % logger.CHUNK_SIZE
    chunk_size = logger.CHUNK_SIZE
    while True:
        raw_line = next(eso_file)
        counter += 1
        if counter == chunk_size:
            logger.increment_progress()
            logger.line_counter += counter
            counter = 0

        # process raw line, leave this in while loop to avoid function call overhead
        try:
            split_line = raw_line.split(",")
            line_id = int(split_line[0])
            line = split_line[1:]
        except ValueError:
            if "End of Data" in raw_line:
                logger.line_counter += counter
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
                raw_outputs = RawEsoData(environment_name, deepcopy(header), ignore_peaks)
                all_raw_data.append(raw_outputs)
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
    if logger.progress != logger.max_progress:
        logger.increment_progress()

    return all_raw_data


def read_file(
    file: TextIO, logger: BaseLogger, ignore_peaks: bool = True
) -> List[RawEsoData]:
    """ Read raw EnergyPlus output file. """
    # //@formatter:off
    cdef int last_standard_item_id
    # //@formatter:on

    # process first few standard lines, ignore timestamp
    version, timestamp = process_statement_line(next(file))
    logger.line_counter += 1
    last_standard_item_id = 6 if version >= 890 else 5

    # Skip standard reporting intervals
    for _ in range(last_standard_item_id):
        next(file)
        logger.line_counter += 1

    # Read header to obtain a header dictionary of EnergyPlus
    # outputs and initialize dictionary for output values
    logger.log_section("processing data dictionary")
    header = read_header(file, logger)

    # Read body to obtain outputs and environment dictionaries
    logger.log_section("processing data")
    return read_body(file, last_standard_item_id, header, ignore_peaks, logger)



@cython.boundscheck(False)
@cython.wraparound(False)
@cython.binding(True)
cdef int count_lines(object file_path):
    # //@formatter:off
    cdef int i
    # //@formatter:on
    with open(file_path, "rb") as f:
        i = 0
        for _ in f:
            i += 1
    i += 1
    return i


def preprocess_file(file_path: Union[str, Path], logger: BaseLogger) -> None:
    """ Set maximum progress for eso file processing. """
    logger.log_section("pre-processing")
    n_lines = count_lines(file_path)
    maximum = n_lines // logger.CHUNK_SIZE
    logger.n_lines = n_lines
    logger.set_maximum_progress(maximum)


def process_eso_file(
    file_path: Union[str, Path], logger: BaseLogger, ignore_peaks: bool = True,
) -> List[RawEsoData]:
    """ Open the eso file and trigger file processing. """
    preprocess_file(file_path, logger)
    try:
        with open(file_path, "r") as file:
            return read_file(file, logger, ignore_peaks=ignore_peaks)
    except StopIteration:
        raise IncompleteFile(f"File is not complete!")
