from datetime import datetime
from typing import Tuple, List, Dict, Optional

from esofile_reader.constants import *
from esofile_reader.mini_classes import IntervalTuple
import calendar

MONTH_END_DAYS = {
    1: 31,
    2: 28,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}


def datetime_helper(month: int, day: int, hour: int, end_minute: int) -> Tuple[int, ...]:
    """
    Convert E+ time format to format acceptable by datetime module.

    EnergyPlus date and time format is not compatible with
    datetime.datetime module. This because hourly information
    can be '24' and end minute can be '60' - which is not
    allowed.

    To get around the issue, logic is in place to
    convert raw input into format as required for datetime
    (or datetime like) module.
    """

    if is_end_day(month, day) and hour == 24 and end_minute == 60:
        # Convert last step in month or of the year
        return (month + 1, 1, 0, 0) if month != 12 else (1, 1, 0, 0)

    elif hour == 24 and end_minute == 60:
        # Convert last step in day
        return month, day + 1, 0, 0

    elif end_minute == 60:
        # Convert last timestep of an hour
        return month, day, hour, 0
    else:
        return month, day, hour - 1, end_minute


def parse_result_datetime(
    date: datetime, month: int, day: int, hour: int, end_min: int
) -> datetime:
    """ Combine index date and peak occurrence date to return an appropriate peak timestamp. """
    if month is not None:
        # Runperiod results, all the timestamp information is
        # available in the output tuple
        m, d, h, min = datetime_helper(month, day, hour, end_min)
    elif day is not None:
        # Monthly results, month needs to be extracted from the datetime
        # index of the output, other line is available in the output tuple
        m, d, h, min = datetime_helper(date.month, day, hour, end_min)
    else:
        # Daily outputs, month and day is extracted from the datetime
        # index, hour and end minute is is taken from the output tuple
        m, d, h, min = datetime_helper(date.month, date.day, hour, end_min)
    # table peak value might overlap to next year
    year = date.year + 1 if (m, d, h, min) == (1, 1, 0, 0) else date.year
    return date.replace(year=year, month=m, day=d, hour=h, minute=min)


def is_end_day(month: int, day: int) -> bool:
    """ Check if day us the month end day. """
    return day == MONTH_END_DAYS[month]


def get_month_n_days_from_cumulative(monthly_cumulative_days: List[int]):
    """
    Transform consecutive number of days in monthly data to actual number of days.

    EnergyPlus monthly results report a total consecutive number of days for each day.
    Raw data reports table as 31, 59..., this function calculates and returns
    actual number of days for each month 31, 28...
    """
    old_num = monthly_cumulative_days.pop(0)
    m_actual_days = [old_num]
    for num in monthly_cumulative_days:
        new_num = num - old_num
        m_actual_days.append(new_num)
        old_num += new_num
    return m_actual_days


def find_num_of_days_annual(ann_num_of_days: List[int], rp_num_of_days: List[int]) -> List[int]:
    """ Use runperiod data to calculate number of days for each annual period. """
    days = rp_num_of_days[0] // len(ann_num_of_days)
    return [days for _ in ann_num_of_days]


def get_num_of_days(cumulative_days: Dict[str, List[int]]) -> Dict[str, List[int]]:
    """ Split num of days and date. """
    num_of_days = {}
    for table, values in cumulative_days.items():
        if table == M:
            # calculate actual number of days for monthly table
            num_of_days[M] = get_month_n_days_from_cumulative(values)
        else:
            num_of_days[table] = values
    # calculate number of days for annual table for
    # an incomplete year run or multi year analysis
    if A in cumulative_days.keys() and RP in cumulative_days.keys():
        num_of_days[A] = find_num_of_days_annual(num_of_days[A], num_of_days[RP])
    return num_of_days


def get_month_end_date(date: datetime) -> datetime:
    """ Return month end date of a given date. """
    return date.replace(day=MONTH_END_DAYS[date.month])


def check_year_increment(
    first_step_data: IntervalTuple,
    current_step_data: IntervalTuple,
    previous_step_data: IntervalTuple,
) -> bool:
    """ Check if year value should be incremented inside environment table. """
    # Only 'Monthly+' tables can have hour == 0
    if current_step_data.hour == 0:
        if first_step_data == current_step_data:
            return True  # duplicate date -> increment year
        elif first_step_data[0] > current_step_data[0]:
            return True  # current numeric month is lower than first -> increment year
        else:
            return False
    else:
        if current_step_data == (12, 31, 24, 60):
            return True
        elif first_step_data == current_step_data and previous_step_data != (12, 31, 24, 60,):
            return True  # duplicate date -> increment year
        else:
            return False


def convert_to_datetime(year: int, interval_tuple: IntervalTuple) -> datetime:
    """ Convert a raw E+ date to datetime format. """
    if interval_tuple.hour == 0:
        # Monthly+ table
        month, day, hour, end_minute = interval_tuple
    else:
        # Process raw EnergyPlus tiem and date information
        month, day, hour, end_minute = datetime_helper(*interval_tuple)
    return datetime(year, month, day, hour, end_minute)


def generate_datetime_dates(raw_dates: List[IntervalTuple], year: int) -> List[datetime]:
    """ Generate datetime index for a given period. """
    dates = [convert_to_datetime(year, raw_dates[0])]
    for i in range(1, len(raw_dates)):
        # based on the first, current and previous
        # steps decide if the year should be incremented
        if check_year_increment(raw_dates[0], raw_dates[i], raw_dates[-1]):
            year += 1
        date = convert_to_datetime(year, raw_dates[i])
        dates.append(date)
    return dates


def convert_to_datetime_index(
    raw_dates: Dict[str, List[IntervalTuple]], year: int
) -> Dict[str, List[datetime]]:
    """ Replace raw date information with datetime like object. """
    dates = {}
    for interval, value in raw_dates.items():
        dates[interval] = generate_datetime_dates(value, year)
    return dates


def update_start_dates(dates: Dict[str, List[datetime]]) -> None:
    """ Set accurate first date for monthly+ tables. """

    def _set_start_date(orig, refs):
        for ref in refs.values():
            orig[0] = ref[0].replace(hour=0, minute=0)
            return orig

    timestep_to_monthly_dates = {k: dates[k] for k in dates if k in [TS, H, D, M]}
    if timestep_to_monthly_dates:
        if M in dates:
            dates[M] = _set_start_date(dates[M], timestep_to_monthly_dates)
        if A in dates:
            dates[A] = _set_start_date(dates[A], timestep_to_monthly_dates)
        if RP in dates:
            dates[RP] = _set_start_date(dates[RP], timestep_to_monthly_dates)


def process_raw_date_data(
    raw_dates: Dict[str, List[IntervalTuple]], cumulative_days: Dict[str, List[int]], year: int
) -> Tuple[Dict[str, List[datetime]], Optional[Dict[str, List[int]]]]:
    """
    Process E+ raw date and time line.

    Transform raw E+ date and time integer data into datetime module Date
    and Datetime objects.

    First, the 'number of days' data for monthly, annual and runperiod
    is separated from the base dictionary.
    Separated date and time data is converted into datetime like objects.

    Based on the available data, start and end date for each environment is found
    and stored.

    """
    monthly_to_runperiod_dates = {k: v for k, v in raw_dates.items() if k in (M, A, RP)}
    if monthly_to_runperiod_dates:
        # Separate number of days data if any M to RP table is available
        num_of_days = get_num_of_days(cumulative_days)
    else:
        num_of_days = None
    # transform raw int data into datetime like index
    dates = convert_to_datetime_index(raw_dates, year)
    # update first day of monthly+ table based on TS, H or D data
    update_start_dates(dates)
    return dates, num_of_days
