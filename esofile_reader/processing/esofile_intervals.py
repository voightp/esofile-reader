import calendar
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

from esofile_reader.constants import *
from esofile_reader.exceptions import LeapYearMismatch, StartDayMismatch
from esofile_reader.mini_classes import IntervalTuple


def parse_eplus_datetime(
    year: int, month: int, day: int, hour: int, end_minute: int
) -> datetime:
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

    if hour == 24 and end_minute == 60:
        # Convert last step of day
        shifted_datetime = datetime(year, month, day, hour - 1)
        corrected_datetime = shifted_datetime + timedelta(hours=1)
    elif end_minute == 60:
        # Convert last timestep of an hour
        corrected_datetime = datetime(year, month, day, hour, 0)
    elif hour == 0:
        corrected_datetime = datetime(year, month, day, hour, end_minute)
    else:
        corrected_datetime = datetime(year, month, day, hour - 1, end_minute)
    return corrected_datetime


def combine_peak_result_datetime(
    date: datetime, month: Optional[int], day: Optional[int], hour: int, end_min: int
) -> datetime:
    """ Combine index date and peak occurrence date to return an appropriate peak timestamp. """
    if month is not None:
        # Runperiod results, all the timestamp information is
        # available in the output tuple
        new_datetime = parse_eplus_datetime(date.year, month, day, hour, end_min)
    elif day is not None:
        # Monthly results, month needs to be extracted from the datetime
        # index of the output, other line is available in the output tuple
        new_datetime = parse_eplus_datetime(date.year, date.month, day, hour, end_min)
    else:
        # Daily outputs, month and day is extracted from the datetime
        # index, hour and end minute is is taken from the output tuple
        new_datetime = parse_eplus_datetime(date.year, date.month, date.day, hour, end_min)
    return new_datetime


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


def check_year_increment(
    first_step_data: IntervalTuple, current_step_data: IntervalTuple,
) -> bool:
    """ Check if year value should be incremented inside environment table. """
    if first_step_data is current_step_data:
        # do not increment first step
        return False
    elif first_step_data == current_step_data:
        # duplicate date -> increment year
        return True
    elif first_step_data > current_step_data:
        # current date comes earlier than first -> increment year
        return True
    else:
        return False


def generate_datetime_dates(raw_dates: List[IntervalTuple], year: int) -> List[datetime]:
    """ Generate datetime index for a given period. """
    dates = []
    for i in range(0, len(raw_dates)):
        # based on the first, current and previous
        # steps decide if the year should be incremented
        if check_year_increment(raw_dates[0], raw_dates[i]):
            year += 1
        # year can be incremented automatically when converting to datetime
        date = parse_eplus_datetime(year, *raw_dates[i])
        dates.append(date)
    return dates


def update_start_dates(dates: Dict[str, List[datetime]]) -> Dict[str, List[datetime]]:
    """ Set accurate first date for monthly+ tables. """

    def set_start_date(orig, refs):
        for ref in refs.values():
            orig[0] = ref[0].replace(hour=0, minute=0)
            return orig

    timestep_to_monthly_dates = {k: dates[k] for k in dates if k in [TS, H, D, M]}
    if timestep_to_monthly_dates:
        for interval in (M, A, RP):
            if interval in dates:
                dates[interval] = set_start_date(dates[interval], timestep_to_monthly_dates)
    return dates


def process_cumulative_days(
    raw_dates: Dict[str, List[IntervalTuple]], cumulative_days: Dict[str, List[int]]
) -> Optional[Dict[str, List[int]]]:
    """ Convert cumulative days to number of days pers step. """
    monthly_to_runperiod_dates = {k: v for k, v in raw_dates.items() if k in (M, A, RP)}
    if monthly_to_runperiod_dates:
        # Separate number of days data if any M to RP table is available
        num_of_days = get_num_of_days(cumulative_days)
    else:
        num_of_days = None
    return num_of_days


def validate_year(
    year: int, is_leap: bool, date: Optional[IntervalTuple], day: Optional[str]
) -> None:
    """ Check if date for given and day corresponds to specified year. """
    if (date and not day) or (not date and day):
        raise ValueError(
            "Both 'date' and 'day' arguments need to be either specified"
            " or set to 'None' to check only for leap year."
        )
    if calendar.isleap(year) is is_leap:
        if date:
            test_datetime = datetime(year, date.month, date.day)
            test_day = test_datetime.strftime("%A")
            if day != test_day and day not in ("SummerDesignDay", "WinterDesignDay",):
                max_year = REFERENCE_YEAR + 10  # give some choices from future
                suitable_years = get_allowed_years(is_leap, date, day, max_year, n_samples=3)
                raise StartDayMismatch(
                    f"Start day '{day}' for given day '{test_datetime.strftime('%Y-%m-%d')}'"
                    f" does not correspond to real calendar day '{test_day}'!"
                    f"\nEither set 'year' kwarg as 'None' to identify year automatically"
                    f" or use one of '{suitable_years}'."
                )
    else:
        raise LeapYearMismatch(
            f"Specified year '{year}' does not match expected calendar data!"
            f" Outputs are reported for {'leap' if is_leap else 'standard'} year"
            f" but given year '{year}' is {'standard' if is_leap else 'leap'}."
            f" Either set 'year' kwarg as 'None' to seek year automatically"
            f" or use an actual {'leap' if is_leap else 'standard'} year."
        )


def is_leap_year_ts_to_d(raw_dates_arr: List[IntervalTuple]) -> bool:
    """ Check if raw dates include 29th of February. """
    for tup in raw_dates_arr:
        if (tup.month, tup.day) == (2, 29):
            return True
    else:
        return False


def is_leap_year_m_to_rp(
    interval: str, raw_dates_arr: List[IntervalTuple], n_days_arr: List[int]
) -> bool:
    """ Check if outputs for given interval ale reported for leap year. """
    if interval == M:
        try:
            # check if February has 29 days
            index = raw_dates_arr.index(IntervalTuple(2, 1, 0, 0))
            return n_days_arr[index] == 29
        except ValueError:
            # February is not included
            return False
    elif interval == A:
        # check if there's 366 days in year
        return n_days_arr[0] == 366
    else:
        return False


def seek_year(
    is_leap: bool, date: Optional[IntervalTuple], day: Optional[str], max_year: int
) -> int:
    """ Find first year matching given criteria. """
    for year in range(max_year, 0, -1):
        if calendar.isleap(year) is is_leap:
            if date:
                test_datetime = datetime(year, date.month, date.day)
                test_start_day = test_datetime.strftime("%A")
                if day == test_start_day:
                    break
                elif day in ("SummerDesignDay", "WinterDesignDay"):
                    logging.info("Sizing simulation, setting year to 2002.")
                    year = 2002
                    break
            else:
                break
    else:
        raise ValueError(
            f"Failed to automatically find year for following arguments"
            f" is_leap='{is_leap}', date='{date}' and day='{day}'."
            f" It seems that there ins't a year between 0 - {max_year} matching"
            f" date and day of week combination."
        )
    return year


def get_allowed_years(
    is_leap: bool,
    first_date: Optional[IntervalTuple],
    first_day: Optional[str],
    max_year: int,
    n_samples: int = 4,
) -> List[int]:
    """ Get a sample of allowed years for given conditions. """
    allowed_years = []
    for i in range(n_samples):
        year = seek_year(is_leap, first_date, first_day, max_year)
        max_year = year - 1
        allowed_years.append(year)
    return allowed_years


def get_lowest_interval(all_intervals: List[str]) -> str:
    """ Find the shortest interval from given ones. """
    return next((interval for interval in (TS, H, D, A, RP) if interval in all_intervals))


def get_info_from_raw_data(
    raw_dates: Dict[str, List[IntervalTuple]],
    days_of_week: Dict[str, List[str]],
    n_days: Dict[str, List[int]],
) -> Tuple[bool, Optional[IntervalTuple], Optional[str]]:
    """ Gather available """
    lowest_interval = get_lowest_interval(list(raw_dates.keys()))
    lowest_interval_values = raw_dates[lowest_interval]
    if lowest_interval in {TS, H, D}:
        is_leap = is_leap_year_ts_to_d(lowest_interval_values)
        first_day = days_of_week[lowest_interval][0]
        first_date = lowest_interval_values[0]
    else:
        n_days_arr = n_days[lowest_interval]
        is_leap = is_leap_year_m_to_rp(lowest_interval, lowest_interval_values, n_days_arr)
        first_date, first_day = None, None
    return is_leap, first_date, first_day


def convert_raw_dates(
    raw_dates: Dict[str, List[IntervalTuple]], year: int
) -> Dict[str, List[datetime]]:
    """ Transform raw E+ date and time data into datetime.datetime objects. """
    dates = {}
    for interval, value in raw_dates.items():
        dates[interval] = generate_datetime_dates(value, year)
    return dates


def convert_raw_date_data(
    raw_dates: Dict[str, List[IntervalTuple]],
    days_of_week: Dict[str, List[str]],
    cumulative_days: Dict[str, List[int]],
    year: Optional[int],
) -> Tuple[Dict[str, List[datetime]], Dict[str, List[int]]]:
    """ Convert EnergyPlus dates into standard datetime format. """
    n_days = process_cumulative_days(raw_dates, cumulative_days)
    is_leap, first_date, first_day = get_info_from_raw_data(raw_dates, days_of_week, n_days)
    if year is None:
        year = seek_year(is_leap, first_date, first_day, REFERENCE_YEAR)
    else:
        validate_year(year, is_leap, first_date, first_day)
    dates = convert_raw_dates(raw_dates, year)
    return update_start_dates(dates), n_days
