import calendar
import logging
from collections import namedtuple
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from esofile_reader.exceptions import LeapYearMismatch, StartDayMismatch
from esofile_reader.processing.eplus import TS, H, D, M, A, RP


def parse_eso_timestamp(
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
    year = date.year
    if month is None:
        # Monthly results, month needs to be extracted from the datetime
        # index of the output, other line is available in the output tuple
        month = date.month
        if day is None:
            # Daily outputs, month and day is extracted from the datetime
            # index, hour and end minute is is taken from the output tuple
            day = date.day
    return parse_eso_timestamp(year, month, day, hour, end_min)


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


def get_annual_n_days(annual_timestamps: List[datetime]) -> List[int]:
    """ Calculate number of days for annual interval. """
    return [366 if calendar.isleap(dt.year) else 365 for dt in annual_timestamps]


def find_num_of_days_annual(ann_num_of_days: List[int], rp_num_of_days: List[int]) -> List[int]:
    """ Use runperiod data to calculate number of days for each annual period. """
    days = rp_num_of_days[0] // len(ann_num_of_days)
    return [days for _ in ann_num_of_days]


EsoTimestamp = namedtuple("EsoTimestamp", "month day hour end_minute")


def check_year_increment(
    first_step_data: EsoTimestamp, current_step_data: EsoTimestamp,
) -> bool:
    """ Check if year value should be incremented inside environment table. """
    if first_step_data is current_step_data:
        # do not increment first step
        return False
    elif first_step_data >= current_step_data:
        # duplicate date -> increment year
        return True
    else:
        return False


def generate_datetime_dates(raw_dates: List[EsoTimestamp], year: int) -> List[datetime]:
    """ Generate datetime index for a given period. """
    dates = []
    for i in range(0, len(raw_dates)):
        # based on the first, current and previous
        # steps decide if the year should be incremented
        if check_year_increment(raw_dates[0], raw_dates[i]):
            year += 1
        # year can be incremented automatically when converting to datetime
        date = parse_eso_timestamp(year, *raw_dates[i])
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


def get_n_days_from_cumulative(
    cumulative_days: Dict[str, List[int]], dates: Dict[str, List[datetime]]
) -> Optional[Dict[str, List[int]]]:
    """ Calculate actual number of days for monthly, runperiod and annual intervals. """
    if cumulative_days:
        n_days = {}
        for table, values in cumulative_days.items():
            if table == M:
                n_days[M] = get_month_n_days_from_cumulative(values)
            elif table == A:
                n_days[A] = get_annual_n_days(dates[A])
            else:
                n_days[table] = values
        return n_days
    else:
        n_days = None
    return n_days


def validate_year(year: int, is_leap: bool, date: EsoTimestamp, day: str) -> None:
    """ Check if date for given and day corresponds to specified year. """
    if calendar.isleap(year) is is_leap:
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
            f" or use {'leap' if is_leap else 'standard'} year."
        )


def is_leap_year_ts_to_d(raw_dates_arr: List[EsoTimestamp]) -> bool:
    """ Check if first year is leap based on timestep, hourly or daily data. """
    for tup in raw_dates_arr:
        if (tup.month, tup.day) == (2, 29):
            return True
        elif check_year_increment(raw_dates_arr[0], tup):
            # stop once first year is covered
            return False
    else:
        return False


def seek_year(is_leap: bool, date: EsoTimestamp, day: str, max_year: int) -> int:
    """ Find first year matching given criteria. """
    for year in range(max_year, 0, -1):
        if day in ("SummerDesignDay", "WinterDesignDay"):
            logging.info("Sizing simulation, setting year to 2002.")
            year = 2002
            break
        elif calendar.isleap(year) is is_leap:
            test_datetime = datetime(year, date.month, date.day)
            test_start_day = test_datetime.strftime("%A")
            if day == test_start_day:
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
    is_leap: bool, first_date: EsoTimestamp, first_day: str, max_year: int, n_samples: int = 4,
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
    return next((interval for interval in (TS, H, D, M, A, RP) if interval in all_intervals))


def convert_raw_dates(
    raw_dates: Dict[str, List[EsoTimestamp]], year: int
) -> Dict[str, List[datetime]]:
    """ Transform raw E+ date and time data into datetime.datetime objects. """
    dates = {}
    for interval, value in raw_dates.items():
        dates[interval] = generate_datetime_dates(value, year)
    return dates


def convert_raw_date_data(
    raw_dates: Dict[str, List[EsoTimestamp]],
    days_of_week: Dict[str, List[str]],
    year: Optional[int],
) -> Dict[str, List[datetime]]:
    """ Convert EnergyPlus dates into standard datetime format. """
    lowest_interval = get_lowest_interval(list(raw_dates.keys()))
    if lowest_interval in {TS, H, D}:
        lowest_interval_values = raw_dates[lowest_interval]
        is_leap = is_leap_year_ts_to_d(lowest_interval_values)
        first_date = lowest_interval_values[0]
        first_day = days_of_week[lowest_interval][0]
        if year is None:
            year = seek_year(is_leap, first_date, first_day, REFERENCE_YEAR)
        else:
            validate_year(year, is_leap, first_date, first_day)
    else:
        # allow any year defined or set EnergyPlus default 2002
        year = year if year else 2002
    dates = convert_raw_dates(raw_dates, year)
    return update_start_dates(dates)


REFERENCE_YEAR = 2020
