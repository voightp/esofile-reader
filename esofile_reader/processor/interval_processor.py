import numpy as np
import pandas as pd

from esofile_reader.constants import *


def update_dt_format(df, timestamp_format):
    """ Set specified 'datetime' str format. """
    if TIMESTAMP_COLUMN in df.index.names:
        ts_index = df.index.get_level_values(TIMESTAMP_COLUMN)

        if isinstance(ts_index, pd.DatetimeIndex):
            new_index = ts_index.strftime(timestamp_format)
            if isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.Index(new_index, name=TIMESTAMP_COLUMN)
            else:
                df.index.set_levels(new_index, level=TIMESTAMP_COLUMN, inplace=True)

    cond = (df.dtypes == np.dtype("datetime64[ns]")).to_list()
    df.loc[:, cond] = df.loc[:, cond].applymap(lambda x: x.strftime(timestamp_format))

    return df


def datetime_helper(month, day, hour, end_minute):
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


def parse_result_dt(date, month, day, hour, end_min):
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


def is_end_day(month, day):
    """ Check if day us the month end day. """
    months = {
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
    return day == months[month]


def month_act_days(monthly_cumulative_days):
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


def find_num_of_days_annual(ann_num_of_days, rp_num_of_days):
    """ Use runperiod data to calculate number of days for each annual period. """
    days = rp_num_of_days[0] // len(ann_num_of_days)
    return [days for _ in ann_num_of_days]


def get_num_of_days(cumulative_days):
    """ Split num of days and date. """
    num_of_days = {}

    for table, values in cumulative_days.items():
        if table == M:
            # calculate actual number of days for monthly table
            num_of_days[M] = month_act_days(values)
        else:
            num_of_days[table] = values

    # calculate number of days for annual table for
    # an incomplete year run or multi year analysis
    if A in cumulative_days.keys() and RP in cumulative_days.keys():
        num_of_days[A] = find_num_of_days_annual(num_of_days[A], num_of_days[RP])

    return num_of_days


def month_end_date(date):
    """ Return month end date of a given date. """
    months = {
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
    end_date = date.replace(day=months[date.month])
    return end_date


def incr_year_env(first_step_data, current_step_data, previous_step_data):
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


def _to_timestamp(year, table_tuple):
    """ Convert a raw E+ date to pandas.Timestamp format. """
    if table_tuple.hour == 0:
        # Monthly+ table
        month, day, hour, end_minute = table_tuple
    else:
        # Process raw EnergyPlus tiem and date information
        month, day, hour, end_minute = datetime_helper(*table_tuple)
    return pd.Timestamp(year, month, day, hour, end_minute)


def _gen_dt(raw_dates, year):
    """ Generate timestamp index for a given period. """
    dates = [_to_timestamp(year, raw_dates[0])]

    for i in range(1, len(raw_dates)):
        # based on the first, current and previous
        # steps decide if the year should be incremented
        if incr_year_env(raw_dates[0], raw_dates[i], raw_dates[-1]):
            year += 1

        date = _to_timestamp(year, raw_dates[i])
        dates.append(date)

    return dates


def convert_to_dt_index(raw_dates, year):
    """ Replace raw date information with datetime like object. """
    dates = {}
    for table, value in raw_dates.items():
        dates[table] = _gen_dt(value, year)

    return dates


def update_start_dates(dates):
    """ Set accurate first date for monthly+ tables. """

    def _set_start_date(orig, refs):
        for ref in refs.values():
            orig[0] = ref[0].replace(hour=0, minute=0)
            return orig

    ts_to_m_envs = {k: dates[k] for k in dates if k in [TS, H, D, M]}
    if ts_to_m_envs:
        if M in dates:
            dates[M] = _set_start_date(dates[M], ts_to_m_envs)

        if A in dates:
            dates[A] = _set_start_date(dates[A], ts_to_m_envs)

        if RP in dates:
            dates[RP] = _set_start_date(dates[RP], ts_to_m_envs)


def interval_processor(dates, cumulative_days, year):
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

    num_of_days = {}
    m_to_rp = {k: v for k, v in dates.items() if k in (M, A, RP)}

    if m_to_rp:
        # Separate number of days data if any M to RP table is available
        num_of_days = get_num_of_days(cumulative_days)

    # transform raw int data into datetime like index
    dates = convert_to_dt_index(dates, year)

    # update first day of monthly+ table based on TS, H or D data
    update_start_dates(dates)

    return dates, num_of_days
