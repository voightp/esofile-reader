import numpy as np
import pandas as pd

from esofile_reader.constants import *
from esofile_reader.utils.utils import slice_dict


class IntervalNotAvailable(KeyError):
    """ Raise an exception when interval is not included in results. """
    pass


class CannotFindEnvironment(Exception):
    """ Raise and exception when there isn't any suitable interval to find environment dates. """
    pass


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

    # interval peak value might overlap to next year
    year = date.year + 1 if (m, d, h, min) == (1, 1, 0, 0) else date.year

    return date.replace(year=year, month=m, day=d, hour=h, minute=min)


def is_end_day(month, day):
    """ Check if day us the month end day. """
    months = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
              7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    return day == months[month]


def month_act_days(m_envs):
    """
    Transform consecutive number of days in monthly data to actual number of days.

    EnergyPlus monthly results report a total consecutive number of days for each day.
    Raw data reports interval as 31, 59..., this function calculates and returns
    actual number of days for each month 31, 28...
    """
    m_list = []

    for m_env in m_envs:
        if len(m_env) > 1:
            old_num = m_env.pop(0)
            new_lst = [old_num]
            for num in m_env:
                new_num = num - old_num
                new_lst.append(new_num)
                old_num += new_num
            m_list.append(new_lst)
        else:
            m_list.append(m_env)

    return m_list


def find_num_of_days_annual(ann_num_of_days, rp_num_of_days):
    """ Use runperiod data to calculate number of days for each annual period. """
    new_ann = []

    for an_env, rp_env in zip(ann_num_of_days, rp_num_of_days):
        # calculate annual number of days when runperiod contains multiple years
        days = rp_env[0] // len(an_env)
        new_ann.append([days for _ in an_env])

    return new_ann


def get_num_of_days(cumulative_days):
    """ Split num of days and date. """
    num_of_days = {}

    for interval, values in cumulative_days.items():
        if interval == M:
            # calculate actual number of days for monthly interval
            num_of_days[M] = month_act_days(values)
        else:
            num_of_days[interval] = values

    # calculate number of days for annual interval for
    # an incomplete year run or multi year analysis
    if A in cumulative_days.keys() and RP in cumulative_days.keys():
        num_of_days[A] = find_num_of_days_annual(num_of_days[A], num_of_days[RP])

    return num_of_days


def month_end_date(date):
    """ Return month end date of a given date. """
    months = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
              7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    end_date = date.replace(day=months[date.month])
    return end_date


def incr_year_env(first_step_data, current_step_data, previous_step_data):
    """ Check if year value should be incremented inside environment interval. """
    # Only 'Monthly+' intervals can have hour == 0
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
        elif first_step_data == current_step_data and previous_step_data != (12, 31, 24, 60):
            return True  # duplicate date -> increment year
        else:
            return False


def _to_timestamp(year, interval_tuple):
    """ Convert a raw E+ date to pandas.Timestamp format. """
    if interval_tuple.hour == 0:
        # Monthly+ interval
        month, day, hour, end_minute = interval_tuple
    else:
        # Process raw EnergyPlus tiem and date information
        month, day, hour, end_minute = datetime_helper(*interval_tuple)
    return pd.Timestamp(year, month, day, hour, end_minute)


def _gen_dt(envs, year):
    """ Generate timestamp index for a given period. """
    new_envs = []
    prev_env_start = None
    for env in envs:
        if prev_env_start:
            # increment year if there could be duplicate date
            if env[0] == prev_env_start:
                year += 1
        prev_env_start = env[0]

        new_env = [_to_timestamp(year, env[0])]
        for i in range(1, len(env)):

            # based on the first, current and previous
            # steps decide if the year should be incremented
            if incr_year_env(env[0], env[i], env[-1]):
                year += 1

            date = _to_timestamp(year, env[i])
            new_env.append(date)

        new_envs.append(new_env)

    return new_envs


def convert_to_dt_index(env_dict, year):
    """ Replace raw date information with datetime like object. """
    for interval, value in env_dict.items():
        env_dict[interval] = _gen_dt(value, year)

    return env_dict


def update_start_dates(env_dict):
    """ Set accurate first date for monthly+ intervals. """

    def _set_start_date(to_be_set_envs, reference):
        for envs in reference.values():
            for r_env, o_env in zip(to_be_set_envs, envs):
                r_env[0] = o_env[0].replace(hour=0, minute=0)
            return to_be_set_envs

    ts_to_m_envs = slice_dict(env_dict, [TS, H, D, M])
    if ts_to_m_envs:
        if M in env_dict:
            env_dict[M] = _set_start_date(env_dict[M], ts_to_m_envs)

        if A in env_dict:
            env_dict[A] = _set_start_date(env_dict[A], ts_to_m_envs)

        if RP in env_dict:
            env_dict[RP] = _set_start_date(env_dict[RP], ts_to_m_envs)


def flat_values(nested_env_dict):
    """ Transform dictionary nested list values into flat lists. """
    if nested_env_dict:
        for key, nested_value in nested_env_dict.items():
            nested_env_dict[key] = [item for lst in nested_value for item in lst]

    return nested_env_dict


def interval_processor(all_envs, cumulative_days, year):
    """
    Process E+ raw date and time line.

    Transform raw E+ date and time integer data into datetime module Date
    and Datetime objects.

    First, the 'number of days' data for monthly, annual and runperiod
    is separated from the base dictionary.
    Separated date and time data is converted into datetime like objects.

    Based on the available data, start and end date for each environment is found
    and stored.

    Finally, values containing nested lists are transformed into flat lists
    to be consistent with output data.
    """

    num_of_days = {}
    m_to_rp = {k: v for k, v in all_envs.items() if k in (M, A, RP)}

    if m_to_rp:
        # Separate number of days data if any M to RP interval is available
        num_of_days = get_num_of_days(cumulative_days)

    # transform raw int data into datetime like index
    dates = convert_to_dt_index(all_envs, year)

    # update first day of monthly+ interval based on
    # shorter interval line
    update_start_dates(dates)

    return (
        flat_values(dates),
        flat_values(num_of_days)
    )
