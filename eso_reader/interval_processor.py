import datetime as dt
import pandas as pd
from eso_reader.constants import TS, H, D, M, A, RP
from eso_reader.constants import YEAR


class IntervalNotAvailable(KeyError):
    """ Raise an exception when interval is not included in results. """
    pass


class CannotFindEnvironment(Exception):
    """ Raise and exception when there isn't any suitable interval to find environment dates. """


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

    # Convert last step in month or of the year
    if is_end_day(month, day) and hour == 24 and end_minute == 60:
        return (month + 1, 1, 0, 0) if month != 12 else (1, 1, 0, 0)

    # Convert last step in day
    elif hour == 24 and end_minute == 60:
        return month, day + 1, 0, 0

    # Convert last hour
    elif hour == 24:
        return month, day, hour - 1, end_minute

    # Convert last timestep of an hour
    elif end_minute == 60:
        return month, day, hour, 0
    else:
        return month, day, hour - 1, end_minute


def parse_result_dt(date, res_tup, month_ix, day_ix, hour_ix, end_min_ix):
    """ Combine index date and peak occurrence date to return an appropriate peak timestamp. """

    # Runperiod results, all the timestamp information is
    # available in the output tuple
    if month_ix is not None:
        m, d, h, min = datetime_helper(res_tup[month_ix], res_tup[day_ix], res_tup[hour_ix], res_tup[end_min_ix])
        return date.replace(month=m, day=d, hour=h, minute=min)

    # Monthly results, month needs to be extracted from the
    # datetime index of the output, other data is available
    # in the output tuple
    elif day_ix is not None:
        m, d, h, min = datetime_helper(date.month, res_tup[day_ix], res_tup[hour_ix], res_tup[end_min_ix])
        return date.replace(month=m, day=d, hour=h, minute=min)

    # Daily outputs, month and day is extracted from the
    # datetime index, hour and end minute is is taken from
    # the output tuple
    else:
        timestamp = datetime_helper(date.month, date.day, res_tup[hour_ix], res_tup[end_min_ix])

        # Daily interval peak value might overlap to next year
        year = date.year + 1 if timestamp == (1, 1, 0, 0) else date.year
        m, d, h, min = timestamp
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
    Raw data reports interval as (31,1), (59,2)..., this function calculates and returns
    actual number of days for each month (31,1), (28,2)...
    """
    m_list = []

    for m_env in m_envs:
        if len(m_env) > 1:
            new_lst = [m_env.pop(0)]
            old_num, _ = new_lst[0]
            for i in range(len(m_env)):
                num, m = m_env[i]
                num = num - old_num
                new_lst.append((num, m))
                old_num += num
            m_list.append(new_lst)
        else:
            m_list.append(m_env)

    return m_list


def split_nested_list(env_list):
    """ Divide a dictionary in two to split  the lowest level tuple. """
    num_of_days_list = [[item[0] for item in environment] for environment in env_list]
    env_list = [[item[1] for item in environment] for environment in env_list]
    return env_list, num_of_days_list


def find_num_of_days_annual(ann_num_of_days, rp_num_of_days):
    """ Use runperiod data to calculate number of days for each annual period. """
    new_ann = []

    for an_env, rp_env in zip(ann_num_of_days, rp_num_of_days):
        # calculate annual number of days when runperiod contains multiple years
        days = rp_env[0] // len(an_env)
        new_ann.append([days for _ in an_env])

    return new_ann


def get_num_of_days(env_dict, intervals):
    """ Separate a num of days data from date data. """
    num_of_days_dict = {}

    # calculate actual number of days for monthly interval
    if M in intervals:
        env_dict[M] = month_act_days(env_dict[M])

    # split nested dictionary to separate lowest level tuple
    for period in intervals:
        env_dict[period], num_of_days_dict[period] = split_nested_list(env_dict[period])

    # calculate number of days for annual interval for
    # an incomplete year run or multi year analysis
    if A in intervals and RP in intervals:
        num_of_days_dict[A] = find_num_of_days_annual(num_of_days_dict[A], num_of_days_dict[RP])

    return env_dict, num_of_days_dict


def num_days_in_month(date):
    """ Return number of days for a given month (as int). """
    months = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
              7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    return months[date.month]


def month_end_date(date):
    """ Return month end date of a given date. """
    end_date = date.replace(day=num_days_in_month(date))  # find last day
    return end_date


def dict_not_empty(dct):
    """ Check if dict and its sub-dicts hold populated lists. """
    if isinstance(dct, dict):
        return any(map(dict_not_empty, dct.values()))
    elif isinstance(dct, list):
        return list_not_empty(dct)
    else:
        return False


def list_not_empty(lst):
    """ Check if list or its sub-lists are empty. """
    if isinstance(lst, list):
        return any(map(list_not_empty, lst))
    return True


def slice_dict(dct, keys):
    """ Slice dictionary using given keys. """
    return {key: dct[key] for key in keys if key in dct}


def incr_year_envs(previous_env_start, current_env_start):
    """ Check if year value should be incremented between environments. """
    return previous_env_start == current_env_start


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

    elif len(current_step_data) == 4:
        if current_step_data == (12, 31, 24, 60):
            return True
        elif first_step_data == current_step_data and previous_step_data != (12, 31, 24, 60):
            return True  # duplicate date -> increment year
        else:
            return False
    else:
        return False


def _to_timestamp(year, tmstmp):
    """ Convert a raw E+ date to pandas.Timestamp format. """
    if tmstmp.hour == 0:
        # Monthly+ interval
        month, day, hour, end_minute = tmstmp
    else:
        # Process raw EnergyPlus tiem and date information
        month, day, hour, end_minute = datetime_helper(*tmstmp)
    return pd.Timestamp(year, month, day, hour, end_minute)


def _gen_dt(envs, year):
    """ Generate timestamp index for a given period. """
    new_envs = []
    prev_env_start = None
    for env in envs:
        # Store first timestamp of the interval
        new_env = [_to_timestamp(year, env[0])]

        # Increment year if there could be duplicate date
        if prev_env_start:
            if incr_year_envs(new_env, prev_env_start):
                year += 1
        prev_env_start = new_env

        # Loop through the interval list
        # to generate datetime like index
        for i in range(1, len(env)):

            # Based on the first, current and previous
            # steps decide if the year should be incremented
            if incr_year_env(env[0], env[i], env[-1]):
                year += 1

            # Create timestamp object
            date = _to_timestamp(year, env[i])
            new_env.append(date)

        new_envs.append(new_env)

    return new_envs


def convert_to_dt_index(env_dict, year):
    """
    Replace raw date information with datetime like object.

    If there isn't any data in the interval, set interval value to None.
    """
    for key, value in env_dict.items():
        env_dict[key] = _gen_dt(value, year)

    return env_dict


def _set_start_date(to_be_set_envs, ts_to_m_envs):
    """ Set interval start dates for given interval. """
    for envs in ts_to_m_envs.values():
        for r_env, o_env in zip(to_be_set_envs, envs):
            r_env[0] = o_env[0].replace(hour=0, minute=0)
        return to_be_set_envs


def update_start_dates(env_dict):
    """ Set accurate first date for monthly+ intervals. """
    ts_to_m_envs = slice_dict(env_dict, [TS, H, D, M])
    if ts_to_m_envs:
        if M in env_dict:
            env_dict[M] = _set_start_date(env_dict[M], ts_to_m_envs)

        if A in env_dict:
            env_dict[A] = _set_start_date(env_dict[A], ts_to_m_envs)

        if RP in env_dict:
            env_dict[RP] = _set_start_date(env_dict[RP], ts_to_m_envs)


def _env_ts_d_h(envs):
    """ Find start and end date for a single environment based on daily, hourly or timestep data. """
    environment = []

    for env in envs:
        # For 'Daily' outputs, end day is the last item
        # in the list
        if env[1] - env[0] == 86400:
            start_date = env[0]
            end_date = env[-1]
        # For 'TS' and 'Hourly' outputs, the last item
        # in the list is next day
        else:
            start_date = env[0].date()
            end_date = env[-2].date()

        environment.append((start_date, end_date))

    return environment


def find_env_ts_to_d(interval):
    """ Find start and end date for all environments based on daily, hourly or timestep data. """
    for envs in interval.values():
        if envs is not None:
            return _env_ts_d_h(envs)  # Return data based on first not empty interval


def _env_m(m_act_days, num_of_days):
    """
    Find start and end date for a single environment based on monthly data.

    Note
    ----
    For interval with more than one month, start day of first month and end day of
    last month is calculated and should be accurate. When there is only a one month
    in interval, start date is set as a first day of month and last day is calculated.
    """
    environment = []

    for env, days in zip(m_act_days, num_of_days):
        # If there is multiple months included,
        # calculate start and end date precisely
        if len(env) > 1:
            f_num_days, f_date = days[0], env[0]
            l_num_days, l_date = days[-1], env[-1]
            start_date = (month_end_date(f_date) - dt.timedelta(days=f_num_days - 1))
            end_date = l_date + dt.timedelta(l_num_days - 1)
            environment.append((start_date, end_date))

        # For a single month environment start date is
        # left as first day of month
        else:
            f_num_days, f_date = env[0], days[0]
            environment.append((f_date, (f_date + dt.timedelta(days=f_num_days - 1))))

    return environment


def _env_r(runperiods, num_of_days):
    """ Find start and end date for a single environment based on runperiod data. """
    environment = []

    for r, num in zip(runperiods, num_of_days):
        environment.append((r[0], r[0] + dt.timedelta(days=(num[0] - 1))))

    return environment


def find_env_m_to_rp(env_dict, num_of_days_dict):
    """ Find start and end date for all environments based on monthly or runperiod data. """
    if list_not_empty(env_dict[M]):
        environment = _env_m(env_dict[M], num_of_days_dict[M])
        return environment

    elif list_not_empty(env_dict[RP]):
        environment = _env_r(env_dict[RP], num_of_days_dict[RP])
        return environment

    else:
        print("Not enough data to find environment!")


def find_environment(all_envs_dates, monthly_to_rp_cd):
    """
    Find start and end date for each environment.

    Note
    ----
    Primary, the start and end date is based on daily, timestep or
    hourly data. If any of these is not available, monthly or runperiod
    data is used. Environment based on interval greater than monthly
    might not give a precise start date.
    """
    ts_to_daily_envs = slice_dict(all_envs_dates, [TS, H, D])
    monthly_rp_envs = slice_dict(all_envs_dates, [M, RP])

    if ts_to_daily_envs:
        # find environments using timestep or hourly interval
        return find_env_ts_to_d(ts_to_daily_envs)

    elif monthly_rp_envs:
        # find environments using monthly or runperiod interval
        return find_env_m_to_rp(monthly_rp_envs, monthly_to_rp_cd)

    else:
        raise CannotFindEnvironment("Cannot find environment!\n"
                                    "Include at least one TS, H, D, M, RP output.")


def flat_values(nested_env_dict):
    """ Transform dictionary nested list values into flat lists. """
    if nested_env_dict:
        for key, nested_value in nested_env_dict.items():
            nested_env_dict[key] = [item for lst in nested_value for item in lst]

    return nested_env_dict


def interval_processor(all_envs_dict):
    """
    Process E+ raw date and time data.

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

    year = YEAR
    interval_keys = all_envs_dict.keys()
    avail_m_to_rp = set(interval_keys).intersection({M, A, RP})
    m_to_rp_cmlt_d = {}

    # process 'Monthly+' intervals
    if any(x in avail_m_to_rp for x in interval_keys):
        # Separate number of days data if any M to RP interval is available
        all_envs_dict, m_to_rp_cmlt_d = get_num_of_days(all_envs_dict, avail_m_to_rp)

    # transform raw int data into datetime like index
    all_envs_dates = convert_to_dt_index(all_envs_dict, year)

    # update first day of monthly+ interval based on
    # shorter interval data
    update_start_dates(all_envs_dates)

    # Find start and end dates for all environments
    all_environment_dates = find_environment(all_envs_dates, m_to_rp_cmlt_d)

    return (
        all_environment_dates,
        flat_values(all_envs_dates),
        flat_values(m_to_rp_cmlt_d)
    )
