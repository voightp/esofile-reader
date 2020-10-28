import pytest

from esofile_reader.processing.esofile_intervals import *


@pytest.mark.parametrize(
    "year,interval_tuple,expected",
    [
        (2002, IntervalTuple(1, 1, 0, 0), datetime(2002, 1, 1, 0, 0)),
        (2002, IntervalTuple(1, 1, 1, 30), datetime(2002, 1, 1, 0, 30)),
        (2002, IntervalTuple(12, 31, 24, 60), datetime(2003, 1, 1, 0, 0)),
        (2002, IntervalTuple(10, 31, 24, 60), datetime(2002, 11, 1, 0, 0)),
        (2002, IntervalTuple(10, 25, 24, 60), datetime(2002, 10, 26, 0, 0, 0)),
        (2002, IntervalTuple(10, 25, 22, 60), datetime(2002, 10, 25, 22, 0, 0)),
        (2002, IntervalTuple(10, 25, 22, 10), datetime(2002, 10, 25, 21, 10, 0)),
    ],
)
def test_parse_eplus_datetime(year, interval_tuple, expected):
    assert parse_eplus_datetime(year, *interval_tuple) == expected


@pytest.mark.parametrize(
    "date,interval_tuple,expected",
    [
        (datetime(2002, 1, 1), (2, 3, 4, 30), datetime(2002, 2, 3, 3, 30)),
        (datetime(2002, 1, 1), (None, 3, 4, 30), datetime(2002, 1, 3, 3, 30)),
        (datetime(2002, 1, 1), (None, None, 10, 30), datetime(2002, 1, 1, 9, 30)),
        (datetime(2002, 12, 31), (None, None, 24, 60), datetime(2003, 1, 1, 0, 0)),
    ],
)
def test_combine_peak_result_datetime(date, interval_tuple, expected):
    assert combine_peak_result_datetime(date, *interval_tuple) == expected


def test_month_act_days():
    m_envs = [31, 59, 90, 97]
    out = get_month_n_days_from_cumulative(m_envs)
    assert out, [31, 28, 31, 7]


def test_month_act_days_single_env():
    m_envs = [[31]]
    out = get_month_n_days_from_cumulative(m_envs)
    assert out == [[31]]


def test_find_num_of_days_annual():
    ann_num_days = [1]
    rp_num_days = [365]
    out = find_num_of_days_annual(ann_num_days, rp_num_days)
    assert out == [365]

    ann_num_days = [1, 2]
    rp_num_days = [700]
    out = find_num_of_days_annual(ann_num_days, rp_num_days)
    assert out == [350, 350]


def test_get_num_of_days():
    days = {M: [10, 20, 30], RP: [100], A: [1]}
    out = get_num_of_days(days)
    assert out == {"monthly": [10, 10, 10], "runperiod": [100], "annual": [100]}


@pytest.mark.parametrize(
    "first_step_data,current_step_data,increment",
    [
        (IntervalTuple(1, 1, 0, 0), IntervalTuple(1, 1, 0, 0), True),
        (IntervalTuple(2, 1, 0, 0), IntervalTuple(1, 1, 0, 0), True),
        (IntervalTuple(1, 1, 1, 0), IntervalTuple(12, 31, 24, 60), False),
        (IntervalTuple(1, 1, 1, 0), IntervalTuple(1, 1, 1, 0), True),
    ],
    ids=["monthly", "monthly", "daily", "daily"],
)
def test_increment_year(first_step_data, current_step_data, increment):
    assert check_year_increment(first_step_data, current_step_data,) is increment


@pytest.mark.parametrize(
    "first_step_data,current_step_data",
    [
        (IntervalTuple(1, 1, 0, 0), IntervalTuple(2, 1, 0, 0)),
        (IntervalTuple(1, 1, 1, 0), IntervalTuple(1, 1, 2, 0)),
    ],
    ids=["monthly", "daily"],
)
def test_do_not_increment_year_monthly(first_step_data, current_step_data):
    assert not check_year_increment(first_step_data, current_step_data)


@pytest.mark.parametrize(
    "year,interval_tuples,expected",
    [
        (
            2002,
            [IntervalTuple(1, 1, 0, 0), IntervalTuple(2, 1, 0, 0), IntervalTuple(3, 1, 0, 0)],
            [
                datetime(2002, 1, 1, 0, 0, 0),
                datetime(2002, 2, 1, 0, 0, 0),
                datetime(2002, 3, 1, 0, 0, 0),
            ],
        ),
        (
            2002,
            [
                IntervalTuple(12, 31, 23, 60),
                IntervalTuple(12, 31, 24, 60),
                IntervalTuple(1, 1, 1, 60),
            ],
            [
                datetime(2002, 12, 31, 23, 0, 0),
                datetime(2003, 1, 1, 0, 0, 0),
                datetime(2003, 1, 1, 1, 0, 0),
            ],
        ),
    ],
)
def test_generate_timestamp_dates(year, interval_tuples, expected):
    assert generate_datetime_dates(interval_tuples, year) == expected


def test_convert_to_dt_index():
    env_dct = {
        "hourly": [
            IntervalTuple(12, 31, 23, 60),
            IntervalTuple(12, 31, 24, 60),
            IntervalTuple(1, 1, 1, 60),
        ],
        "monthly": [
            IntervalTuple(1, 1, 0, 0),
            IntervalTuple(2, 1, 0, 0),
            IntervalTuple(3, 1, 0, 0),
        ],
    }
    dates = convert_raw_dates(env_dct, 2002)
    assert dates == {
        "hourly": [
            datetime(2002, 12, 31, 23, 00, 00),
            datetime(2003, 1, 1, 00, 00, 00),
            datetime(2003, 1, 1, 1, 00, 00),
        ],
        "monthly": [
            datetime(2002, 1, 1, 0, 0, 0),
            datetime(2002, 2, 1, 0, 0, 0),
            datetime(2002, 3, 1, 0, 0, 0),
        ],
    }


def test_update_start_dates():
    env_dct = {
        "hourly": [datetime(2002, 5, 26, 0, 0), datetime(2002, 5, 26, 1, 0)],
        "monthly": [datetime(2002, 5, 1, 0, 0)],
        "annual": [datetime(2002, 1, 1, 0, 0)],
        "runperiod": [datetime(2002, 1, 1, 0, 0)],
    }
    update_start_dates(env_dct)
    assert env_dct == {
        "hourly": [datetime(2002, 5, 26, 0, 0), datetime(2002, 5, 26, 1, 0)],
        "monthly": [datetime(2002, 5, 26, 0, 0)],
        "annual": [datetime(2002, 5, 26, 0, 0)],
        "runperiod": [datetime(2002, 5, 26, 0, 0)],
    }


@pytest.mark.parametrize(
    "year, is_leap, date, day",
    [
        (2020, True, IntervalTuple(10, 28, 0, 0), "Wednesday"),
        (2020, True, IntervalTuple(2, 29, 0, 0), "Saturday"),
        (2020, True, IntervalTuple(1, 1, 0, 0), "Wednesday"),
        (2020, True, None, None),
        (2016, True, None, None),
        (2000, True, None, None),
        (2002, False, IntervalTuple(10, 28, 0, 0), "Monday"),
        (2002, False, IntervalTuple(2, 28, 0, 0), "Thursday"),
        (2002, False, IntervalTuple(1, 1, 0, 0), "Tuesday"),
        (1900, False, None, None),
        (2001, False, None, None),
        (1990, False, None, None),
    ],
)
def test_validate_year(year, is_leap, date, day):
    assert validate_year(year, is_leap, date, day) is None


@pytest.mark.parametrize(
    "year, is_leap, date, day, error",
    [
        (2019, True, IntervalTuple(10, 28, 0, 0), "Wednesday", LeapYearMismatch),
        (2001, True, None, None, LeapYearMismatch),
        (2002, False, IntervalTuple(10, 28, 0, 0), "Tuesday", StartDayMismatch),
        (2020, True, IntervalTuple(1, 1, 0, 0), "Friday", StartDayMismatch),
    ],
)
def test_validate_year_incorrect(year, is_leap, date, day, error):
    with pytest.raises(error):
        validate_year(year, is_leap, date, day)


@pytest.mark.parametrize(
    "dates, expected",
    [
        (
            [
                IntervalTuple(2, 28, 0, 0),
                IntervalTuple(2, 29, 0, 0),
                IntervalTuple(3, 1, 0, 0),
                IntervalTuple(3, 2, 0, 0),
            ],
            True,
        ),
        (
            [
                IntervalTuple(2, 27, 0, 0),
                IntervalTuple(2, 28, 0, 0),
                IntervalTuple(3, 1, 0, 0),
                IntervalTuple(3, 2, 0, 0),
            ],
            False,
        ),
    ],
)
def test_is_leap_year_ts_to_d(dates, expected):
    assert is_leap_year_ts_to_d(dates) is expected


@pytest.mark.parametrize(
    "interval, dates, n_days, expected",
    [
        (
            M,
            [IntervalTuple(1, 1, 0, 0), IntervalTuple(2, 1, 0, 0), IntervalTuple(3, 1, 0, 0),],
            [31, 29, 31],
            True,
        ),
        (
            M,
            [IntervalTuple(1, 1, 0, 0), IntervalTuple(2, 1, 0, 0), IntervalTuple(3, 1, 0, 0),],
            [31, 28, 31],
            False,
        ),
        (A, [IntervalTuple(1, 1, 0, 0),], [366], True),
        (
            M,
            [IntervalTuple(3, 1, 0, 0), IntervalTuple(4, 1, 0, 0), IntervalTuple(5, 1, 0, 0),],
            [31, 30, 31],
            False,
        ),
        (A, [IntervalTuple(1, 1, 0, 0),], [365], False),
        (RP, [IntervalTuple(1, 1, 0, 0)], [366], False,),
    ],
)
def test_is_leap_year_m_to_rp(interval, dates, n_days, expected):
    assert is_leap_year_m_to_rp(interval, dates, n_days) is expected


@pytest.mark.parametrize(
    "is_leap, date, day, max_year, expected",
    [
        (True, IntervalTuple(2, 1, 0, 0), "Sunday", 2020, 2004),
        (True, IntervalTuple(2, 2, 0, 0), "Monday", 2020, 2004),
        (True, IntervalTuple(2, 3, 0, 0), "Tuesday", 2020, 2004),
        (True, IntervalTuple(2, 4, 0, 0), "Wednesday", 2020, 2004),
        (True, IntervalTuple(2, 5, 0, 0), "Thursday", 2020, 2004),
        (True, IntervalTuple(2, 6, 0, 0), "Friday", 2020, 2004),
        (True, IntervalTuple(2, 7, 0, 0), "Saturday", 2020, 2004),
        (False, IntervalTuple(2, 1, 0, 0), "Sunday", 2020, 2015),
        (False, IntervalTuple(2, 2, 0, 0), "Monday", 2020, 2015),
        (False, IntervalTuple(2, 3, 0, 0), "Tuesday", 2020, 2015),
        (False, IntervalTuple(2, 4, 0, 0), "Wednesday", 2020, 2015),
        (False, IntervalTuple(2, 5, 0, 0), "Thursday", 2020, 2015),
        (False, IntervalTuple(2, 6, 0, 0), "Friday", 2020, 2015),
        (False, IntervalTuple(2, 7, 0, 0), "Saturday", 2020, 2015),
    ],
)
def test_seek_year(is_leap, date, day, max_year, expected):
    assert seek_year(is_leap, date, day, max_year) == expected


@pytest.mark.parametrize(
    "is_leap, date, day, max_year, expected",
    [
        (True, IntervalTuple(1, 1, 0, 0), "Sunday", 2030, [2012, 1984, 1956]),
        (True, IntervalTuple(1, 1, 0, 0), "Monday", 2030, [2024, 1996, 1968]),
        (True, IntervalTuple(1, 1, 0, 0), "Tuesday", 2030, [2008, 1980, 1952]),
        (True, IntervalTuple(1, 1, 0, 0), "Wednesday", 2030, [2020, 1992, 1964]),
        (True, IntervalTuple(1, 1, 0, 0), "Friday", 2030, [2016, 1988, 1960]),
        (True, IntervalTuple(1, 1, 0, 0), "Saturday", 2030, [2028, 2000, 1972]),
        (False, IntervalTuple(1, 1, 0, 0), "Sunday", 2030, [2023, 2017, 2006]),
        (False, IntervalTuple(1, 1, 0, 0), "Monday", 2030, [2029, 2018, 2007]),
        (False, IntervalTuple(1, 1, 0, 0), "Tuesday", 2030, [2030, 2019, 2013]),
        (False, IntervalTuple(1, 1, 0, 0), "Wednesday", 2030, [2025, 2014, 2003]),
        (False, IntervalTuple(1, 1, 0, 0), "Friday", 2030, [2027, 2021, 2010]),
        (False, IntervalTuple(1, 1, 0, 0), "Saturday", 2030, [2022, 2011, 2005]),
    ],
)
def test_get_allowed_years(is_leap, date, day, max_year, expected):
    assert get_allowed_years(is_leap, date, day, max_year, n_samples=3) == expected
