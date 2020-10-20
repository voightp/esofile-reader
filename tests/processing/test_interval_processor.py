import pytest

from esofile_reader.processing.esofile_intervals import *


def test_datetime_helper_year_end():
    d = (12, 31, 24, 60)
    assert datetime_helper(*d) == (1, 1, 0, 0)


def test_datetime_helper_month_end():
    d = (10, 31, 24, 60)
    assert datetime_helper(*d) == (11, 1, 0, 0)


def test_datetime_helper_day_end():
    d = (10, 25, 24, 60)
    assert datetime_helper(*d) == (10, 26, 0, 0)


def test_datetime_helper_last_hour():
    d = (10, 25, 24, 30)
    assert datetime_helper(*d) == (10, 25, 23, 30)


def test_datetime_helper_end_minute():
    d = (10, 25, 22, 60)
    assert datetime_helper(*d) == (10, 25, 22, 0)


def test_datetime_helper_other():
    d = (10, 25, 22, 10)
    assert datetime_helper(*d) == (10, 25, 21, 10)


def test_parse_result_dt_runperiod():
    date = datetime(2002, 1, 1)
    dt = parse_result_datetime(date, 2, 3, 4, 30)
    assert dt == datetime(2002, 2, 3, 3, 30)


def test_parse_result_dt_monthly():
    date = datetime(2002, 1, 1)
    dt = parse_result_datetime(date, None, 3, 4, 30)
    assert dt == datetime(2002, 1, 3, 3, 30)


def test_parse_result_dt_daily():
    date = datetime(2002, 1, 1)
    dt = parse_result_datetime(date, None, None, 10, 30)
    assert dt == datetime(2002, 1, 1, 9, 30)


def test_parse_result_dt_daily_add_year():
    date = datetime(2002, 12, 31)
    dt = parse_result_datetime(date, None, None, 24, 60)
    assert dt == datetime(2003, 1, 1, 0, 0)


def test_is_end_day():
    days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    for i, d in enumerate(days):
        assert is_end_day(i + 1, d)


def test_is_not_end_day():
    days = [25, 26, 1, 8, 5, 24, 30, 30, 3, 1, 3, 25]
    for i, d in enumerate(days):
        assert not is_end_day(i + 1, d)


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


def test_month_end_date():
    dates = [datetime(2002, i + 1, 10, 1) for i in range(12)]
    end_dates = [
        datetime(2002, 1, 31, 1),
        datetime(2002, 2, 28, 1),
        datetime(2002, 3, 31, 1),
        datetime(2002, 4, 30, 1),
        datetime(2002, 5, 31, 1),
        datetime(2002, 6, 30, 1),
        datetime(2002, 7, 31, 1),
        datetime(2002, 8, 31, 1),
        datetime(2002, 9, 30, 1),
        datetime(2002, 10, 31, 1),
        datetime(2002, 11, 30, 1),
        datetime(2002, 12, 31, 1),
    ]
    for date, end_date in zip(dates, end_dates):
        assert get_month_end_date(date) == end_date


@pytest.mark.parametrize(
    "first_step_data,current_step_data,previous_step_data",
    [
        (IntervalTuple(1, 1, 0, 0), IntervalTuple(1, 1, 0, 0), IntervalTuple(2, 1, 0, 0)),
        (IntervalTuple(2, 1, 0, 0), IntervalTuple(1, 1, 0, 0), IntervalTuple(2, 1, 0, 0)),
        (IntervalTuple(1, 1, 1, 0), IntervalTuple(12, 31, 24, 60), IntervalTuple(1, 1, 1, 0)),
        (IntervalTuple(1, 1, 1, 0), IntervalTuple(1, 1, 1, 0), IntervalTuple(1, 1, 2, 0)),
    ],
    ids=["monthly", "monthly", "daily", "daily"],
)
def test_increment_year_monthly(first_step_data, current_step_data, previous_step_data):
    assert check_year_increment(first_step_data, current_step_data, previous_step_data)


@pytest.mark.parametrize(
    "first_step_data,current_step_data,previous_step_data",
    [
        (IntervalTuple(1, 1, 0, 0), IntervalTuple(2, 1, 0, 0), IntervalTuple(3, 1, 0, 0)),
        (IntervalTuple(1, 1, 1, 0), IntervalTuple(1, 1, 2, 0), IntervalTuple(1, 1, 3, 0)),
    ],
    ids=["monthly", "daily"],
)
def test_do_not_increment_year_monthly(first_step_data, current_step_data, previous_step_data):
    assert not check_year_increment(first_step_data, current_step_data, previous_step_data)


@pytest.mark.parametrize(
    "year,interval_tuple,expected",
    [
        (2002, IntervalTuple(1, 1, 0, 0), datetime(2002, 1, 1, 0, 0)),
        (2002, IntervalTuple(1, 1, 1, 30), datetime(2002, 1, 1, 0, 30)),
    ],
)
def test__to_timestamp(year, interval_tuple, expected):
    assert convert_to_datetime(year, interval_tuple) == expected


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
    dates = convert_to_datetime_index(env_dct, 2002)
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
