from esofile_reader.processing.esofile_time import *
from esofile_reader.processing.extensions.esofile import read_file
from esofile_reader.processing.progress_logger import GenericLogger
from tests.session_fixtures import *


@pytest.mark.parametrize(
    "year,interval_tuple,expected",
    [
        (2002, EsoTimestamp(1, 1, 0, 0), datetime(2002, 1, 1, 0, 0)),
        (2002, EsoTimestamp(1, 1, 1, 30), datetime(2002, 1, 1, 0, 30)),
        (2002, EsoTimestamp(12, 31, 24, 60), datetime(2003, 1, 1, 0, 0)),
        (2002, EsoTimestamp(10, 31, 24, 60), datetime(2002, 11, 1, 0, 0)),
        (2002, EsoTimestamp(10, 25, 24, 60), datetime(2002, 10, 26, 0, 0, 0)),
        (2002, EsoTimestamp(10, 25, 22, 60), datetime(2002, 10, 25, 22, 0, 0)),
        (2002, EsoTimestamp(10, 25, 22, 10), datetime(2002, 10, 25, 21, 10, 0)),
    ],
)
def test_parse_eso_timestamp(year, interval_tuple, expected):
    assert parse_eso_timestamp(year, *interval_tuple) == expected


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


@pytest.mark.parametrize("year, expected", [(2020, 366), (2001, 365)])
def test_get_num_of_days(year, expected):
    days = {M: [10, 20, 30], RP: [123], A: [None]}
    dates = {A: [datetime(year, 1, 1)]}
    out = get_n_days_from_cumulative(days, dates)
    assert out == {"monthly": [10, 10, 10], "runperiod": [123], "annual": [expected]}


@pytest.mark.parametrize(
    "first_step_data,current_step_data,increment",
    [
        (EsoTimestamp(1, 1, 0, 0), EsoTimestamp(1, 1, 0, 0), True),
        (EsoTimestamp(2, 1, 0, 0), EsoTimestamp(1, 1, 0, 0), True),
        (EsoTimestamp(1, 1, 1, 0), EsoTimestamp(12, 31, 24, 60), False),
        (EsoTimestamp(1, 1, 1, 0), EsoTimestamp(1, 1, 1, 0), True),
    ],
    ids=["monthly", "monthly", "daily", "daily"],
)
def test_increment_year(first_step_data, current_step_data, increment):
    assert check_year_increment(first_step_data, current_step_data,) is increment


@pytest.mark.parametrize(
    "first_step_data,current_step_data",
    [
        (EsoTimestamp(1, 1, 0, 0), EsoTimestamp(2, 1, 0, 0)),
        (EsoTimestamp(1, 1, 1, 0), EsoTimestamp(1, 1, 2, 0)),
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
            [EsoTimestamp(1, 1, 0, 0), EsoTimestamp(2, 1, 0, 0), EsoTimestamp(3, 1, 0, 0)],
            [
                datetime(2002, 1, 1, 0, 0, 0),
                datetime(2002, 2, 1, 0, 0, 0),
                datetime(2002, 3, 1, 0, 0, 0),
            ],
        ),
        (
            2002,
            [
                EsoTimestamp(12, 31, 23, 60),
                EsoTimestamp(12, 31, 24, 60),
                EsoTimestamp(1, 1, 1, 60),
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
            EsoTimestamp(12, 31, 23, 60),
            EsoTimestamp(12, 31, 24, 60),
            EsoTimestamp(1, 1, 1, 60),
        ],
        "monthly": [
            EsoTimestamp(1, 1, 0, 0),
            EsoTimestamp(2, 1, 0, 0),
            EsoTimestamp(3, 1, 0, 0),
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
        (2020, True, EsoTimestamp(10, 28, 0, 0), "Wednesday"),
        (2020, True, EsoTimestamp(2, 29, 0, 0), "Saturday"),
        (2020, True, EsoTimestamp(1, 1, 0, 0), "Wednesday"),
        (2002, False, EsoTimestamp(10, 28, 0, 0), "Monday"),
        (2002, False, EsoTimestamp(2, 28, 0, 0), "Thursday"),
        (2002, False, EsoTimestamp(1, 1, 0, 0), "Tuesday"),
    ],
)
def test_validate_year(year, is_leap, date, day):
    assert validate_year(year, is_leap, date, day) is None


@pytest.mark.parametrize(
    "year, is_leap, date, day, error",
    [
        (2019, True, EsoTimestamp(10, 28, 0, 0), "Wednesday", LeapYearMismatch),
        (2001, True, None, None, LeapYearMismatch),
        (2002, False, EsoTimestamp(10, 28, 0, 0), "Tuesday", StartDayMismatch),
        (2020, True, EsoTimestamp(1, 1, 0, 0), "Friday", StartDayMismatch),
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
                EsoTimestamp(2, 28, 0, 0),
                EsoTimestamp(2, 29, 0, 0),
                EsoTimestamp(3, 1, 0, 0),
                EsoTimestamp(3, 2, 0, 0),
            ],
            True,
        ),
        (
            [
                EsoTimestamp(2, 27, 0, 0),
                EsoTimestamp(2, 28, 0, 0),
                EsoTimestamp(3, 1, 0, 0),
                EsoTimestamp(3, 2, 0, 0),
            ],
            False,
        ),
    ],
)
def test_is_leap_year_ts_to_d(dates, expected):
    assert is_leap_year_ts_to_d(dates) is expected


@pytest.mark.parametrize(
    "is_leap, date, day, max_year, expected",
    [
        (True, EsoTimestamp(2, 1, 0, 0), "Sunday", 2020, 2004),
        (True, EsoTimestamp(2, 2, 0, 0), "Monday", 2020, 2004),
        (True, EsoTimestamp(2, 3, 0, 0), "Tuesday", 2020, 2004),
        (True, EsoTimestamp(2, 4, 0, 0), "Wednesday", 2020, 2004),
        (True, EsoTimestamp(2, 5, 0, 0), "Thursday", 2020, 2004),
        (True, EsoTimestamp(2, 6, 0, 0), "Friday", 2020, 2004),
        (True, EsoTimestamp(2, 7, 0, 0), "Saturday", 2020, 2004),
        (False, EsoTimestamp(2, 1, 0, 0), "Sunday", 2020, 2015),
        (False, EsoTimestamp(2, 2, 0, 0), "Monday", 2020, 2015),
        (False, EsoTimestamp(2, 3, 0, 0), "Tuesday", 2020, 2015),
        (False, EsoTimestamp(2, 4, 0, 0), "Wednesday", 2020, 2015),
        (False, EsoTimestamp(2, 5, 0, 0), "Thursday", 2020, 2015),
        (False, EsoTimestamp(2, 6, 0, 0), "Friday", 2020, 2015),
        (False, EsoTimestamp(2, 7, 0, 0), "Saturday", 2020, 2015),
    ],
)
def test_seek_year(is_leap, date, day, max_year, expected):
    assert seek_year(is_leap, date, day, max_year) == expected


@pytest.mark.parametrize(
    "is_leap, date, day, max_year, expected",
    [
        (True, EsoTimestamp(1, 1, 0, 0), "Sunday", 2030, [2012, 1984, 1956]),
        (True, EsoTimestamp(1, 1, 0, 0), "Monday", 2030, [2024, 1996, 1968]),
        (True, EsoTimestamp(1, 1, 0, 0), "Tuesday", 2030, [2008, 1980, 1952]),
        (True, EsoTimestamp(1, 1, 0, 0), "Wednesday", 2030, [2020, 1992, 1964]),
        (True, EsoTimestamp(1, 1, 0, 0), "Friday", 2030, [2016, 1988, 1960]),
        (True, EsoTimestamp(1, 1, 0, 0), "Saturday", 2030, [2028, 2000, 1972]),
        (False, EsoTimestamp(1, 1, 0, 0), "Sunday", 2030, [2023, 2017, 2006]),
        (False, EsoTimestamp(1, 1, 0, 0), "Monday", 2030, [2029, 2018, 2007]),
        (False, EsoTimestamp(1, 1, 0, 0), "Tuesday", 2030, [2030, 2019, 2013]),
        (False, EsoTimestamp(1, 1, 0, 0), "Wednesday", 2030, [2025, 2014, 2003]),
        (False, EsoTimestamp(1, 1, 0, 0), "Friday", 2030, [2027, 2021, 2010]),
        (False, EsoTimestamp(1, 1, 0, 0), "Saturday", 2030, [2022, 2011, 2005]),
    ],
)
def test_get_allowed_years(is_leap, date, day, max_year, expected):
    assert get_allowed_years(is_leap, date, day, max_year, n_samples=3) == expected


@pytest.mark.parametrize(
    "drop_intervals, year, expected_start_end",
    [
        (
            [],
            None,
            {
                TS: (datetime(2020, 1, 1, 0, 30), datetime(2021, 1, 1, 0)),
                H: (datetime(2020, 1, 1, 1, 0), datetime(2021, 1, 1, 0)),
                D: (datetime(2020, 1, 1), datetime(2020, 12, 31, 0)),
                M: (datetime(2020, 1, 1), datetime(2020, 12, 1, 0)),
                A: (datetime(2020, 1, 1), datetime(2020, 1, 1)),
                RP: (datetime(2020, 1, 1), datetime(2020, 1, 1)),
            },
        ),
        (
            [TS, H, D],
            None,
            {
                M: (datetime(2002, 1, 1), datetime(2002, 12, 1, 0)),
                A: (datetime(2002, 1, 1), datetime(2002, 1, 1)),
                RP: (datetime(2002, 1, 1), datetime(2002, 1, 1)),
            },
        ),
        (
            [],
            2020,
            {
                TS: (datetime(2020, 1, 1, 0, 30), datetime(2021, 1, 1, 0)),
                H: (datetime(2020, 1, 1, 1, 0), datetime(2021, 1, 1, 0)),
                D: (datetime(2020, 1, 1), datetime(2020, 12, 31, 0)),
                M: (datetime(2020, 1, 1), datetime(2020, 12, 1, 0)),
                A: (datetime(2020, 1, 1), datetime(2020, 1, 1)),
                RP: (datetime(2020, 1, 1), datetime(2020, 1, 1)),
            },
        ),
        (
            [TS, H, D],
            2010,
            {
                M: (datetime(2010, 1, 1), datetime(2010, 12, 1, 0)),
                A: (datetime(2010, 1, 1), datetime(2010, 1, 1)),
                RP: (datetime(2010, 1, 1), datetime(2010, 1, 1)),
            },
        ),
    ],
)
def test_convert_raw_date_data(drop_intervals, year, expected_start_end):
    with open(Path(EPLUS_TEST_FILES_PATH, "eplusout_leap_year.eso"), "r") as file:
        logger = GenericLogger("foo")
        with logger.log_task("Test leap year"):
            all_raw_outputs = read_file(file, logger)
            raw_outputs = all_raw_outputs[-1]
            for interval in drop_intervals:
                raw_outputs.remove_interval_data(interval)

            dates = convert_raw_date_data(raw_outputs.dates, raw_outputs.days_of_week, year)
            for interval, date_arr in dates.items():
                assert date_arr[0] == expected_start_end[interval][0]
                assert date_arr[-1] == expected_start_end[interval][1]
