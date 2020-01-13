import unittest
import pandas as pd
from esofile_reader.processing.interval_processor import *
from esofile_reader.processing.interval_processor import (_to_timestamp, _gen_dt,
                                                          _set_start_date, _env_ts_d_h,
                                                          _env_r)
from esofile_reader.constants import *
from esofile_reader.utils.mini_classes import IntervalTuple
from datetime import datetime


class TestIntervalProcessing(unittest.TestCase):
    def test_datetime_helper_year_end(self):
        d = (12, 31, 24, 60)
        self.assertEqual(datetime_helper(*d), (1, 1, 0, 0))

    def test_datetime_helper_month_end(self):
        d = (10, 31, 24, 60)
        self.assertEqual(datetime_helper(*d), (11, 1, 0, 0))

    def test_datetime_helper_day_end(self):
        d = (10, 25, 24, 60)
        self.assertEqual(datetime_helper(*d), (10, 26, 0, 0))

    def test_datetime_helper_last_hour(self):
        d = (10, 25, 24, 30)
        self.assertEqual(datetime_helper(*d), (10, 25, 23, 30))

    def test_datetime_helper_end_minute(self):
        d = (10, 25, 22, 60)
        self.assertEqual(datetime_helper(*d), (10, 25, 22, 0))

    def test_datetime_helper_other(self):
        d = (10, 25, 22, 10)
        self.assertEqual(datetime_helper(*d), (10, 25, 21, 10))

    def test_parse_result_dt_runperiod(self):
        date = datetime(2002, 1, 1)
        dt = parse_result_dt(date, 2, 3, 4, 30)
        self.assertEqual(dt, datetime(2002, 2, 3, 3, 30))

    def test_parse_result_dt_monthly(self):
        date = datetime(2002, 1, 1)
        dt = parse_result_dt(date, None, 3, 4, 30)
        self.assertEqual(dt, datetime(2002, 1, 3, 3, 30))

    def test_parse_result_dt_daily(self):
        date = datetime(2002, 1, 1)
        dt = parse_result_dt(date, None, None, 10, 30)
        self.assertEqual(dt, datetime(2002, 1, 1, 9, 30))

    def test_parse_result_dt_daily_add_year(self):
        date = datetime(2002, 12, 31)
        dt = parse_result_dt(date, None, None, 24, 60)
        self.assertEqual(dt, datetime(2003, 1, 1, 0, 0))

    def test_is_end_day(self):
        days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        for i, d in enumerate(days):
            self.assertTrue(is_end_day(i + 1, d))

    def test_is_not_end_day(self):
        days = [25, 26, 1, 8, 5, 24, 30, 30, 3, 1, 3, 25]
        for i, d in enumerate(days):
            self.assertFalse(is_end_day(i + 1, d))

    def test_month_act_days(self):
        m_envs = [[31, 59, 90, 97], [6, 30, 52, 70, 71]]
        out = month_act_days(m_envs)
        self.assertEqual(out, [[31, 28, 31, 7], [6, 24, 22, 18, 1]])

    def test_find_num_of_days_annual(self):
        ann_num_days = [[1], [1, 2]]
        rp_num_days = [[365], [700]]
        out = find_num_of_days_annual(ann_num_days, rp_num_days)
        self.assertEqual(out, [[365], [350, 350]])

    def test_get_num_of_days(self):
        days = {
            M: [[10, 20, 30], [11, 12, 13]],
            RP: [[100], [700]],
            A: [[1], [1, 2]]
        }
        out = get_num_of_days(days)
        self.assertEqual(out, {"monthly": [[10, 10, 10], [11, 1, 1]],
                               "runperiod": [[100], [700]],
                               "annual": [[100], [350, 350]]})

    def test_month_end_date(self):
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
            datetime(2002, 12, 31, 1)
        ]
        for date, end_date in zip(dates, end_dates):
            self.assertEqual(month_end_date(date), end_date)

    def test_incr_year_env_monthly(self):
        self.assertTrue(
            incr_year_env(
                IntervalTuple(1, 1, 0, 0),
                IntervalTuple(1, 1, 0, 0),
                IntervalTuple(2, 1, 0, 0)
            )
        )
        self.assertTrue(
            incr_year_env(
                IntervalTuple(2, 1, 0, 0),
                IntervalTuple(1, 1, 0, 0),
                IntervalTuple(2, 1, 0, 0)
            )
        )
        self.assertFalse(
            incr_year_env(
                IntervalTuple(1, 1, 0, 0),
                IntervalTuple(2, 1, 0, 0),
                IntervalTuple(3, 1, 0, 0)
            )
        )

    def test_incr_year_env_daily(self):
        self.assertTrue(
            incr_year_env(
                IntervalTuple(1, 1, 1, 0),
                IntervalTuple(12, 31, 24, 60),
                IntervalTuple(1, 1, 1, 0)
            )
        )
        self.assertTrue(
            incr_year_env(
                IntervalTuple(1, 1, 1, 0),
                IntervalTuple(1, 1, 1, 0),
                IntervalTuple(1, 1, 2, 0)
            )
        )
        self.assertFalse(
            incr_year_env(
                IntervalTuple(1, 1, 1, 0),
                IntervalTuple(1, 1, 2, 0),
                IntervalTuple(1, 1, 3, 0)
            )
        )

    def test__to_timestamp(self):
        ts = _to_timestamp(2002, IntervalTuple(1, 1, 0, 0))
        self.assertEqual(ts, datetime(2002, 1, 1, 0, 0))

        ts = _to_timestamp(2002, IntervalTuple(1, 1, 1, 30))
        self.assertEqual(ts, datetime(2002, 1, 1, 0, 30))

    def test__gen_dt(self):
        envs = [[
            IntervalTuple(1, 1, 0, 0),
            IntervalTuple(2, 1, 0, 0),
            IntervalTuple(3, 1, 0, 0),
        ], [
            IntervalTuple(1, 1, 0, 0),
            IntervalTuple(2, 1, 0, 0),
            IntervalTuple(3, 1, 0, 0),
        ], [
            IntervalTuple(1, 1, 0, 0),
            IntervalTuple(2, 1, 0, 0),
            IntervalTuple(3, 1, 0, 0),
        ]]
        dt_envs = _gen_dt(envs, 2002)
        self.assertEqual(dt_envs,
                         [[datetime(2002, 1, 1, 0, 0, 0), datetime(2002, 2, 1, 0, 0, 0),
                           datetime(2002, 3, 1, 0, 0, 0)],
                          [datetime(2003, 1, 1, 0, 0, 0), datetime(2003, 2, 1, 0, 0, 0),
                           datetime(2003, 3, 1, 0, 0, 0)],
                          [datetime(2004, 1, 1, 0, 0, 0), datetime(2004, 2, 1, 0, 0, 0),
                           datetime(2004, 3, 1, 0, 0, 0)]])

    def test__gen_dt_span_year(self):
        envs = [[
            IntervalTuple(12, 31, 23, 60),
            IntervalTuple(12, 31, 24, 60),
            IntervalTuple(1, 1, 1, 60),
        ], [
            IntervalTuple(12, 31, 23, 60),
            IntervalTuple(12, 31, 24, 60),
            IntervalTuple(1, 1, 1, 60),
        ], [
            IntervalTuple(12, 31, 23, 60),
            IntervalTuple(12, 31, 24, 60),
            IntervalTuple(1, 1, 1, 60),
        ]]
        dt_envs = _gen_dt(envs, 2002)
        self.assertEqual(dt_envs,
                         [[datetime(2002, 12, 31, 23, 00, 00), datetime(2003, 1, 1, 00, 00, 00),
                           datetime(2003, 1, 1, 1, 00, 00)],
                          [datetime(2004, 12, 31, 23, 00, 00), datetime(2005, 1, 1, 00, 00, 00),
                           datetime(2005, 1, 1, 1, 00, 00)],
                          [datetime(2006, 12, 31, 23, 00, 00), datetime(2007, 1, 1, 00, 00, 00),
                           datetime(2007, 1, 1, 1, 00, 00)]])

    def test_convert_to_dt_index(self):
        pass

    def test__set_start_date(self):
        pass

    def test_update_start_dates(self):
        pass

    def test__env_ts_d_h(self):
        pass

    def test_find_env_ts_to_d(self):
        pass

    def test__env_m(self):
        pass

    def test__env_r(self):
        pass

    def test_find_env_m_to_rp(self):
        pass

    def test_find_environment(self):
        pass

    def test_flat_values(self):
        pass

    def test_interval_processor(self):
        pass
