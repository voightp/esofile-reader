import unittest
import pandas as pd

from pandas.testing import assert_frame_equal

from esofile_reader.convertor import *


class TestOutputsConversion(unittest.TestCase):
    def test_apply_conversion(self):
        columns = pd.MultiIndex.from_tuples([(1, "bar"), (2, "baz")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [2])

        test_mi = pd.MultiIndex.from_tuples([(1, "foo"), (2, "baz")], names=["id", "units"])
        test_df = pd.DataFrame([[0.5, 1], [1, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_apply_conversion_peak(self):
        columns = pd.MultiIndex.from_tuples(
            [(1, "bar", "value"), (2, "bar", "ts")], names=["id", "units", "data"]
        )
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [2])

        test_mi = pd.MultiIndex.from_tuples(
            [(1, "foo", "value"), (2, "foo", "ts")], names=["id", "units", "data"]
        )
        test_df = pd.DataFrame([[0.5, 1], [1, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_apply_conversion_callable(self):
        columns = pd.MultiIndex.from_tuples(
            [(1, "bar", "value"), (2, "bar", "ts")], names=["id", "units", "data"]
        )
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [lambda x: 2 * x])

        test_mi = pd.MultiIndex.from_tuples(
            [(1, "foo", "value"), (2, "foo", "ts")], names=["id", "units", "data"]
        )
        test_df = pd.DataFrame([[2, 1], [4, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_apply_conversion_array(self):
        columns = pd.MultiIndex.from_tuples(
            [(1, "bar", "value"), (2, "bar", "ts")], names=["id", "units", "data"]
        )
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [[2, 4]])

        test_mi = pd.MultiIndex.from_tuples(
            [(1, "foo", "value"), (2, "foo", "ts")], names=["id", "units", "data"]
        )
        test_df = pd.DataFrame([[0.5, 1], [0.5, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_convert_units_energy(self):
        columns = pd.MultiIndex.from_tuples([(1, "J"), (2, "W")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = convert_units(df, "SI", "W", "kWh")

        test_mi = pd.MultiIndex.from_tuples([(1, "kWh"), (2, "W")], names=["id", "units"])
        test_df = pd.DataFrame([[1 / 3600000, 1], [2 / 3600000, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_convert_units_energy_per_area(self):
        columns = pd.MultiIndex.from_tuples([(1, "J/m2"), (2, "W")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = convert_units(df, "SI", "W", "kWh")

        test_mi = pd.MultiIndex.from_tuples([(1, "kWh/m2"), (2, "W")], names=["id", "units"])
        test_df = pd.DataFrame([[1 / 3600000, 1], [2 / 3600000, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_convert_units_power(self):
        columns = pd.MultiIndex.from_tuples([(1, "J"), (2, "W")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = convert_units(df, "SI", "kW", "J")

        test_mi = pd.MultiIndex.from_tuples([(1, "J"), (2, "kW")], names=["id", "units"])
        test_df = pd.DataFrame([[1, 1 / 1000], [2, 2 / 1000]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_convert_units_power_per_area(self):
        columns = pd.MultiIndex.from_tuples([(1, "J/m2"), (2, "W/m2")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = convert_units(df, "SI", "kW", "J")

        test_mi = pd.MultiIndex.from_tuples([(1, "J/m2"), (2, "kW/m2")], names=["id", "units"])
        test_df = pd.DataFrame([[1, 1 / 1000], [2, 2 / 1000]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_convert_units_si_to_ip(self):
        columns = pd.MultiIndex.from_tuples(
            [(1, "m"), (2, "W/m2"), (3, "deltaC")], names=["id", "units"]
        )
        df = pd.DataFrame([[1, 1, 1], [2, 2, 2]], columns=columns)
        out = convert_units(df, "IP", "W", "J")

        test_mi = pd.MultiIndex.from_tuples(
            [(1, "ft"), (2, "W/m2"), (3, "deltaF")], names=["id", "units"]
        )
        test_df = pd.DataFrame(
            [[1 / 0.3048, 1, 1.8], [2 / 0.30479999953, 2, 3.6]], columns=test_mi
        )
        assert_frame_equal(out, test_df)

    def test_convert_units_no_valid(self):
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = convert_units(df, "SI", "W", "J")

        assert_frame_equal(df, out)

    def test_update_multiindex_axis0(self):
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
        index = pd.Index(["a", "b"], name="dt")
        df = pd.DataFrame([[1, 1], [2, 2]], index=index, columns=columns)

        update_multiindex(df, "dt", ["a"], ["c"], axis=0)

        self.assertListEqual(df.index.tolist(), [("c",), ("b",)])

    def test_update_multiindex_axis1(self):
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
        index = pd.Index(["a", "b"], name="dt")
        df = pd.DataFrame([[1, 1], [2, 2]], index=index, columns=columns)

        update_multiindex(df, "units", ["m"], ["ft"], axis=1)

        self.assertListEqual(df.columns.tolist(), [(1, "ft"), (2, "W/m2")])

    def test_rate_and_energy_units(self):
        self.assertTrue(rate_and_energy_units(["W", "J", "J"]))

    def test_rate_and_energy_units_per_area(self):
        self.assertTrue(rate_and_energy_units(["W/m2", "J/m2", "J/m2"]))

    def test_rate_and_energy_units_invalid(self):
        self.assertFalse(rate_and_energy_units(["W", "J", "J/m2"]))

    def test_rate_and_energy_units_per_area_invalid(self):
        self.assertFalse(rate_and_energy_units(["W/m2", "JW/m2", "J"]))

    def test_get_n_steps(self):
        dt_index = pd.date_range("01/01/2002 01:00", freq="h", periods=5)
        steps = get_n_steps(dt_index)
        self.assertEqual(steps, 1.0)

        dt_index = pd.date_range("01/01/2002 01:00", freq="30min", periods=5)
        steps = get_n_steps(dt_index)
        self.assertEqual(steps, 2.0)

    def test_rate_to_energy_hourly(self):
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
        index = pd.Index(
            pd.date_range("01/01/2002 01:00", freq="h", periods=3), name=TIMESTAMP_COLUMN
        )
        df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)

        df = convert_rate_to_energy(df, H)

        test_columns = pd.MultiIndex.from_tuples(
            [(1, "m"), (2, "J/m2")], names=["id", "units"]
        )
        test_index = pd.Index(
            pd.date_range("01/01/2002 01:00", freq="h", periods=3), name=TIMESTAMP_COLUMN
        )
        test_df = pd.DataFrame(
            [[1, 1 * 3600], [2, None], [3, 3 * 3600]], index=test_index, columns=test_columns
        )

        assert_frame_equal(df, test_df)

    def test_rate_to_energy_daily(self):
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
        index = pd.Index(
            pd.date_range("01/01/2002 01:00", freq="d", periods=3), name=TIMESTAMP_COLUMN
        )
        df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)

        df = convert_rate_to_energy(df, D)

        test_columns = pd.MultiIndex.from_tuples(
            [(1, "m"), (2, "J/m2")], names=["id", "units"]
        )
        test_index = pd.Index(
            pd.date_range("01/01/2002 01:00", freq="d", periods=3), name=TIMESTAMP_COLUMN
        )
        test_df = pd.DataFrame(
            [[1, 1 * 3600 * 24], [2, None], [3, 3 * 3600 * 24]],
            index=test_index,
            columns=test_columns,
        )

        assert_frame_equal(df, test_df)

    def test_rate_to_energy_n_days(self):
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
        index = pd.Index(
            pd.date_range("01/01/2002 01:00", freq="30d", periods=3), name=TIMESTAMP_COLUMN
        )
        df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)
        nd_df = pd.DataFrame({"n_days": [30, 30, 31]}, index=index)

        df = convert_rate_to_energy(df, M, nd_df["n_days"])

        test_columns = pd.MultiIndex.from_tuples(
            [(1, "m"), (2, "J/m2")], names=["id", "units"]
        )
        test_index = pd.Index(
            pd.date_range("01/01/2002 01:00", freq="30d", periods=3), name=TIMESTAMP_COLUMN
        )
        test_df = pd.DataFrame(
            [[1, 1 * 3600 * 24 * 30], [2, None], [3, 3 * 3600 * 24 * 31]],
            index=test_index,
            columns=test_columns,
        )

        assert_frame_equal(df, test_df)

    def test_rate_to_energy_na(self):
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "FOO/m2")], names=["id", "units"])
        index = pd.Index(
            pd.date_range("01/01/2002 01:00", freq="30d", periods=3), name=TIMESTAMP_COLUMN
        )
        df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)
        nd_df = pd.DataFrame({"n_days": [30, 30, 31]}, index=index)

        df = convert_rate_to_energy(df, M, nd_df["n_days"])
        assert_frame_equal(df, df)

    def test_rate_to_energy_missing_ndays(self):
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "FOO/m2")], names=["id", "units"])
        index = pd.Index(
            pd.date_range("01/01/2002 01:00", freq="30d", periods=3), name=TIMESTAMP_COLUMN
        )
        df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)

        with self.assertRaises(TypeError):
            _ = convert_rate_to_energy(df, M)

    def test_energy_units_invalid(self):
        self.assertIsNone(energy_table("FOO"))

    def test_rate_units_invalid(self):
        self.assertIsNone(rate_table("FOO"))


if __name__ == "__main__":
    unittest.main()
