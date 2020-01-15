import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal
from esofile_reader.outputs.convertor import *


class TestOutputsConversion(unittest.TestCase):
    def test_apply_conversion(self):
        columns = pd.MultiIndex.from_tuples([(1, "bar"), (2, "baz")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [2])

        test_mi = pd.MultiIndex.from_tuples([(1, "foo"), (2, "baz")], names=["id", "units"])
        test_df = pd.DataFrame([[0.5, 1], [1, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_apply_conversion_peak(self):
        columns = pd.MultiIndex.from_tuples([(1, "bar", "value"), (2, "bar", "ts")],
                                            names=["id", "units", "data"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [2])

        test_mi = pd.MultiIndex.from_tuples([(1, "foo", "value"), (2, "foo", "ts")],
                                            names=["id", "units", "data"])
        test_df = pd.DataFrame([[0.5, 1], [1, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_apply_conversion_callable(self):
        columns = pd.MultiIndex.from_tuples([(1, "bar", "value"), (2, "bar", "ts")],
                                            names=["id", "units", "data"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [lambda x: 2 * x])

        test_mi = pd.MultiIndex.from_tuples([(1, "foo", "value"), (2, "foo", "ts")],
                                            names=["id", "units", "data"])
        test_df = pd.DataFrame([[2, 1], [4, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_apply_conversion_array(self):
        columns = pd.MultiIndex.from_tuples([(1, "bar", "value"), (2, "bar", "ts")],
                                            names=["id", "units", "data"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = apply_conversion(df, ["bar"], ["foo"], [[2, 4]])

        test_mi = pd.MultiIndex.from_tuples([(1, "foo", "value"), (2, "foo", "ts")],
                                            names=["id", "units", "data"])
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
        columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
        df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
        out = convert_units(df, "IP", "W", "J")

        test_mi = pd.MultiIndex.from_tuples([(1, "ft"), (2, "W/m2")], names=["id", "units"])
        test_df = pd.DataFrame([[1 / 0.30479999953, 1], [2 / 0.30479999953, 2]], columns=test_mi)
        assert_frame_equal(out, test_df)

    def test_update_multiindex(self):
        pass

    def test_verify_units(self):
        pass

    def test_get_n_steps(self):
        pass

    def test_rate_to_energy(self):
        pass


if __name__ == "__main__":
    unittest.main()
