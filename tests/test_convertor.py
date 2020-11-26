import pytest
from pandas.testing import assert_frame_equal

from esofile_reader.convertor import *
from esofile_reader.df.level_names import TIMESTAMP_COLUMN, COLUMN_LEVELS
from esofile_reader.processing.eplus import H, M


def test_apply_conversion():
    columns = pd.MultiIndex.from_tuples([(1, "bar"), (2, "baz")], names=["id", "units"])
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = apply_conversion(df, [("bar", "foo", 2)])

    test_mi = pd.MultiIndex.from_tuples([(1, "foo"), (2, "baz")], names=["id", "units"])
    test_df = pd.DataFrame([[0.5, 1], [1, 2]], columns=test_mi)
    assert_frame_equal(out, test_df)


def test_apply_conversion_peak():
    columns = pd.MultiIndex.from_tuples(
        [(1, "bar", "value"), (2, "bar", "ts")], names=["id", "units", "data"]
    )
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = apply_conversion(df, [("bar", "foo", 2)])

    test_mi = pd.MultiIndex.from_tuples(
        [(1, "foo", "value"), (2, "foo", "ts")], names=["id", "units", "data"]
    )
    test_df = pd.DataFrame([[0.5, 1], [1, 2]], columns=test_mi)
    assert_frame_equal(out, test_df)


def test_apply_conversion_callable():
    columns = pd.MultiIndex.from_tuples(
        [(1, "bar", "value"), (2, "bar", "ts")], names=["id", "units", "data"]
    )
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = apply_conversion(df, [("bar", "foo", lambda x: 2 * x)])

    test_mi = pd.MultiIndex.from_tuples(
        [(1, "foo", "value"), (2, "foo", "ts")], names=["id", "units", "data"]
    )
    test_df = pd.DataFrame([[2, 1], [4, 2]], columns=test_mi)
    assert_frame_equal(out, test_df)


def test_apply_conversion_array():
    columns = pd.MultiIndex.from_tuples(
        [(1, "bar", "value"), (2, "bar", "ts")], names=["id", "units", "data"]
    )
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = apply_conversion(df, [("bar", "foo", [2, 4])])

    test_mi = pd.MultiIndex.from_tuples(
        [(1, "foo", "value"), (2, "foo", "ts")], names=["id", "units", "data"]
    )
    test_df = pd.DataFrame([[0.5, 1], [0.5, 2]], columns=test_mi)
    assert_frame_equal(out, test_df)


def test_convert_units_energy():
    columns = pd.MultiIndex.from_tuples([(1, "J"), (2, "W")], names=["id", "units"])
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = convert_units(df, "SI", "W", "kWh")

    test_mi = pd.MultiIndex.from_tuples([(1, "kWh"), (2, "W")], names=["id", "units"])
    test_df = pd.DataFrame([[1 / 3600000, 1], [2 / 3600000, 2]], columns=test_mi)
    assert_frame_equal(out, test_df)


def test_convert_units_energy_per_area():
    columns = pd.MultiIndex.from_tuples([(1, "J/m2"), (2, "W")], names=["id", "units"])
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = convert_units(df, "SI", "W", "kWh")

    test_mi = pd.MultiIndex.from_tuples([(1, "kWh/m2"), (2, "W")], names=["id", "units"])
    test_df = pd.DataFrame([[1 / 3600000, 1], [2 / 3600000, 2]], columns=test_mi)
    assert_frame_equal(out, test_df)


def test_convert_units_power():
    columns = pd.MultiIndex.from_tuples([(1, "J"), (2, "W")], names=["id", "units"])
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = convert_units(df, "SI", "kW", "J")

    test_mi = pd.MultiIndex.from_tuples([(1, "J"), (2, "kW")], names=["id", "units"])
    test_df = pd.DataFrame([[1, 1 / 1000], [2, 2 / 1000]], columns=test_mi)
    assert_frame_equal(out, test_df)


def test_convert_units_power_per_area():
    columns = pd.MultiIndex.from_tuples([(1, "J/m2"), (2, "W/m2")], names=["id", "units"])
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = convert_units(df, "SI", "kW", "J")

    test_mi = pd.MultiIndex.from_tuples([(1, "J/m2"), (2, "kW/m2")], names=["id", "units"])
    test_df = pd.DataFrame([[1, 1 / 1000], [2, 2 / 1000]], columns=test_mi)
    assert_frame_equal(out, test_df)


def test_convert_units_si_to_ip():
    columns = pd.MultiIndex.from_tuples(
        [(1, "m"), (2, "W/m2"), (3, "deltaC")], names=["id", "units"]
    )
    df = pd.DataFrame([[1, 1, 1], [2, 2, 2]], columns=columns)
    out = convert_units(df, "IP", "W", "J")

    test_mi = pd.MultiIndex.from_tuples(
        [(1, "ft"), (2, "W/sqf"), (3, "deltaF")], names=["id", "units"]
    )
    test_df = pd.DataFrame(
        [[1 / 0.3048, 1 / 10.76, 1.8], [2 / 0.3048, 2 / 10.76, 3.6]], columns=test_mi
    )
    assert_frame_equal(out, test_df)


def test_convert_units_si_to_ip_w_sqf_vs_Btuh():
    columns = pd.MultiIndex.from_tuples(
        [(1, "m"), (2, "W/m2"), (3, "deltaC")], names=["id", "units"]
    )
    df = pd.DataFrame([[1, 1, 1], [2, 2, 2]], columns=columns)
    out = convert_units(df, "IP", "Btu/h", "J")

    test_mi = pd.MultiIndex.from_tuples(
        [(1, "ft"), (2, "Btu/h-ft2"), (3, "deltaF")], names=["id", "units"]
    )
    test_df = pd.DataFrame(
        [
            [1 / 0.3048, 1 / (0.2930711 * 10.76391), 1.8],
            [2 / 0.3048, 2 / (0.2930711 * 10.76391), 3.6],
        ],
        columns=test_mi,
    )
    assert_frame_equal(out, test_df)


def test_convert_units_no_valid():
    columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
    df = pd.DataFrame([[1, 1], [2, 2]], columns=columns)
    out = convert_units(df, "SI", "W", "J")

    assert_frame_equal(df, out)


def test_update_multiindex_axis0():
    columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
    index = pd.Index(["a", "b"], name="dt")
    df = pd.DataFrame([[1, 1], [2, 2]], index=index, columns=columns)

    update_multiindex(df, "dt", ["a"], ["c"], axis=0)

    assert df.index.tolist() == [("c",), ("b",)]


def test_update_multiindex_axis1():
    columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
    index = pd.Index(["a", "b"], name="dt")
    df = pd.DataFrame([[1, 1], [2, 2]], index=index, columns=columns)
    update_multiindex(df, "units", ["m"], ["ft"], axis=1)
    assert df.columns.tolist() == [(1, "ft"), (2, "W/m2")]


def test_rate_and_energy_units():
    assert all_rate_or_energy(["W", "J", "J"])


def test_rate_and_energy_units_per_area():
    assert all_rate_or_energy(["W/m2", "J/m2", "J/m2"])


def test_rate_and_energy_units_invalid():
    assert not all_rate_or_energy(["W", "J", "J/m2"])


def test_rate_and_energy_units_per_area_invalid():
    assert not all_rate_or_energy(["W/m2", "JW/m2", "J"])


def test_get_n_steps_hourly():
    dt_index = pd.date_range("01/01/2002 01:00", freq="h", periods=5)
    steps = get_n_steps(dt_index)
    assert steps == 1.0


def test_get_n_steps_subhourly():
    dt_index = pd.date_range("01/01/2002 01:00", freq="30min", periods=5)
    steps = get_n_steps(dt_index)
    assert steps == 2.0


def test_rate_to_energy_hourly():
    columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
    index = pd.Index(
        pd.date_range("01/01/2002 01:00", freq="h", periods=3), name=TIMESTAMP_COLUMN
    )
    df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)
    df = convert_rate_to_energy(df, H)

    test_columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "J/m2")], names=["id", "units"])
    test_index = pd.Index(
        pd.date_range("01/01/2002 01:00", freq="h", periods=3), name=TIMESTAMP_COLUMN
    )
    test_df = pd.DataFrame(
        [[1, 1 * 3600], [2, None], [3, 3 * 3600]], index=test_index, columns=test_columns
    )
    assert_frame_equal(df, test_df)


def test_rate_to_energy_daily():
    columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
    index = pd.Index(
        pd.date_range("01/01/2002 01:00", freq="d", periods=3), name=TIMESTAMP_COLUMN
    )
    df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)
    df = convert_rate_to_energy(df)

    test_columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "J/m2")], names=["id", "units"])
    test_index = pd.Index(
        pd.date_range("01/01/2002 01:00", freq="d", periods=3), name=TIMESTAMP_COLUMN
    )
    test_df = pd.DataFrame(
        [[1, 1 * 3600 * 24], [2, None], [3, 3 * 3600 * 24]],
        index=test_index,
        columns=test_columns,
    )
    assert_frame_equal(df, test_df)


def test_rate_to_energy_n_days():
    columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "W/m2")], names=["id", "units"])
    index = pd.Index(
        pd.date_range("01/01/2002 01:00", freq="30d", periods=3), name=TIMESTAMP_COLUMN
    )
    df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)
    nd_df = pd.DataFrame({"n_days": [30, 30, 31]}, index=index)

    df = convert_rate_to_energy(df, nd_df["n_days"])

    test_columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "J/m2")], names=["id", "units"])
    test_index = pd.Index(
        pd.date_range("01/01/2002 01:00", freq="30d", periods=3), name=TIMESTAMP_COLUMN
    )
    test_df = pd.DataFrame(
        [[1, 1 * 3600 * 24 * 30], [2, None], [3, 3 * 3600 * 24 * 31]],
        index=test_index,
        columns=test_columns,
    )
    assert_frame_equal(df, test_df)


def test_rate_to_energy_na():
    columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "FOO/m2")], names=["id", "units"])
    index = pd.Index(
        pd.date_range("01/01/2002 01:00", freq="30d", periods=3), name=TIMESTAMP_COLUMN
    )
    df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)
    nd_df = pd.DataFrame({"n_days": [30, 30, 31]}, index=index)

    df = convert_rate_to_energy(df, nd_df["n_days"])
    assert_frame_equal(df, df)


def test_rate_to_energy_missing_ndays():
    columns = pd.MultiIndex.from_tuples([(1, "m"), (2, "FOO/m2")], names=["id", "units"])
    index = pd.Index(
        pd.date_range("01/01/2002 01:00", freq="30d", periods=3), name=TIMESTAMP_COLUMN
    )
    df = pd.DataFrame([[1, 1], [2, None], [3, 3]], index=index, columns=columns)
    with pytest.raises(TypeError):
        _ = convert_rate_to_energy(df, M)


@pytest.mark.parametrize(
    "table", [rate_table, rate_table_per_area, energy_table, energy_table_per_area]
)
def test_energy_rate_units_invalid(table):
    with pytest.raises(KeyError):
        _ = table("FOO")


def test_si_units_invalid():
    try:
        si_to_ip("FOO")
    except KeyError:
        pytest.fail("si_to_ip should pass KeyError silently!")


MI = pd.MultiIndex.from_tuples([("special", "foo", "abc", "bar", "baz")], names=COLUMN_LEVELS)
MI_EMPTY = pd.MultiIndex.from_tuples([], names=COLUMN_LEVELS)
MI_N_DAYS = pd.MultiIndex.from_tuples(
    [("special", "foo", N_DAYS_COLUMN, "bar", "baz")], names=COLUMN_LEVELS
)


@pytest.mark.parametrize(
    "df, can_convert",
    [
        (pd.DataFrame([], columns=MI_EMPTY), False),
        (
            pd.DataFrame(
                np.ndarray([3, 1]),
                index=pd.date_range("2002/01/01", freq="D", periods=3),
                columns=MI,
            ),
            True,
        ),
        (
            pd.DataFrame(
                np.ndarray([3, 1]),
                index=pd.date_range("2002/01/01", freq="MS", periods=3),
                columns=MI,
            ),
            False,
        ),
        (
            pd.DataFrame(
                np.ndarray([3, 1]),
                index=pd.date_range("2002/01/01", freq="MS", periods=3),
                columns=MI_N_DAYS,
            ),
            True,
        ),
        (pd.DataFrame(np.ndarray([3, 1]), index=pd.RangeIndex(3), columns=MI_N_DAYS), True),
        (pd.DataFrame(np.ndarray([3, 1]), index=pd.RangeIndex(3), columns=MI), False),
    ],
)
def test_can_convert_rate_to_energy(df, can_convert):
    assert can_convert == can_convert_rate_to_energy(df)
