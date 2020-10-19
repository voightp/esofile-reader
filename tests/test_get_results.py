from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal
from pytest import lazy_fixture

from esofile_reader import get_results
from esofile_reader.eso_file import PeaksNotIncluded
from esofile_reader.exceptions import InvalidOutputType, InvalidUnitsSystem
from esofile_reader.mini_classes import Variable, SimpleVariable, PathLike
from esofile_reader.results_processing.table_formatter import TableFormatter
from esofile_reader.storages.df_storage import DFStorage
from esofile_reader.storages.pqt_storage import ParquetStorage
from tests.session_fixtures import *


@pytest.fixture(scope="module")
def eso_file(eplusout1):
    return eplusout1


@pytest.fixture(scope="module")
def df_file(eplusout1):
    dfs = DFStorage()
    id_ = dfs.store_file(eplusout1)
    return dfs.files[id_]


@pytest.fixture(scope="module")
def parquet_file(eplusout1):
    pqs = ParquetStorage()
    id_ = pqs.store_file(eplusout1)
    try:
        yield pqs.files[id_]
    finally:
        del pqs


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("eso_file"),
        lazy_fixture("df_file"),
        lazy_fixture("parquet_file"),
        Path(ROOT_PATH, "eso_files", "eplusout1.eso"),
    ],
)
def file(request):
    return request.param


@pytest.fixture(scope="module")
def simple_excel_file():
    pth = Path(ROOT_PATH, "eso_files", "test_excel_results.xlsx")
    sheets = ["simple-template-monthly", "simple-template-range"]
    return ResultsFile.from_excel(pth, sheets)


@pytest.fixture(scope="module")
def simple_df_file(simple_excel_file):
    dfs = DFStorage()
    id_ = dfs.store_file(simple_excel_file)
    return dfs.files[id_]


@pytest.fixture(scope="module")
def simple_parquet_file(simple_excel_file):
    pqs = ParquetStorage()
    id_ = pqs.store_file(simple_excel_file)
    try:
        yield pqs.files[id_]
    finally:
        del pqs


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("simple_excel_file"),
        lazy_fixture("simple_df_file"),
        lazy_fixture("simple_parquet_file"),
        Path(ROOT_PATH, "eso_files", "test_excel_results.xlsx"),
    ],
)
def simple_file(request):
    return request.param


TEST_DF = pd.DataFrame(
    [[22.592079], [24.163740], [25.406725], [26.177191], [25.619201], [23.862254]],
    columns=pd.MultiIndex.from_tuples(
        [("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")], names=["key", "type", "units"]
    ),
    index=pd.MultiIndex.from_product(
        [["eplusout1"], pd.date_range("2002-04-1", freq="MS", periods=6)],
        names=["file", "timestamp"],
    ),
)

TEST_SIMPLE_DF = pd.DataFrame(
    [
        [19.14850348],
        [18.99527211],
        [20.98875615],
        [22.78142137],
        [24.3208488],
        [25.47972495],
        [26.16745932],
        [25.68404781],
        [24.15289436],
        [22.47691717],
        [20.58877632],
        [18.66182101],
    ],
    index=pd.MultiIndex.from_product(
        [["test_excel_results"], pd.date_range(start="2002/01/01", freq="MS", periods=12)],
        names=["file", "timestamp"],
    ),
    columns=pd.MultiIndex.from_tuples([("BLOCK1:ZONE1", "C")], names=["key", "units"]),
)
TEST_SIMPLE_VARIABLE = SimpleVariable("monthly-simple", "BLOCK1:ZONE1", "C")
TEST_VARIABLE = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")


@pytest.mark.parametrize(
    "test_file, variables, start_date, end_date, expected_df",
    [
        (lazy_fixture("file"), TEST_VARIABLE, None, None, TEST_DF),
        (lazy_fixture("file"), TEST_VARIABLE, datetime(2002, 4, 10), None, TEST_DF.iloc[1:, :]),
        (lazy_fixture("file"), TEST_VARIABLE, None, datetime(2002, 8, 10), TEST_DF.iloc[:5, :]),
        (
            lazy_fixture("file"),
            TEST_VARIABLE,
            datetime(2002, 4, 10),
            datetime(2002, 8, 10),
            TEST_DF.iloc[1:5, :],
        ),
        (lazy_fixture("simple_file"), TEST_SIMPLE_VARIABLE, None, None, TEST_SIMPLE_DF),
        (
            lazy_fixture("simple_file"),
            TEST_SIMPLE_VARIABLE,
            datetime(2002, 4, 10),
            None,
            TEST_SIMPLE_DF.iloc[4:, :],
        ),
        (
            lazy_fixture("simple_file"),
            TEST_SIMPLE_VARIABLE,
            None,
            datetime(2002, 8, 10),
            TEST_SIMPLE_DF.iloc[:8, :],
        ),
        (
            lazy_fixture("simple_file"),
            TEST_SIMPLE_VARIABLE,
            datetime(2002, 4, 10),
            datetime(2002, 8, 10),
            TEST_SIMPLE_DF.iloc[4:8, :],
        ),
    ],
)
def test_get_results(test_file, variables, start_date, end_date, expected_df):
    df = get_results(test_file, variables, start_date=start_date, end_date=end_date)
    assert_frame_equal(df, expected_df)


TEST_PEAK_DF = pd.DataFrame(
    [
        [26.177191, datetime(2002, 7, 1)],  # max
        [22.5920785, datetime(2002, 4, 1)],  # min
        [25.4067253, datetime(2002, 6, 1)],  # sliced max
        [24.1637403, datetime(2002, 5, 1)],  # sliced min
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
            ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp"),
        ],
        names=["key", "type", "units", "data"],
    ),
    index=pd.MultiIndex.from_product([["eplusout1"], 4 * [0]], names=["file", None]),
)

TEST_SIMPLE_PEAK_DF = pd.DataFrame(
    [
        [26.16745932, datetime(2002, 7, 1)],  # max
        [18.661821, datetime(2002, 12, 1)],  # min
        [25.479725, datetime(2002, 6, 1)],  # sliced max
        [24.320849, datetime(2002, 5, 1)],  # sliced min
    ],
    columns=pd.MultiIndex.from_tuples(
        [("BLOCK1:ZONE1", "C", "value"), ("BLOCK1:ZONE1", "C", "timestamp"),],
        names=["key", "units", "data"],
    ),
    index=pd.MultiIndex.from_product([["test_excel_results"], 4 * [0]], names=["file", None]),
)


@pytest.mark.parametrize(
    "test_file, variables,peak,  start_date, end_date, expected_df",
    [
        (
            lazy_fixture("file"),
            TEST_VARIABLE,
            "global_max",
            None,
            None,
            TEST_PEAK_DF.iloc[[0], :],
        ),
        (
            lazy_fixture("file"),
            TEST_VARIABLE,
            "global_min",
            None,
            None,
            TEST_PEAK_DF.iloc[[1], :],
        ),
        (
            lazy_fixture("file"),
            TEST_VARIABLE,
            "global_max",
            datetime(2002, 4, 10),
            datetime(2002, 6, 10),
            TEST_PEAK_DF.iloc[[2], :],
        ),
        (
            lazy_fixture("file"),
            TEST_VARIABLE,
            "global_min",
            datetime(2002, 4, 10),
            datetime(2002, 6, 10),
            TEST_PEAK_DF.iloc[[3], :],
        ),
        (
            lazy_fixture("simple_file"),
            TEST_SIMPLE_VARIABLE,
            "global_max",
            None,
            None,
            TEST_SIMPLE_PEAK_DF.iloc[[0], :],
        ),
        (
            lazy_fixture("simple_file"),
            TEST_SIMPLE_VARIABLE,
            "global_min",
            None,
            None,
            TEST_SIMPLE_PEAK_DF.iloc[[1], :],
        ),
        (
            lazy_fixture("simple_file"),
            TEST_SIMPLE_VARIABLE,
            "global_max",
            datetime(2002, 4, 10),
            datetime(2002, 6, 10),
            TEST_SIMPLE_PEAK_DF.iloc[[2], :],
        ),
        (
            lazy_fixture("simple_file"),
            TEST_SIMPLE_VARIABLE,
            "global_min",
            datetime(2002, 4, 10),
            datetime(2002, 6, 10),
            TEST_SIMPLE_PEAK_DF.iloc[[3], :],
        ),
    ],
)
def test_get_results_global_peak(test_file, variables, peak, start_date, end_date, expected_df):
    df = get_results(
        test_file, variables, output_type=peak, start_date=start_date, end_date=end_date
    )
    assert_frame_equal(df, expected_df)


TEST_LOCAL_MAX_DF = pd.DataFrame(
    [
        [30.837382, datetime(2002, 4, 20, 15, 30)],
        [34.835386, datetime(2002, 5, 26, 16, 0)],
        [41.187972, datetime(2002, 6, 30, 15, 30)],
        [38.414505, datetime(2002, 7, 21, 16, 0)],
        [38.694873, datetime(2002, 8, 18, 15, 30)],
        [35.089822, datetime(2002, 9, 15, 15, 0)],
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
            ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp"),
        ],
        names=["key", "type", "units", "data"],
    ),
    index=pd.MultiIndex.from_product(
        [["eplusout2"], pd.date_range("2002-04-1", freq="MS", periods=6)],
        names=["file", "timestamp"],
    ),
)


def test_get_results_output_type_local_max(eplusout2_peaks):
    df = get_results(eplusout2_peaks, TEST_VARIABLE, output_type="local_max")
    assert_frame_equal(df, TEST_LOCAL_MAX_DF)


TEST_LOCAL_MIN_DF = pd.DataFrame(
    [
        [13.681526, datetime(2002, 4, 10, 5, 30)],
        [17.206312, datetime(2002, 5, 7, 5, 30)],
        [19.685125, datetime(2002, 6, 12, 5, 0)],
        [22.279566, datetime(2002, 7, 4, 6, 0)],
        [20.301202, datetime(2002, 8, 31, 6, 0)],
        [16.806496, datetime(2002, 9, 24, 6, 0)],
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
            ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp"),
        ],
        names=["key", "type", "units", "data"],
    ),
    index=pd.MultiIndex.from_product(
        [["eplusout2"], pd.date_range("2002-04-1", freq="MS", periods=6)],
        names=["file", "timestamp"],
    ),
)


def test_get_results_output_type_local_min(eplusout2_peaks):
    df = get_results(eplusout2_peaks, TEST_VARIABLE, output_type="local_min")
    assert_frame_equal(df, TEST_LOCAL_MIN_DF)


def test_get_results_output_type_local_na(file):
    with pytest.raises(PeaksNotIncluded):
        _ = get_results(file, TEST_VARIABLE, output_type="local_min")


def test_get_results_output_type_invalid(file):
    with pytest.raises(InvalidOutputType):
        _ = get_results(file, TEST_VARIABLE, output_type="foo")


TEST_VARIABLES = [
    Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
    Variable("runperiod", "Meter", "Electricity:Facility", "J"),
    Variable("runperiod", "Meter", "InteriorLights:Electricity", "J"),
    Variable("runperiod", "BLOCK1:ZONEB", "Zone Air Relative Humidity", "%"),
]

TEST_SIMPLE_VARIABLES = [
    SimpleVariable("monthly-simple", "BLOCK3:ZONE1", ""),
    SimpleVariable("monthly-simple", "Environment", "W/m2"),
    SimpleVariable("monthly-simple", "BLOCK1:ZONE1", "kgWater/kgDryAir"),
    SimpleVariable("monthly-simple", "BLOCK1:ZONE1", "C"),
]

TEST_SI_DF = pd.DataFrame(
    [
        [22.592079, 26409744634.6392, 9873040320, 42.1419698525608],
        [24.163740, None, None, None],
        [25.406725, None, None, None],
        [26.177191, None, None, None],
        [25.619201, None, None, None],
        [23.862254, None, None, None],
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
            ("Meter", "Electricity:Facility", "J"),
            ("Meter", "InteriorLights:Electricity", "J"),
            ("BLOCK1:ZONEB", "Zone Air Relative Humidity", "%"),
        ],
        names=["key", "type", "units"],
    ),
    index=pd.MultiIndex.from_product(
        [["eplusout1"], pd.date_range("2002-04-01", freq="MS", periods=6)],
        names=["file", "timestamp"],
    ),
)

TEST_SIMPLE_SI_DF = pd.DataFrame(
    [
        [4.44599391, 19.04502688, 0.004855573, 19.14850348],
        [4.280304696, 32.32626488, 0.004860482, 18.99527211],
        [4.059385744, 62.03965054, 0.005461099, 20.98875615],
        [4.394446155, 82.49756944, 0.005840664, 22.78142137],
        [4.44599391, 111.5, 0.007228851, 24.3208488],
        [3.99495105, 123.0475694, 0.007842664, 25.47972495],
        [4.44599391, 120.1125672, 0.009539482, 26.16745932],
        [4.252689827, 97.23555108, 0.009332843, 25.68404781],
        [4.194698603, 72.05486111, 0.007949586, 24.15289436],
        [4.44599391, 41.96303763, 0.007626202, 22.47691717],
        [4.194698603, 28.640625, 0.006508911, 20.58877632],
        [4.252689827, 17.43850806, 0.005512091, 18.66182101],
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            ("BLOCK3:ZONE1", ""),
            ("Environment", "W/m2"),
            ("BLOCK1:ZONE1", "kgWater/kgDryAir"),
            ("BLOCK1:ZONE1", "C"),
        ],
        names=["key", "units"],
    ),
    index=pd.MultiIndex.from_product(
        [["test_excel_results"], pd.date_range(start="2002/01/01", freq="MS", periods=12)],
        names=["file", "timestamp"],
    ),
)


@pytest.mark.parametrize(
    "test_file, variables, expected_df",
    [
        (lazy_fixture("file"), TEST_VARIABLES, TEST_SI_DF),
        (lazy_fixture("simple_file"), TEST_SIMPLE_VARIABLES, TEST_SIMPLE_SI_DF),
    ],
)
def test_get_multiple_results_units_system_si(test_file, variables, expected_df):
    df = get_results(test_file, variables, units_system="SI")
    assert_frame_equal(df, expected_df)


TEST_IP_DF = pd.DataFrame(
    [
        [72.6657414, 26409744634.6392, 9873040320, 42.1419698525608,],
        [75.49473256, None, None, None],
        [77.73210562, None, None, None],
        [79.11894354, None, None, None],
        [78.11456209, None, None, None],
        [74.95205651, None, None, None],
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "F"),
            ("Meter", "Electricity:Facility", "J"),
            ("Meter", "InteriorLights:Electricity", "J"),
            ("BLOCK1:ZONEB", "Zone Air Relative Humidity", "%"),
        ],
        names=["key", "type", "units"],
    ),
    index=pd.MultiIndex.from_product(
        [["eplusout1"], pd.date_range("2002-04-01", freq="MS", periods=6)],
        names=["file", "timestamp"],
    ),
)

TEST_SIMPLE_IP_DF = pd.DataFrame(
    [
        [4.44599391, 1.769984, 0.004855573, 66.467306],
        [4.280304696, 3.004300, 0.004860482, 66.191490],
        [4.059385744, 5.765767, 0.005461099, 69.779761],
        [4.394446155, 7.667060, 0.005840664, 73.006558],
        [4.44599391, 10.362454, 0.007228851, 75.777528],
        [3.99495105, 11.435648, 0.007842664, 77.863505],
        [4.44599391, 11.162878, 0.009539482, 79.101427],
        [4.252689827, 9.036761, 0.009332843, 78.231286],
        [4.194698603, 6.696548, 0.007949586, 75.475210],
        [4.44599391, 3.899911, 0.007626202, 72.458451],
        [4.194698603, 2.661768, 0.006508911, 69.059797],
        [4.252689827, 1.620679, 0.005512091, 65.591278],
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            ("BLOCK3:ZONE1", ""),
            ("Environment", "W/sqf"),
            ("BLOCK1:ZONE1", "kgWater/kgDryAir"),
            ("BLOCK1:ZONE1", "F"),
        ],
        names=["key", "units"],
    ),
    index=pd.MultiIndex.from_product(
        [["test_excel_results"], pd.date_range(start="2002/01/01", freq="MS", periods=12)],
        names=["file", "timestamp"],
    ),
)


@pytest.mark.parametrize(
    "test_file, variables, expected_df",
    [
        (lazy_fixture("file"), TEST_VARIABLES, TEST_IP_DF),
        (lazy_fixture("simple_file"), TEST_SIMPLE_VARIABLES, TEST_SIMPLE_IP_DF),
    ],
)
def test_get_multiple_results_units_system_ip(test_file, variables, expected_df):
    df = get_results(test_file, variables, units_system="IP")
    assert_frame_equal(df, expected_df)


def test_get_results_units_system_invalid(simple_file):
    with pytest.raises(InvalidUnitsSystem):
        _ = get_results(simple_file, TEST_SIMPLE_VARIABLE, units_system="FOO")


RATE_VARIABLES = [
    Variable("monthly", "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
    Variable("runperiod", "BLOCK1:ZONEB", "Zone People Sensible Heating Rate", "W"),
]
SIMPLE_RATE_VARIABLES = [SimpleVariable("monthly-simple", "Environment", "W/m2")]


@pytest.mark.parametrize(
    "test_file, variables, expected_df",
    [
        (
            lazy_fixture("file"),
            RATE_VARIABLES,
            pd.DataFrame(
                [
                    [213833700.00000003, 1415172751.050007],
                    [298641600.0, None],
                    [318939300.0, None],
                    [321709500.0, None],
                    [260435699.99999997, None],
                    [186766200.0, None],
                ],
                columns=pd.MultiIndex.from_tuples(
                    [
                        ("Environment", "Site Diffuse Solar Radiation Rate per Area", "J/m2"),
                        ("BLOCK1:ZONEB", "Zone People Sensible Heating Rate", "J"),
                    ],
                    names=["key", "type", "units"],
                ),
                index=pd.MultiIndex.from_product(
                    [["eplusout1"], pd.date_range("2002-04-01", freq="MS", periods=6)],
                    names=["file", "timestamp"],
                ),
            ),
        ),
        (
            lazy_fixture("simple_file"),
            SIMPLE_RATE_VARIABLES,
            pd.DataFrame(
                [
                    [51010200.0],
                    [78203700.0],
                    [166167000.0],
                    [213833700.0],
                    [298641600.0],
                    [318939300.0],
                    [321709500.0],
                    [260435700.0],
                    [186766200.0],
                    [112393800.0],
                    [74236500.0],
                    [46707300.0],
                ],
                columns=pd.MultiIndex.from_tuples(
                    [("Environment", "J/m2"),], names=["key", "units"],
                ),
                index=pd.MultiIndex.from_product(
                    [
                        ["test_excel_results"],
                        pd.date_range(start="2002/01/01", freq="MS", periods=12),
                    ],
                    names=["file", "timestamp"],
                ),
            ),
        ),
    ],
)
def test_get_results_rate_to_energy(test_file, variables, expected_df):
    df = get_results(test_file, variables, rate_to_energy=True)
    assert_frame_equal(df, expected_df)


@pytest.mark.parametrize(
    "test_file, variables, expected_df",
    [
        (
            lazy_fixture("file"),
            RATE_VARIABLES,
            pd.DataFrame(
                [
                    [0.08249756944444445, 0.08950444944406542],
                    [0.1115, None],
                    [0.12304756944444445, None],
                    [0.12011256720430107, None],
                    [0.09723555107526882, None],
                    [0.0720548611111111, None],
                ],
                columns=pd.MultiIndex.from_tuples(
                    [
                        ("Environment", "Site Diffuse Solar Radiation Rate per Area", "kW/m2"),
                        ("BLOCK1:ZONEB", "Zone People Sensible Heating Rate", "kW"),
                    ],
                    names=["key", "type", "units"],
                ),
                index=pd.MultiIndex.from_product(
                    [["eplusout1"], pd.date_range("2002-04-01", freq="MS", periods=6)],
                    names=["file", "timestamp"],
                ),
            ),
        ),
        (
            lazy_fixture("simple_file"),
            SIMPLE_RATE_VARIABLES,
            pd.DataFrame(
                [
                    [0.01904502688172043],
                    [0.03232626488095238],
                    [0.06203965053763441],
                    [0.08249756944444445],
                    [0.1115],
                    [0.12304756944444441],
                    [0.12011256720430111],
                    [0.09723555107526882],
                    [0.0720548611111111],
                    [0.041963037634408604],
                    [0.028640625],
                    [0.01743850806451613],
                ],
                columns=pd.MultiIndex.from_tuples(
                    [("Environment", "kW/m2"),], names=["key", "units"],
                ),
                index=pd.MultiIndex.from_product(
                    [
                        ["test_excel_results"],
                        pd.date_range(start="2002/01/01", freq="MS", periods=12),
                    ],
                    names=["file", "timestamp"],
                ),
            ),
        ),
    ],
)
def test_get_results_rate(test_file, variables, expected_df):
    df = get_results(test_file, variables, rate_to_energy=False, rate_units="kW")
    assert_frame_equal(df, expected_df)


def test_get_results_ignore_peaks():
    ef = EsoFile(Path(ROOT_PATH, "eso_files", "eplusout1.eso"), ignore_peaks=False)
    assert list(ef.peak_tables.keys()) == ["local_min", "local_max"]


def test_multiple_files_invalid_variables(file, eplusout2):
    files = [file, eplusout2]
    assert get_results(files, [Variable(None, "foo", "bar", "baz")] * 3) is None


def test_get_results_multiple_files(file, eplusout2):
    files = [file, eplusout2]
    v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
    table_formatter = TableFormatter(file_name_position="column")
    df = get_results(files, v, table_formatter=table_formatter)
    test_df = pd.DataFrame(
        [
            [22.592079, 23.448357],
            [24.163740, 24.107510],
            [25.406725, 24.260228],
            [26.177191, 24.458445],
            [25.619201, 24.378681],
            [23.862254, 24.010489],
        ],
        columns=pd.MultiIndex.from_tuples(
            [
                ("eplusout1", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
                ("eplusout2", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
            ],
            names=["file", "key", "type", "units"],
        ),
        index=pd.DatetimeIndex(
            pd.date_range("2002-04-01", freq="MS", periods=6), name="timestamp"
        ),
    )

    assert_frame_equal(df, test_df, check_freq=False)
