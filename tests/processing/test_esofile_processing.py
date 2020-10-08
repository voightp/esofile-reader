import datetime
from io import StringIO
from math import nan

import numpy as np

from esofile_reader.constants import *
from esofile_reader.exceptions import InvalidLineSyntax, BlankLineError, IncompleteFile
from esofile_reader.mini_classes import Variable, IntervalTuple
from esofile_reader.processing.esofile_intervals import process_raw_date_data
from esofile_reader.processing.extensions.esofile import (
    process_statement_line,
    process_header_line,
    read_header,
    process_sub_monthly_interval_line,
    process_monthly_plus_interval_line,
    read_body,
)
from esofile_reader.processing.extensions.raw_tables import (
    generate_df_tables,
    generate_peak_tables,
    remove_duplicates,
)
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from tests.session_fixtures import *

HEADER_PATH = Path(ROOT_PATH, "eso_files", "header.txt")
BODY_PATH = Path(ROOT_PATH, "eso_files", "body.txt")


@pytest.fixture(scope="module")
def header_content():
    with open(HEADER_PATH, "r") as f:
        return read_header(f, EsoFileProgressLogger("foo"))


@pytest.fixture(scope="function")
def all_raw_outputs(header_content):
    with open(BODY_PATH, "r") as f:
        return read_body(f, 6, header_content, False, EsoFileProgressLogger("dummy"))


@pytest.fixture(scope="function")
def raw_outputs(all_raw_outputs):
    return all_raw_outputs[0]


def test_esofile_statement():
    line = "Program Version,EnergyPlus, " "Version 9.1.0-08d2e308bb, YMD=2019.07.23 15:19"
    version, timestamp = process_statement_line(line)
    assert version == 910
    assert timestamp == datetime.datetime(2019, 7, 23, 15, 19, 00)


@pytest.mark.parametrize(
    "line,line_tuple",
    [
        (
            "8,7,Environment,Air Temperature [C] !Daily [Value,Min,Hour,Minute,Max,Hour,Minute]",
            (8, "Environment", "Air Temperature", "C", "daily"),
        ),
        (
            "302,1,InteriorEquipment:Electricity [J] !Hourly",
            (302, "Meter", "InteriorEquipment:Electricity", "J", "hourly"),
        ),
    ],
)
def test_header_line(line, line_tuple):
    assert line_tuple == process_header_line(line)


def test_header_line_invalid_line():
    line = "302,1,InteriorEquipment,Electricity,[J], !Hourly"
    with pytest.raises(AttributeError):
        process_header_line(line)


def test_read_header():
    file = StringIO(
        "7,1,Environment,Air Temperature [C] !Hourly\n"
        "3676,11,Some meter [ach] !RunPeriod [Value,Min,Month,Day,Hour,Minute,Max,Month,Day,Hour,Minute]\n"
        "End of Data Dictionary\n"
    )
    header_dct = read_header(file, EsoFileProgressLogger("foo"))
    test_header = {
        "hourly": {7: Variable("hourly", "Environment", "Air Temperature", "C")},
        "runperiod": {3676: Variable("runperiod", "Meter", "Some meter", "ach")},
    }

    assert header_dct == test_header


def test_read_header_blank_line():
    s = (
        "7,1,Environment,Site Outdoor Air Drybulb Temperature [C] !Hourly\n\n"
        "End of Data Dictionary\n"
    )
    file = StringIO(s)

    with pytest.raises(BlankLineError):
        read_header(file, EsoFileProgressLogger("foo"))


@pytest.mark.parametrize(
    "interval,id_,variable",
    [
        (
            "timestep",
            37,
            Variable(
                "timestep", "Environment", "Site Direct Solar Radiation Rate per Area", "W/m2"
            ),
        ),
        (
            "hourly",
            163,
            Variable("hourly", "BLOCK1:ZONE1", "Zone Mean Radiant Temperature", "C"),
        ),
        ("daily", 24, Variable("daily", "Environment", "Site Wind Speed", "m/s")),
        ("monthly", 45, Variable("monthly", "Environment", "Site Solar Azimuth Angle", "deg")),
        ("runperiod", 31, Variable("runperiod", "Environment", "Site Wind Direction", "deg")),
        ("runperiod", 562, Variable("runperiod", "Meter", "DistrictCooling:Facility", "J")),
    ],
)
def test_read_header_from_file(header_content, interval, id_, variable):
    assert header_content[interval][id_] == variable


@pytest.mark.parametrize(
    "line_id,line,processed_line",
    [
        (
            2,
            [" 1", " 2", " 3", " 0", "10.00", "0.00", "60.00", "Saturday"],
            (H, IntervalTuple(2, 3, 10, 60), "Saturday",),
        ),
        (
            2,
            [" 1", " 2", " 3", " 0", "10.00", "0.00", "30.00", "Saturday"],
            (TS, IntervalTuple(2, 3, 10, 30), "Saturday",),
        ),
        (3, [" 20", " 1", " 2", " 0", "Saturday"], (D, IntervalTuple(1, 2, 0, 0), "Saturday")),
    ],
)
def test_process_sub_monthly_interval_line(line_id, line, processed_line):
    assert process_sub_monthly_interval_line(line_id, line) == processed_line


@pytest.mark.parametrize(
    "line_id,line,processed_line",
    [
        (4, [" 58", " 1"], (M, IntervalTuple(1, 1, 0, 0), 58)),
        (5, ["365"], (RP, IntervalTuple(1, 1, 0, 0), 365)),
        (6, ["1"], (A, IntervalTuple(1, 1, 0, 0), None)),
    ],
)
def test_process_monthly_plus_interval_line(line_id, line, processed_line):
    assert process_monthly_plus_interval_line(line_id, line) == processed_line


def test_read_body_env_names(raw_outputs):
    assert raw_outputs.environment_name == "UNTITLED (3O-O6:O1-O7)"


# fmt: off
@pytest.mark.parametrize(
    "interval,id_,values",
    [
        ("timestep", 7, [
            15.65, 14.3, 14.15, 14.0, 12.8, 11.6, 10.899999999999999, 10.2, 11.05, 11.9,
            13.0, 14.1, 15.05, 16.0, 17.35, 18.7, 20.4, 22.1, 23.75, 25.4, 26.4, 27.4,
            28.0, 28.6, 29.1, 29.6, 30.200000000000004, 30.8, 31.05, 31.3, 30.75, 30.2,
            29.6, 29.0, 28.7, 28.4, 27.45, 26.5, 25.7, 24.9, 24.049999999999998, 23.2,
            22.25, 21.3, 20.25, 19.2, 18.1, 17.0, 16.05, 15.1, 14.3, 13.5, 12.95, 12.4,
            12.100000000000002, 11.8, 11.600000000000002, 11.4, 11.15, 10.9, 11.45,
            12.0, 11.9, 11.8, 12.5, 13.2, 13.75, 14.3, 14.75, 15.2, 15.35, 15.5, 16.55,
            17.6, 17.200000000000004, 16.8, 16.5, 16.2, 15.5, 14.8, 15.6, 16.4,
            16.299999999999998, 16.2, 15.7, 15.2, 14.45, 13.7, 13.05, 12.4, 11.9, 11.4,
            10.850000000000002, 10.3, 10.2, 10.1, ]),
        ("hourly", 8, [
            14.975000000000002, 14.075, 12.2, 10.549999999999999, 11.475000000000002,
            13.55, 15.525, 18.025, 21.25, 24.575, 26.9, 28.3, 29.35, 30.5, 31.175,
            30.475, 29.3, 28.549999999999998, 26.975, 25.299999999999998, 23.625,
            21.775, 19.725, 17.55, 15.575, 13.9, 12.675, 11.950000000000001, 11.5,
            11.025, 11.725, 11.850000000000002, 12.85, 14.025, 14.975, 15.425,
            17.075000000000004, 17.0, 16.35, 15.15, 16.0, 16.25, 15.45, 14.075,
            12.725000000000002, 11.65, 10.575000000000001, 10.149999999999999, ]),
        ("hourly", 163, [
            nan, nan, nan, nan, nan, nan, nan, nan, nan,
            nan, nan, nan, nan, nan, nan, nan, nan, nan,
            nan, nan, nan, nan, nan, nan, nan, nan, nan,
            nan, nan, nan, 31.939895115385128, 31.703936337515786,
            32.280660803461618, 32.62177706428757, 32.88418951192571,
            33.009496155093547, 33.03911553829569, 32.92907267649866,
            32.65682359572439, 32.31898695867979, 32.197143544621329,
            31.872368037056775, nan, nan, nan, nan, nan, nan, ]),
        ("daily", 14, [12.883333333333335, 10.19895789513146]),
        ("monthly", 15, [12.883333333333335, 10.19895789513146]),
        ("runperiod", 16, [11.541145614232397])
    ]
)
def test_read_body_raw_outputs(raw_outputs, interval, id_, values):
    assert raw_outputs.outputs[interval][id_] == values


# fmt: on


@pytest.mark.parametrize(
    "interval,id_,values",
    [
        ("daily", 9, [[10.2, 4, 60, 31.3, 15, 60], [10.1, 24, 60, 17.6, 13, 60]]),
        ("daily", 622, [[0.0, 1, 15, 0.0, 1, 15], [0.0, 1, 15, 1.1844716168217186, 10, 60]]),
        (
            "monthly",
            10,
            [[10.2, 30, 4, 60, 31.3, 30, 15, 60], [10.1, 1, 24, 60, 17.6, 1, 13, 60]],
        ),
        ("runperiod", 11, [[10.1, 7, 1, 24, 60, 31.3, 6, 30, 15, 60]]),
    ],
)
def test_read_body_raw_peak_outputs(raw_outputs, interval, id_, values):
    assert raw_outputs.peak_outputs[interval][id_] == values


@pytest.mark.parametrize(
    "interval,index,values",
    [
        ("timestep", 0, IntervalTuple(month=6, day=30, hour=1, end_minute=30)),
        ("timestep", -1, IntervalTuple(month=7, day=1, hour=24, end_minute=60)),
        ("hourly", 0, IntervalTuple(month=6, day=30, hour=1, end_minute=60)),
        ("hourly", -1, IntervalTuple(month=7, day=1, hour=24, end_minute=60)),
        ("daily", 0, IntervalTuple(month=6, day=30, hour=0, end_minute=0)),
        ("daily", -1, IntervalTuple(month=7, day=1, hour=0, end_minute=0)),
        ("monthly", 0, IntervalTuple(month=6, day=1, hour=0, end_minute=0)),
        ("monthly", 0, IntervalTuple(month=6, day=1, hour=0, end_minute=0)),
        ("runperiod", -1, IntervalTuple(month=1, day=1, hour=0, end_minute=0)),
    ],
)
def test_read_body_raw_dates(raw_outputs, interval, index, values):
    assert raw_outputs.dates[interval][index] == values


@pytest.mark.parametrize("interval,values", [("monthly", [1, 2]), ("runperiod", [2]),])
def test_read_body_cumulative_days(raw_outputs, interval, values):
    assert raw_outputs.cumulative_days[interval] == values


@pytest.mark.parametrize(
    "interval,values",
    [
        ("timestep", ["Sunday"] * 48 + ["Monday"] * 48),
        ("hourly", ["Sunday"] * 24 + ["Monday"] * 24),
        ("daily", ["Sunday", "Monday"]),
    ],
)
def test_read_body_day_of_week(raw_outputs, interval, values):
    assert raw_outputs.days_of_week[interval] == values


@pytest.mark.parametrize(
    "peak,interval,shape",
    [
        ("local_min", "daily", (2, 42)),
        ("local_min", "monthly", (2, 42)),
        ("local_min", "runperiod", (1, 42)),
        ("local_max", "daily", (2, 42)),
        ("local_max", "monthly", (2, 42)),
        ("local_max", "runperiod", (1, 42)),
    ],
)
def test_generate_peak_tables(raw_outputs, peak, interval, shape):
    dates = raw_outputs.dates
    cumulative_days = raw_outputs.cumulative_days
    header = raw_outputs.header
    raw_peak_outputs = raw_outputs.peak_outputs
    logger = EsoFileProgressLogger("foo")
    dates, n_days = process_raw_date_data(dates, cumulative_days, 2002)
    outputs = generate_peak_tables(raw_peak_outputs, header, dates, logger)
    assert outputs[peak][interval].shape == shape


@pytest.mark.parametrize(
    "interval,shape",
    [
        ("daily", (2, 21)),
        ("monthly", (2, 21)),
        ("runperiod", (1, 21)),
        ("daily", (2, 21)),
        ("monthly", (2, 21)),
        ("runperiod", (1, 21)),
    ],
)
def test_generate_df_tables(raw_outputs, interval, shape):
    dates = raw_outputs.dates
    cumulative_days = raw_outputs.cumulative_days
    header = raw_outputs.header
    outputs = raw_outputs.outputs
    logger = EsoFileProgressLogger("foo")
    dates, n_days = process_raw_date_data(dates, cumulative_days, 2002)
    outputs = generate_df_tables(outputs, header, dates, logger)
    assert outputs[interval].shape == shape


@pytest.mark.parametrize("interval", [TS, H, D])
def test_df_tables_day_type(eplusout_all_intervals, interval):
    col = ("special", interval, "day", "", "")
    assert eplusout_all_intervals.tables[interval].loc[:, col].dtype == np.dtype("object")


@pytest.mark.parametrize("interval", [M, A, RP])
def test_df_tables_n_days_type(eplusout_all_intervals, interval):
    col = ("special", interval, "n days", "", "")
    assert eplusout_all_intervals.tables[interval].loc[:, col].dtype == np.dtype("int64")


@pytest.mark.parametrize("interval", [TS, H, D, M, A, RP])
def test_df_tables_numeric_type(eplusout_all_intervals, interval):
    dtypes = eplusout_all_intervals.tables.get_numeric_table(interval).dtypes
    assert set(dtypes) == {np.dtype("float64")}


def test_remove_duplicates():
    v1 = Variable("hourly", "a", "b", "c")
    v2 = Variable("hourly", "d", "e", "f")
    v3 = Variable("hourly", "g", "h", "i")
    ids = {1: v1, 2: v2}
    header_dct = {"hourly": {1: v1, 2: v2, 3: v3}}
    outputs_dct = {"hourly": {1: v1, 3: v3}}
    peak_outpus = None

    remove_duplicates(ids, header_dct, outputs_dct, peak_outpus)

    assert header_dct["hourly"] == {3: v3}
    assert outputs_dct["hourly"] == {3: v3}


def test_header_invalid_line():
    f = StringIO("this is wrong!")
    with pytest.raises(AttributeError):
        read_header(f, EsoFileProgressLogger("foo"))


def test_body_invalid_line():
    f = StringIO("this is wrong!")
    with pytest.raises(InvalidLineSyntax):
        read_body(f, 6, {"a": {}}, False, EsoFileProgressLogger("foo"))


def test_body_blank_line(header_content):
    f = StringIO(
        """1,UNTITLED (3O-O6:O1-O7),  51.15,  -0.18,   0.00,  62.00
2,1, 6,30, 1, 1, 0.00,30.00,Sunday
7,15.65
12,12.45
17,100600.0
22,1.8
27,45.0

32,0.0
37,0.0
42,6.20102865211224
47,-15.405960920221065
177,0.7500926860763895
620,0.0"""
    )
    with pytest.raises(BlankLineError):
        read_body(f, 6, header_content, False, EsoFileProgressLogger("foo"))


def test_file_blank_line():
    with pytest.raises(IncompleteFile):
        EsoFile(
            Path(ROOT_PATH, "eso_files", "eplusout_incomplete.eso"),
            EsoFileProgressLogger("foo"),
        )


def test_non_numeric_line():
    with pytest.raises(InvalidLineSyntax):
        EsoFile(
            Path(ROOT_PATH, "eso_files", "eplusout_invalid_line.eso"),
            EsoFileProgressLogger("foo"),
        )


def test_logging_level_info():
    EsoFile(
        Path(ROOT_PATH, "eso_files", "eplusout1.eso"),
        progress_logger=EsoFileProgressLogger("foo", level=20),
    )
