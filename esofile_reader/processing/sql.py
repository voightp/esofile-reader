from pathlib import Path

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine

from esofile_reader.processing.sql_time import (
    get_timestamps,
    get_env_interval_pairs,
    get_days_of_week,
    get_intervals,
    parse_eplus_timestamps,
    create_time_table,
)
from esofile_reader.processing.sql_variables import (
    create_report_data_table,
    create_data_dictionary_table,
    get_reporting_frequencies,
    get_header_data,
    parse_sql_header,
)

TEST_FILE_PATH = Path(Path(__file__).parents[2], "tests", "eso_files", "eplusout_leap_year.sql")


def set_up_db(path: str, echo=True) -> Engine:
    engine = create_engine(f"sqlite:///{path}", echo=echo)
    metadata = MetaData(bind=engine)

    # reflect database object, this could be done using 'autoload=True' but
    # better to define tables explicitly to have a nice reference
    time_table = create_time_table(metadata)
    data_dict_table = create_data_dictionary_table(metadata)
    data_table = create_report_data_table(metadata)

    time_series = {}
    interval_types = []

    with engine.connect() as conn:
        # get environment time data
        pairs = get_env_interval_pairs(conn, time_table)
        for env_index, interval_type in pairs:
            # interval data
            eplus_timestamps = get_timestamps(conn, time_table, env_index, interval_type)
            datetime_timestamps = parse_eplus_timestamps(eplus_timestamps)
            if interval_type <= 2:
                days_of_week = get_days_of_week(conn, time_table, env_index, interval_type)
            else:
                minute_intervals = get_intervals(conn, time_table, env_index, interval_type)

        # header data
        reporting_frequencies = get_reporting_frequencies(conn, data_dict_table)
        for frequency in reporting_frequencies:
            sql_header = get_header_data(conn, data_dict_table, frequency)
            header = parse_sql_header(sql_header)

    return engine


set_up_db(TEST_FILE_PATH, False)
