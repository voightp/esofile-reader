from copy import deepcopy
from typing import Dict, List

from sqlalchemy import MetaData, create_engine, Table
from sqlalchemy.engine.base import Connection, Engine

from esofile_reader.mini_classes import Variable, PathLike
from esofile_reader.processing.progress_logger import GenericLogger
from esofile_reader.processing.raw_data import RawSqlData
from esofile_reader.processing.sql_time import (
    get_dates,
    get_days_of_week,
    get_intervals,
    create_time_table,
    create_environment_periods_table,
    get_interval_types,
    get_environment_details,
    INTERVAL_TYPE_MAP,
)
from esofile_reader.processing.sql_variables import (
    create_report_data_table,
    create_data_dictionary_table,
    get_output_data,
    process_sql_header,
)


def process_environment_data(
    conn: Connection,
    time_table: Table,
    data_table: Table,
    env_index: int,
    env_name: str,
    header: Dict[str, Dict[int, Variable]],
    logger: GenericLogger,
) -> RawSqlData:
    dates = {}
    days_of_week = {}
    n_minutes = {}
    outputs = {}
    interval_types = get_interval_types(conn, time_table, env_index)
    logger.set_maximum_progress(len(interval_types))
    for interval_type in interval_types:
        interval = INTERVAL_TYPE_MAP[interval_type]
        dates[interval] = get_dates(conn, time_table, env_index, interval_type)
        if interval_type <= 2:
            days_of_week[interval] = get_days_of_week(
                conn, time_table, env_index, interval_type
            )
        else:
            n_minutes[interval] = get_intervals(conn, time_table, env_index, interval_type)
        outputs[interval] = get_output_data(
            conn, time_table, data_table, interval_type, env_index
        )
        logger.increment_progress()
    return RawSqlData(
        environment_name=env_name,
        header=deepcopy(header),
        outputs=outputs,
        dates=dates,
        n_minutes=n_minutes,
        days_of_week=days_of_week,
    )


def read_sql_file(
    engine: Engine, metadata: MetaData, logger: GenericLogger
) -> List[RawSqlData]:
    # reflect database object, this could be done using 'autoload=True' but
    # better to define tables explicitly to have a useful reference
    time_table = create_time_table(metadata)
    data_dict_table = create_data_dictionary_table(metadata)
    data_table = create_report_data_table(metadata)
    environments_table = create_environment_periods_table(metadata)

    with engine.connect() as conn:
        header = process_sql_header(conn, data_dict_table)
        all_raw_data = []
        for env_index, env_name in get_environment_details(conn, environments_table):
            logger.log_section(f"Processing environment '{env_name}'!")
            raw_sql_data = process_environment_data(
                conn, time_table, data_table, env_index, env_name, header, logger
            )
            all_raw_data.append(raw_sql_data)
    return all_raw_data


def process_sql_file(
    file_path: PathLike, logger: GenericLogger, echo=False,
) -> List[RawSqlData]:
    engine = create_engine(f"sqlite:///{file_path}", echo=echo)
    metadata = MetaData(bind=engine)
    # validate sql schema
    return read_sql_file(engine, metadata, logger)
