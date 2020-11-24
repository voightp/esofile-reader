from copy import deepcopy
from typing import List, Tuple, Dict

from sqlalchemy import (
    MetaData,
    create_engine,
    Table,
    Column,
    Integer,
    String,
    select,
    and_,
    ForeignKey,
    Float,
)
from sqlalchemy.engine import reflection
from sqlalchemy.engine.base import Connection, Engine

from esofile_reader.constants import *
from esofile_reader.exceptions import NoResults
from esofile_reader.mini_classes import PathLike, Variable
from esofile_reader.processing.progress_logger import BaseLogger
from esofile_reader.processing.raw_data import RawSqlData
from esofile_reader.processing.sql_time import (
    get_dates,
    get_days_of_week,
    get_cumulative_days,
    create_time_table,
    create_environment_periods_table,
    get_interval_types,
    get_environment_details,
    INTERVAL_TYPE_MAP,
)

DATA_DICT_MAP = {
    "Zone Timestep": TS,
    "Hourly": H,
    "Daily": D,
    "Monthly": M,
    "Run Period": RP,
    "Annual": A,
    "HVAC System Timestep": TS,
}


def create_report_data_table(metadata: MetaData) -> Table:
    return Table(
        "ReportData",
        metadata,
        Column("ReportDataIndex", Integer, primary_key=True),
        Column("TimeIndex", Integer, ForeignKey("Time.TimeIndex")),
        Column(
            "ReportDataDictionaryIndex",
            Integer,
            ForeignKey("ReportDataDictionary.ReportDataDictionaryIndex"),
        ),
        Column("Value", Float),
    )


def create_data_dictionary_table(metadata: MetaData) -> Table:
    return Table(
        "ReportDataDictionary",
        metadata,
        Column("ReportDataDictionaryIndex", Integer, primary_key=True),
        Column("IsMeter", Integer),
        Column("Type", String),
        Column("IndexGroup", String),
        Column("TimestepType", String),
        Column("KeyValue", String),
        Column("Name", String),
        Column("ReportingFrequency", String),
        Column("ScheduleName", String),
        Column("Units", String),
    )


def get_reporting_frequencies(conn: Connection, data_dictionary_table: Table) -> List[str]:
    statement = select([data_dictionary_table.c.ReportingFrequency]).distinct()
    return [r[0] for r in conn.execute(statement)]


def get_variable_data(
    conn: Connection, data_dict_table: Table, frequency: str
) -> List[Tuple[int, int, str, str, str, str]]:
    statement = select(
        [
            data_dict_table.c.ReportDataDictionaryIndex,
            data_dict_table.c.IsMeter,
            data_dict_table.c.KeyValue,
            data_dict_table.c.Name,
            data_dict_table.c.ReportingFrequency,
            data_dict_table.c.Units,
        ]
    ).where(data_dict_table.c.ReportingFrequency == frequency)
    return conn.execute(statement).fetchall()


def process_sql_header(
    conn: Connection, data_dict_table: Table
) -> Dict[str, Dict[int, Variable]]:
    from collections import defaultdict

    header = defaultdict(dict)
    for frequency in get_reporting_frequencies(conn, data_dict_table):
        sql_variable_data = get_variable_data(conn, data_dict_table, frequency)
        header[DATA_DICT_MAP[frequency]].update(parse_sql_variable_data(sql_variable_data))
    return header


def parse_sql_variable_data(
    sql_header: List[Tuple[int, int, str, str, str, str]]
) -> Dict[int, Variable]:
    header = {}
    for id_, is_meter, key, type_, frequency, units in sql_header:
        if is_meter:
            if key == "Cumulative ":
                type_ = "Cumulative " + type_
                key = "Cumulative Meter"
            else:
                key = "Meter"
        if frequency == "HVAC System Timestep":
            type_ = "System - " + type_
        header[id_] = Variable(DATA_DICT_MAP[frequency], key, type_, units)
    return header


def get_output_data(
    conn: Connection, time_table: Table, data_table: Table, interval_type: int, env_index: int,
) -> List[Tuple[int, int, float]]:
    s = (
        select(
            [
                time_table.c.TimeIndex,
                data_table.c.ReportDataDictionaryIndex,
                data_table.c.Value,
            ]
        )
        .select_from(time_table.join(data_table))
        .where(
            and_(
                time_table.c.IntervalType == interval_type,
                time_table.c.EnvironmentPeriodIndex == env_index,
            )
        )
    )
    return conn.execute(s).fetchall()


def process_environment_data(
    conn: Connection,
    time_table: Table,
    data_table: Table,
    env_index: int,
    env_name: str,
    header: Dict[str, Dict[int, Variable]],
    logger: BaseLogger,
) -> RawSqlData:
    dates = {}
    days_of_week = {}
    cumulative_days = {}
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
            cumulative_days[interval] = get_cumulative_days(
                conn, time_table, env_index, interval_type
            )
        outputs[interval] = get_output_data(
            conn, time_table, data_table, interval_type, env_index
        )
        logger.increment_progress()
    return RawSqlData(
        environment_name=env_name,
        header=header,
        outputs=outputs,
        dates=dates,
        cumulative_days=cumulative_days,
        days_of_week=days_of_week,
    )


def read_sql_file(engine: Engine, metadata: MetaData, logger: BaseLogger) -> List[RawSqlData]:
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
            logger.log_section(f"Processing environment '{env_name}'")
            raw_sql_data = process_environment_data(
                conn, time_table, data_table, env_index, env_name, deepcopy(header), logger
            )
            all_raw_data.append(raw_sql_data)
    return all_raw_data


def validate_sql_file(engine, required):
    inspector = reflection.Inspector.from_engine(engine)
    table_names = inspector.get_table_names()
    return all(map(lambda x: x in table_names, required))


def process_sql_file(file_path: PathLike, logger: BaseLogger, echo=False) -> List[RawSqlData]:
    engine = create_engine(f"sqlite:///{file_path}", echo=echo)
    metadata = MetaData(bind=engine)
    required = ["Time", "ReportData", "ReportDataDictionary"]
    if validate_sql_file(engine, required):
        return read_sql_file(engine, metadata, logger)
    else:
        raise NoResults(f"Database does not contain '[{required}]' tables.")
