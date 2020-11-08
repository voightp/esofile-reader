from typing import Dict

from sqlalchemy import ForeignKey, Float

from esofile_reader.mini_classes import Variable
from esofile_reader.processing.sql_time import *

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
        Column("TimeIndex", Integer, ForeignKey("Time.TimeIndex"),),
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
    header = {}
    for frequency in get_reporting_frequencies(conn, data_dict_table):
        sql_variable_data = get_variable_data(conn, data_dict_table, frequency)
        header[DATA_DICT_MAP[frequency]] = parse_sql_variable_data(sql_variable_data)
    return header


def parse_sql_variable_data(
    sql_header: List[Tuple[int, int, str, str, str, str]]
) -> Dict[int, Variable]:
    header = {}
    for id_, is_meter, key, type_, frequency, units in sql_header:
        if is_meter:
            meter_type = "Cumulative Meter" if key.strip() == "Cumulative" else "Meter"
            key = type_
            type_ = meter_type
        header[id_] = Variable(DATA_DICT_MAP[frequency], key, type_, units)
    return header


def get_output_data(
    conn: Connection, time_table: Table, data_table: Table, interval_type: int, env_index: int,
) -> Dict[str, List[Tuple[int, int, float]]]:
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
