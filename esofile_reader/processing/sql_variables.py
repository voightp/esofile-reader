from typing import List, Tuple, Dict

from sqlalchemy import select, Table, Column, Integer, String, ForeignKey, Float, MetaData
from sqlalchemy.engine.base import Connection

from esofile_reader.constants import *
from esofile_reader.mini_classes import Variable

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


def get_reporting_frequencies(conn: Connection, data_dict_table: Table):
    s = select([data_dict_table.c.ReportingFrequency]).distinct()
    return [r[0] for r in conn.execute(s)]


def get_header_data(
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


def parse_sql_header(
    sql_header: List[Tuple[int, int, str, str, str, str]]
) -> Dict[int, Variable]:
    header = {}
    for id_, is_meter, key, type_, frequency, units in sql_header:
        if is_meter:
            type_ = key
            key = "Cumulative Meter" if key == "Cumulative" else "Meter"
        header[id_] = Variable(DATA_DICT_MAP[frequency], key, type_, units)
    return header
