from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict

from sqlalchemy import select, and_, func, literal, Table, Column, Integer, String, MetaData
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.selectable import Select

from esofile_reader.constants import *
from esofile_reader.processing.esofile_time import get_annual_n_days

INTERVAL_TYPE_MAP = {
    -1: TS,
    1: H,
    2: D,
    3: M,
    4: RP,
    5: A,
}


def create_time_table(metadata: MetaData) -> Table:
    return Table(
        "Time",
        metadata,
        Column("TimeIndex", Integer, primary_key=True),
        Column("Year", Integer),
        Column("Month", Integer),
        Column("Day", Integer),
        Column("Hour", Integer),
        Column("Minute", Integer),
        Column("Dst", Integer),
        Column("Interval", Integer),
        Column("IntervalType", Integer),
        Column("SimulationDays", Integer),
        Column("DayType", String),
        Column("EnvironmentPeriodIndex", Integer),
        Column("WarmupFlag", Integer),
    )


def create_environment_periods_table(metadata: MetaData) -> Table:
    return Table(
        "EnvironmentPeriods",
        metadata,
        Column("EnvironmentPeriodIndex", Integer, primary_key=True),
        Column("SimulationIndex", Integer),
        Column("EnvironmentName", String),
        Column("EnvironmentType", Integer),
    )


def get_environment_details(conn: Connection, env_table: Table) -> List[Tuple[int, str]]:
    statement = select([env_table.c.EnvironmentPeriodIndex, env_table.c.EnvironmentName])
    return conn.execute(statement).fetchall()


def get_time_columns_with_placeholders(time_table, interval_type: int) -> List[Column]:
    month_placeholder = literal(1, type_=Integer)
    day_placeholder = literal(1, type_=Integer)
    hour_placeholder = literal(0, type_=Integer)
    minute_placeholder = literal(0, type_=Integer)

    if interval_type in {1, -1}:
        columns = [
            time_table.c.Month,
            time_table.c.Day,
            time_table.c.Hour,
            time_table.c.Minute,
        ]
    elif interval_type == 2:
        columns = [
            time_table.c.Month,
            time_table.c.Day,
            hour_placeholder,
            minute_placeholder,
        ]
    elif interval_type == 3:
        columns = [
            time_table.c.Month,
            day_placeholder,
            hour_placeholder,
            minute_placeholder,
        ]
    elif interval_type in {4, 5}:
        columns = [
            month_placeholder,
            day_placeholder,
            hour_placeholder,
            minute_placeholder,
        ]
    else:
        raise KeyError(f"Unexpected interval type '{interval_type}'!")
    return columns


def get_interval_types(conn: Connection, time_table: Table, env_index: int) -> List[int]:
    statement = (
        select([time_table.c.IntervalType])
        .where(time_table.c.EnvironmentPeriodIndex == env_index)
        .distinct()
    )
    return [r[0] for r in conn.execute(statement)]


def get_filtered_statement(
    time_table: Table, env_index: int, interval_type: int, columns: List[Column]
) -> Select:
    return select(columns).where(
        and_(
            time_table.c.IntervalType == interval_type,
            time_table.c.EnvironmentPeriodIndex == env_index,
        )
    )


def get_lowest_year_statement(time_table: Table, env_index: int) -> Optional[int]:
    return select([func.min(time_table.c.Year)]).where(
        time_table.c.EnvironmentPeriodIndex == env_index
    )


def get_date_columns(time_table: Table, env_index: int, interval_type: int):
    year = (
        get_lowest_year_statement(time_table, env_index)
        if interval_type == 4
        else time_table.c.Year
    )
    month, day, hour, minute = get_time_columns_with_placeholders(time_table, interval_type)
    return year, month, day, hour, minute


def get_dates_statement(time_table: Table, env_index: int, interval_type: int) -> Select:
    year, month, day, hour, minute = get_date_columns(time_table, env_index, interval_type)
    statement = get_filtered_statement(
        time_table,
        env_index=env_index,
        interval_type=interval_type,
        columns=[year, month, day, hour, minute],
    )
    return statement


def get_dates(
    conn: Connection, time_table: Table, env_index: int, interval_type: int
) -> List[Tuple[int, ...]]:
    return conn.execute(get_dates_statement(time_table, env_index, interval_type)).fetchall()


def get_days_of_week(
    conn: Connection, time_table: Table, env_index: int, interval_type: int
) -> List[str]:
    statement = get_filtered_statement(
        time_table, env_index, interval_type, [time_table.c.DayType]
    )
    return [r[0] for r in conn.execute(statement)]


def get_intervals(
    conn: Connection, time_table: Table, env_index: int, interval_type: int
) -> List[Optional[int]]:
    statement = get_filtered_statement(
        time_table, env_index, interval_type, [time_table.c.Interval]
    )
    return [r[0] for r in conn.execute(statement)]


def get_n_days_from_minutes(
    n_minutes: Dict[str, List[Optional[int]]], dates: Dict[str, List[datetime]]
) -> Dict[str, List[int]]:
    n_days = {}
    for interval, n_minutes_arr in n_minutes.items():
        if interval == A:
            n_days[A] = get_annual_n_days(dates[A])
        else:
            n_days[interval] = [int(n / 1440) if n else None for n in n_minutes_arr]
    return n_days


def parse_sql_timestamp(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    if hour == 24:
        # Convert last step of day
        shifted_datetime = datetime(year, month, day, hour - 1)
        corrected_datetime = shifted_datetime + timedelta(hours=1)
    else:
        corrected_datetime = datetime(year, month, day, hour, minute)
    return corrected_datetime


def parse_sql_timestamps(eplus_timestamps: List[Tuple[int, ...]]) -> List[datetime]:
    timestamps = []
    for eplus_timestamp in eplus_timestamps:
        year, month, day, hour, minute = eplus_timestamp
        year = 2002 if (year == 0 or year is None) else year
        timestamps.append(parse_sql_timestamp(year, month, day, hour, minute))
    return timestamps


def convert_raw_sql_date_data(
    eplus_timestamps: Dict[str, List[Tuple[int, ...]]]
) -> Dict[str, List[datetime]]:
    datetime_dates = {}
    for interval, eplus_timestamp in eplus_timestamps.items():
        datetime_dates[interval] = parse_sql_timestamps(eplus_timestamp)
    return datetime_dates
