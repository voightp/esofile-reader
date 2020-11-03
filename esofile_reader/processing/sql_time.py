from datetime import datetime
from typing import Optional, List, Tuple

from sqlalchemy import select, and_, func, literal, Table, Column, Integer, String, MetaData
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.selectable import Select

from esofile_reader.constants import *
from esofile_reader.processing.esofile_time import parse_eplus_timestamp

MONTH_PLACEHOLDER = literal(1)
DAY_PLACEHOLDER = literal(1)
HOUR_PLACEHOLDER = literal(0)
MINUTE_PLACEHOLDER = literal(0)

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


def get_env_interval_pairs(conn: Connection, time_table: Table) -> List[Tuple[int, int]]:
    s = select([time_table.c.EnvironmentPeriodIndex, time_table.c.IntervalType]).distinct()
    return conn.execute(s).fetchall()


def get_env_interval_select(
    time_table: Table, env_index: int, interval_type: int, columns: List[Column]
) -> Select:
    return select(columns).where(
        and_(
            time_table.c.IntervalType == interval_type,
            time_table.c.EnvironmentPeriodIndex == env_index,
        )
    )


def get_timestep_statement(time_table: Table, env_index: int) -> Select:
    # dealing with hourly, zone timestep and system timestep
    return get_env_interval_select(
        time_table,
        env_index=env_index,
        interval_type=-1,
        columns=[
            time_table.c.Year,
            time_table.c.Month,
            time_table.c.Day,
            time_table.c.Hour,
            time_table.c.Minute,
        ],
    )


def get_hourly_statement(time_table: Table, env_index: int) -> Select:
    # dealing with hourly, zone timestep and system timestep
    return get_env_interval_select(
        time_table,
        env_index=env_index,
        interval_type=1,
        columns=[
            time_table.c.Year,
            time_table.c.Month,
            time_table.c.Day,
            time_table.c.Hour,
            time_table.c.Minute,
        ],
    )


def get_daily_statement(time_table: Table, env_index: int) -> Select:
    return get_env_interval_select(
        time_table,
        env_index=env_index,
        interval_type=2,
        columns=[
            time_table.c.Year,
            time_table.c.Month,
            time_table.c.Day,
            HOUR_PLACEHOLDER,
            MINUTE_PLACEHOLDER,
        ],
    )


def get_monthly_statement(time_table: Table, env_index: int) -> Select:
    return get_env_interval_select(
        time_table,
        env_index=env_index,
        interval_type=3,
        columns=[
            time_table.c.Year,
            time_table.c.Month,
            DAY_PLACEHOLDER,
            HOUR_PLACEHOLDER,
            MINUTE_PLACEHOLDER,
        ],
    )


def get_annual_statement(time_table: Table, env_index: int) -> Select:
    return get_env_interval_select(
        time_table,
        env_index=env_index,
        interval_type=5,
        columns=[
            time_table.c.Year,
            MONTH_PLACEHOLDER,
            DAY_PLACEHOLDER,
            HOUR_PLACEHOLDER,
            MINUTE_PLACEHOLDER,
        ],
    )


def get_min_year_statement(time_table: Table, env_index: int) -> Optional[int]:
    return select([func.min(time_table.c.Year)]).where(
        time_table.c.EnvironmentPeriodIndex == env_index
    )


def get_runperiod_statement(time_table: Table, env_index: int) -> Select:
    min_year = get_min_year_statement(time_table, env_index)
    return get_env_interval_select(
        time_table,
        env_index=env_index,
        interval_type=4,
        columns=[
            min_year,
            MONTH_PLACEHOLDER,
            DAY_PLACEHOLDER,
            HOUR_PLACEHOLDER,
            MINUTE_PLACEHOLDER,
        ],
    )


def get_timestamps(
    conn: Connection, time_table: Table, env_index: int, interval_type: int
) -> List[Tuple[int, ...]]:
    switch = {
        -1: get_timestep_statement,
        1: get_hourly_statement,
        2: get_daily_statement,
        3: get_monthly_statement,
        4: get_runperiod_statement,
        5: get_annual_statement,
    }
    statement = switch[interval_type](time_table, env_index)
    return conn.execute(statement).fetchall()


def get_days_of_week(
    conn: Connection, time_table: Table, env_index: int, interval_type: int
) -> List[str]:
    statement = get_env_interval_select(
        time_table, env_index, interval_type, [time_table.c.DayType]
    )
    return [r[0] for r in conn.execute(statement)]


def get_intervals(
    conn: Connection, time_table: Table, env_index: int, interval_type: int
) -> List[Optional[int]]:
    statement = get_env_interval_select(
        time_table, env_index, interval_type, [time_table.c.Interval]
    )
    return [r[0] for r in conn.execute(statement)]


def parse_eplus_timestamps(eplus_timestamps: List[Tuple[int, ...]]) -> List[datetime]:
    timestamps = []
    for eplus_timestamp in eplus_timestamps:
        year, month, day, hour, minute = eplus_timestamp
        year = 2002 if year == 0 else year
        timestamps.append(parse_eplus_timestamp(year, month, day, hour, minute))
    return timestamps


def process_interval_data(interval_type: int):
    pass
