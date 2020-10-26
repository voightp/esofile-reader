from pathlib import Path

from sqlalchemy import Table, MetaData, create_engine, select
from sqlalchemy.engine import Engine

TEST_FILE_PATH = Path(Path(__file__).parents[2], "tests", "eso_files", "eplusout.sql")


def set_up_db(path: str, echo=True) -> Engine:
    engine = create_engine(f"sqlite:///{path}", echo=echo)
    metadata = MetaData(bind=engine)

    # time_table = Table(
    #     "Time",
    #     metadata,
    #     Column("TimeIndex", Integer, primary_key=True),
    #     Column("Year", Integer),
    #     Column("Month", Integer),
    #     Column("Day", Integer),
    #     Column("Hour", Integer),
    #     Column("Minute", Integer),
    #     Column("Dst", Integer),
    #     Column("Interval", Integer),
    #     Column("IntervalType", Integer),
    #     Column("SimulationDays", Integer),
    #     Column("DayType", String),
    #     Column("EnvironmentPeriodIndex", String),
    #     Column("WarmpupFlag", String),
    # )

    # data_table = Table(
    #     "ReportData",
    #     metadata,
    #     Column("ReportDataIndex", Integer, primary_key=True),
    #     Column(
    #         "ReportDataDictionaryIndex",
    #         Integer,
    #         ForeignKey("ReportDataDictionary.ReportDataDictionaryIndex")
    #     ),
    #     Column("Value", Float),
    # )

    # data_dict_table = Table(
    #     "ReportDataDictionary",
    #     metadata,
    #     Column("ReportDataDictionaryIndex", Integer, primary_key=True),
    #     Column("IsMeter", Integer),
    #     Column("Type", String),
    #     Column("IndexGroup", String),
    #     Column("TimestepType", String),
    #     Column("KeyValue", String),
    #     Column("Name", String),
    #     Column("ReportingFrequency", String),
    #     Column("ScheduleName", String),
    #     Column("Units", String),
    # )

    time_table = Table("Time", metadata, autoload=True)
    data_dict_table = Table("ReportDataDictionary", metadata, autoload=True)
    data_table = Table("ReportData", metadata, autoload=True)

    metadata.reflect()
    time_series = {}
    interval_types = []

    with engine.connect() as conn:
        s = select([time_table.c.IntervalType]).distinct()
        res = conn.execute(s).fetchall()
        interval_types = [r[0] for r in res]
        for interval_type in interval_types:
            if abs(interval_type) == 1:
                # dealing with hourly, zone timestep and system timestep
                s = select(
                    time_table.c.TimeIndex,
                    time_table.c.Year,
                    time_table.c.Month,
                    time_table.c.Day,
                    time_table.c.Hour,
                    time_table.c.Minute,
                    time_table.c.Interval,
                    time_table.c.DayType,
                ).where(time_table.c.IntervalType.in_([1, -1]))
            elif interval_type == 2:
                # daily
                s = select(
                    time_table.c.TimeIndex,
                    time_table.c.Year,
                    time_table.c.Month,
                    time_table.c.Day,
                    time_table.c.DayType,
                ).where(time_table.c.IntervalType == 2)
            elif interval_type == 3:
                # monthly
                s = select(
                    time_table.c.TimeIndex,
                    time_table.c.Year,
                    time_table.c.Month,
                    time_table.c.Day,
                    time_table.c.Interval,
                ).where(time_table.c.IntervalType == 3)
            elif interval_type == 4:
                # runperiod
                s = select(
                    time_table.c.TimeIndex,
                    time_table.c.Year,
                    time_table.c.Month,
                    time_table.c.Day,
                    time_table.c.Interval,
                ).where(time_table.c.IntervalType == 4)
            elif interval_type == 5:
                # annual - not sure yet
                pass

        # s = select(
        #     [
        #         time_table.c.Year,
        #         time_table.c.Month,
        #         time_table.c.Day
        #     ]
        # ).where(or_(time_table.c.IntervalType.in_([-1,1])))
        # res = conn.execute(s)
        # print(res.fetchall())

    return engine
