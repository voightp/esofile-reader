from uuid import uuid1
from esofile_reader import EsoFile
from esofile_reader.utils.utils import profile
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, \
    DateTime, Boolean, Sequence, Text, inspect, select
import pandas as pd
import contextlib
from sqlalchemy import exc
import sqlalchemy

FILE_TABLE = "resultfiles"


def set_up_db(path=None, echo=True):
    path = path if path else ":memory:"

    engine = create_engine(f'sqlite:///{path}', echo=echo)
    metadata = MetaData(engine, reflect=True)

    if FILE_TABLE not in metadata.tables.keys():
        file = Table(
            FILE_TABLE, metadata,
            Column("id", Integer, Sequence('db_id_seq'), primary_key=True),
            Column("file_path", String(120)),
            Column("file_name", String(50)),
            Column("file_timestamp", DateTime),
            Column("table_indexes", String(20)),
            Column("table_timestep", String(20)),
            Column("table_hourly", String(20)),
            Column("table_daily", String(20)),
            Column("table_monthly", String(20)),
            Column("table_annual", String(20)),
            Column("table_runperiod", String(20))
        )

        with contextlib.suppress(exc.InvalidRequestError, exc.OperationalError):
            file.create()

    return engine, metadata


def merge_df_values(df: pd.DataFrame, separator: str = " ") -> pd.Series:
    """ Merge all column values into a single str pd.Series. """
    df = df.astype(str)
    str_df = df.apply(lambda x: f"{separator}".join(x.to_list()))
    return str_df


@profile
def results_table_generator(metadata, file_id, interval, df):
    name = f"outputs-{interval}-{file_id}]"

    table = Table(
        name, metadata,
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        Column("interval", String(50)),
        Column("key", String(50)),
        Column("variable", String(50)),
        Column("units", String(50)),
        Column("values", Text)
    )

    table.create()

    sr = merge_df_values(df, "\t")
    ins = []

    for index, values in sr.iteritems():
        ins.append(
            {
                "id": index[0],
                "interval": index[1],
                "key": index[2],
                "variable": index[3],
                "units": index[4],
                "values": values
            }
        )

    return name, ins


@profile
def dates_table_generator(metadata, file_id, interval, date_range):
    name = f"index-{interval}-{file_id}]"

    table = Table(
        name, metadata,
        Column("timestep_dates", Text),
        Column("hourly_dates", Text),
        Column("daily_dates", Text),
        Column("monthly_dates", Text),
        Column("annual_dates", Text),
        Column("runperiod_dates", Text),
        Column("timestep_days", Text),
        Column("hourly_days", Text),
        Column("daily_days", Text),
        Column("monthly_n_days", Text),
        Column("annual_n_days", Text),
        Column("runperiod_n_days", Text),
    )

    table.create()

    str_date_range = "\t".join(date_range.strftime("%Y/%m/%d %H:%M%S"))
    ins = {"timestamps": str_date_range}

    return name, ins


@profile
def store_file(result_file, engine, metadata):
    f = metadata.tables["resultfiles"]
    ins = f.insert().values(
        file_path=result_file.file_path,
        file_name=result_file.file_name,
        file_timestamp=result_file.created
    )
    with engine.connect() as conn:
        id_ = conn.execute(ins).inserted_primary_key[0]

    for interval, df in result_file._outputs.items():
        results_name, results_ins = results_table_generator(metadata, id_, interval, df.only_numeric)
        timestamp_name, timestamp_ins = dates_table_generator(metadata, id_, interval, df.index)

        with engine.connect() as conn:
            conn.execute(metadata.tables[results_name].insert(), results_ins)
            conn.execute(metadata.tables[timestamp_name].insert(), timestamp_ins)


if __name__ == "__main__":
    ef = EsoFile(r"C:\Users\vojtechp1\AppData\Local\DesignBuilder\EnergyPlus\eplusout.eso",
                 report_progress=True)

    eng, meta = set_up_db(echo=False)

    store_file(ef, eng, meta)

    eng, meta = set_up_db("test.db", echo=False)

    store_file(ef, eng, meta)
