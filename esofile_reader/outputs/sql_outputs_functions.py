from uuid import uuid1
from esofile_reader import EsoFile
from esofile_reader.utils.utils import profile
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, \
    DateTime, Boolean, Sequence, Text, inspect, select
import pandas as pd
import contextlib
from sqlalchemy import exc
import sqlalchemy


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
