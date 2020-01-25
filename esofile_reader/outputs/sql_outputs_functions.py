from uuid import uuid1
from esofile_reader import EsoFile
from esofile_reader.utils.utils import profile
from esofile_reader.constants import *
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, \
    DateTime, Boolean, Sequence, Text, inspect, select
import pandas as pd
import contextlib
from sqlalchemy import exc
import sqlalchemy


@profile
def results_table_generator(metadata, file_id, interval):
    name = f"outputs-{interval}-{file_id}"

    table = Table(
        name, metadata,
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        Column("interval", String(50)),
        Column("key", String(50)),
        Column("variable", String(50)),
        Column("units", String(50)),
        Column("values", Text),
    )

    table.create()

    return name


@profile
def dates_table_generator(metadata, file_id):
    name = f"indexes-{file_id}"

    table = Table(
        name, metadata,
        Column("timestep_dt", Text),
        Column("hourly_dt", Text),
        Column("daily_dt", Text),
        Column("monthly_dt", Text),
        Column("annual_dt", Text),
        Column("runperiod_dt", Text),
        Column("timestep_days", Text),
        Column("hourly_days", Text),
        Column("daily_days", Text),
        Column("monthly_n_days", Text),
        Column("annual_n_days", Text),
        Column("runperiod_n_days", Text),
    )

    table.create()

    return name


def merge_df_values(df: pd.DataFrame, separator: str) -> pd.Series:
    """ Merge all column values into a single str pd.Series. """
    df = df.astype(str)
    str_df = df.apply(lambda x: f"{separator}".join(x.to_list()))
    return str_df


@profile
def create_results_insert(df, separator):
    sr = merge_df_values(df, separator)
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

    return ins


@profile
def merge_sr_values(sr: pd.Series, separator: str) -> str:
    """ Merge all column values into a single str pd.Series. """
    sr = sr.astype(str).tolist()
    return f"{separator}".join(sr)


@profile
def create_index_insert(interval, df, separator=" "):
    ids = df.columns.get_level_values("id")
    ins = {}

    if N_DAYS_COLUMN in ids:
        # this should be available only for monthly - runperiod
        ins[f"{interval}_n_days"] = merge_sr_values(df.loc[:, N_DAYS_COLUMN], separator)

    if DAY_COLUMN in ids:
        # this should be available only for monthly - runperiod
        ins[f"{interval}_days"] = merge_sr_values(df.loc[:, DAY_COLUMN], separator)

    if isinstance(df.index, pd.DatetimeIndex):
        str_date_range = f"{separator}".join(df.index.strftime("%Y/%m/%d %H:%M:%S"))
        ins[f"{interval}_dt"] = str_date_range

    return ins
