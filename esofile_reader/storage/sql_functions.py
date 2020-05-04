from typing import Iterable, Any, Dict, List

import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime


def create_results_table(metadata: MetaData, file_id: int, interval: str) -> Table:
    name = f"{file_id}-results-{interval}"

    table = Table(
        name,
        metadata,
        Column("id", Integer, primary_key=True, index=True, autoincrement=True),
        Column("interval", String(50)),
        Column("key", String(50)),
        Column("type", String(50)),
        Column("units", String(50)),
        Column("str_values", String(50)),
    )

    table.create()

    return table


def create_datetime_table(metadata: MetaData, file_id: int, interval: str) -> Table:
    name = f"{file_id}-index-{interval}"

    table = Table(name, metadata, Column("value", DateTime))

    table.create()

    return table


def create_n_days_table(metadata: MetaData, file_id: int, interval: str) -> Table:
    name = f"{file_id}-n_days-{interval}"

    table = Table(name, metadata, Column("value", Integer))

    table.create()

    return table


def create_day_table(metadata: MetaData, file_id: int, interval: str) -> Table:
    name = f"{file_id}-day-{interval}"

    table = Table(name, metadata, Column("value", String(10)))

    table.create()

    return table


def create_value_insert(values: Iterable[Any]) -> List[Dict[str, Any]]:
    ins = []
    for value in values:
        ins.append({"value": value})
    return ins


def destringify_values(df: pd.DataFrame, separator="\t"):
    """ Transform joined str field into numeric columns. """
    names = df.index.names
    df = df.applymap(lambda x: x.split(separator)).T

    dct = {}
    for index, val in df.iloc[0, :].iteritems():
        dct[index] = val
    df = pd.DataFrame(dct, dtype=float)
    df.columns.set_names(names, inplace=True)

    return df


def merge_df_values(df: pd.DataFrame, separator: str) -> pd.Series:
    """ Merge all column values into a single str pd.Series. """
    df = df.astype(str)
    str_df = df.apply(lambda x: f"{separator}".join(x.to_list()))
    return str_df
