import re
from typing import Iterable, Any, Dict, List, Type, Union, Optional, Tuple

import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime

from esofile_reader.constants import *


def parse_table_name(name: str) -> Optional[Tuple[int, str, str]]:
    p = re.compile("^(\d+)-([\w\s]+)-([\w\s]+)$")
    f = p.findall(name)
    if f:
        id_ = int(f[0][0])
        key = f[0][1]
        interval = f[0][2]
        return id_, key, interval


def get_table_name(file_id: int, key: str, interval: str) -> str:
    name = f"{file_id}-{key}-{interval}"
    p = re.compile("^(\d+)-([\w\s]+)-([\w\s]+)$")
    if bool(p.match(name)):
        return name
    else:
        raise NameError(
            f"Invalid sql table name: '{name}'. "
            f"Table key and interval should only use alphanumeric characters"
        )


def create_results_table(
        metadata: MetaData, file_id: int, interval: str, is_simple: bool
) -> Table:
    name = get_table_name(file_id, "results", interval)
    if is_simple:
        table = Table(
            name,
            metadata,
            Column(ID_LEVEL, Integer, primary_key=True, index=True, autoincrement=True),
            Column(INTERVAL_LEVEL, String(50)),
            Column(KEY_LEVEL, String(50)),
            Column(UNITS_LEVEL, String(50)),
            Column(STR_VALUES, String(50)),
        )
    else:
        table = Table(
            name,
            metadata,
            Column(ID_LEVEL, Integer, primary_key=True, index=True, autoincrement=True),
            Column(INTERVAL_LEVEL, String(50)),
            Column(KEY_LEVEL, String(50)),
            Column(TYPE_LEVEL, String(50)),
            Column(UNITS_LEVEL, String(50)),
            Column(STR_VALUES, String(50)),
        )
    table.create()
    return table


def create_datetime_table(metadata: MetaData, file_id: int, interval: str) -> Table:
    name = get_table_name(file_id, "index", interval)
    table = Table(name, metadata, Column(VALUE_LEVEL, DateTime))
    table.create()
    return table


def create_special_table(
        metadata: MetaData,
        file_id: int, interval: str,
        key: str,
        column_type: Union[Type[Integer], Type[String]]
) -> Table:
    name = get_table_name(file_id, key, interval)
    table = Table(name, metadata, Column(VALUE_LEVEL, column_type))
    table.create()
    return table


def create_value_insert(values: Iterable[Any]) -> List[Dict[str, Any]]:
    ins = []
    for value in values:
        ins.append({VALUE_LEVEL: value})
    return ins


def destringify_values(df: pd.DataFrame, separator="\t"):
    """ Transform joined str field into numeric columns. """
    names = df.index.names
    df = df.applymap(lambda x: x.split(separator)).T

    dct = {}
    for index, val in df.iloc[0, :].iteritems():
        dct[index] = val
    df = pd.DataFrame(dct, dtype="float64")
    df.columns.set_names(names, inplace=True)

    return df


def merge_df_values(df: pd.DataFrame, separator: str) -> pd.Series:
    """ Merge all column values into a single str pd.Series. """
    df = df.astype(str)
    str_df = df.apply(lambda x: f"{separator}".join(x.to_list()))
    return str_df
