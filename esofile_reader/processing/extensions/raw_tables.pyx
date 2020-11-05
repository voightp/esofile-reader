import contextlib
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.mini_classes import Variable
from esofile_reader.processing.progress_logger import EsoFileLogger
from esofile_reader.df.df_functions import create_peak_min_outputs, create_peak_max_outputs
from esofile_reader.df.df_tables import DFTables


def create_header_df(
    header: Dict[int, Variable], interval: str, index_name: str, columns: List[str]
) -> pd.DataFrame:
    """ Create a raw header pd.DataFrame for given interval. """
    rows, index = [], []
    for id_, var in header.items():
        rows.append([interval, var.key, var.type, var.units])
        index.append(id_)
    return pd.DataFrame(rows, columns=columns, index=pd.Index(index, name=index_name))


def create_values_df(outputs_dct: Dict[int, List[float]], index_name: str) -> pd.DataFrame:
    """ Create a raw values pd.DataFrame for given interval. """
    df = pd.DataFrame(outputs_dct)
    df = df.T
    df.index.set_names(index_name, inplace=True)
    return df


def generate_peak_tables(
    raw_peak_outputs: Dict[str, Dict[int, List[float]]],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str, List[datetime]],
    progress_logger: EsoFileLogger,
) -> Dict[str, DFTables]:
    """ Transform processed peak output data into DataFrame like classes. """
    min_peaks = DFTables()
    max_peaks = DFTables()
    for interval, values in raw_peak_outputs.items():
        df_values = create_values_df(values, ID_LEVEL)
        df_header = create_header_df(header[interval], interval, ID_LEVEL, COLUMN_LEVELS[1:])
        df = pd.merge(df_header, df_values, sort=False, left_index=True, right_index=True)
        df.set_index(keys=list(COLUMN_LEVELS[1:]), append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)

        min_df = create_peak_min_outputs(interval, df)
        min_peaks[interval] = min_df

        max_df = create_peak_max_outputs(interval, df)
        max_peaks[interval] = max_df

        progress_logger.increment_progress()

    # Peak outputs are stored in dictionary to distinguish min and max
    peak_outputs = {"local_min": min_peaks, "local_max": max_peaks}

    return peak_outputs


def generate_df_tables(
    raw_outputs: Dict[str, Dict[int, List[float]]],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str, List[datetime]],
    progress_logger: EsoFileLogger,
) -> DFTables:
    """ Transform processed output data into DataFrame like classes. """
    tables = DFTables()
    for interval, values in raw_outputs.items():
        df_values = create_values_df(values, ID_LEVEL)
        df_header = create_header_df(header[interval], interval, ID_LEVEL, COLUMN_LEVELS[1:])
        df = pd.merge(df_header, df_values, sort=False, left_index=True, right_index=True)
        df.set_index(keys=list(COLUMN_LEVELS[1:]), append=True, inplace=True)
        df = df.T
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)
        tables[interval] = df
        progress_logger.increment_progress()
    return tables


def insert_special_columns(
    tables: DFTables, special_data: Dict[str, Optional[Dict[str, List[Any]]]]
) -> None:
    """ Add other special columns. """
    for key, dct in special_data.items():
        if dct:
            for interval, arr in dct.items():
                tables.insert_special_column(interval, key, arr)


def remove_duplicates(
    duplicates: Dict[int, Variable],
    header: Dict[str, Dict[int, Variable]],
    outputs: Dict[str, Dict[int, List[float]]],
    peak_outputs: Optional[Dict[str, Dict[int, List[float]]]],
) -> None:
    """ Remove duplicate outputs from results set. """
    for id_, v in duplicates.items():
        logging.info(f"Duplicate variable found, removing variable: '{id_} - {v}'.")
        for dct in [header, outputs, peak_outputs]:
            if dct:
                with contextlib.suppress(KeyError):
                    del dct[v.table][id_]
