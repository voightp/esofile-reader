from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.df.df_functions import create_peak_min_outputs, create_peak_max_outputs
from esofile_reader.df.df_tables import DFTables
from esofile_reader.exceptions import FormatNotSupported
from esofile_reader.mini_classes import Variable
from esofile_reader.processing.esofile_time import convert_raw_date_data, get_n_days
from esofile_reader.processing.progress_logger import EsoFileLogger
from esofile_reader.processing.raw_data import RawData

try:
    from esofile_reader.processing.extensions.esofile import process_eso_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install(pyximport=True, language_level=3)
    from esofile_reader.processing.extensions.esofile import process_eso_file


def create_header_df(
    header: Dict[int, Variable], interval: str, index_name: str, columns: List[str]
) -> pd.DataFrame:
    """ Create a raw header pd.DataFrame for given interval. """
    rows, index = [], []
    for id_, var in header.items():
        rows.append([interval, var.key, var.type, var.units])
        index.append(id_)
    return pd.DataFrame(rows, columns=columns, index=pd.Index(index, name=index_name))


def create_values_df_from_columns(
    outputs_dct: Dict[int, List[float]], index_name: str
) -> pd.DataFrame:
    """ Create a raw values pd.DataFrame for given interval. """
    df = pd.DataFrame(outputs_dct)
    df = df.T
    df.index.set_names(index_name, inplace=True)
    return df


def insert_special_columns(
    tables: DFTables, special_data: Dict[str, Optional[Dict[str, List[Any]]]]
) -> None:
    """ Add other special columns. """
    for key, dct in special_data.items():
        if dct:
            for interval, arr in dct.items():
                tables.insert_special_column(interval, key, arr)


def choose_parser(file_path: Path):
    suffix = file_path.suffix
    if suffix == ".eso":
        parser = RawEsoParser()
    elif suffix == ".sql":
        parser = RawSqlParser()
    else:
        raise FormatNotSupported(
            f"Cannot process file '{file_path}', only '.eso'"
            f" and '.sql' EnergyPlus outputs are supported."
        )
    return parser


class Parser:
    @staticmethod
    @abstractmethod
    def process_file(file_path: Path, progress_logger: EsoFileLogger, ignore_peaks: bool):
        pass

    @staticmethod
    @abstractmethod
    def parse_date_data(raw_data: RawData, year: int) -> Dict[str, List[datetime]]:
        pass

    @staticmethod
    def parse_outputs(
        outputs: Dict[str, Any],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        special_columns: Dict[str, Dict[str, List[Union[str, int]]]],
        progress_logger: EsoFileLogger,
    ) -> DFTables:
        pass

    @staticmethod
    def parse_peak_outputs(
        peak_outputs: Dict[str, Any],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        progress_logger: EsoFileLogger,
    ):
        pass


class RawEsoParser(Parser):
    @staticmethod
    def process_file(file_path, progress_logger, ignore_peaks):
        return process_eso_file(file_path, progress_logger, ignore_peaks=ignore_peaks)

    @staticmethod
    def parse_date_data(raw_eso_data, year):
        n_days = get_n_days(raw_eso_data.dates, raw_eso_data.cumulative_days)
        dates = convert_raw_date_data(raw_eso_data.dates, raw_eso_data.days_of_week, year)
        return dates, n_days

    @staticmethod
    def parse_outputs(
        outputs: Dict[str, Dict[int, List[float]]],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        special_columns: Dict[str, Dict[str, List[Union[str, int]]]],
        progress_logger: EsoFileLogger,
    ) -> DFTables:
        tables = DFTables()
        for interval, values in outputs.items():
            df_values = create_values_df_from_columns(values, ID_LEVEL)
            df_header = create_header_df(
                header[interval], interval, ID_LEVEL, COLUMN_LEVELS[1:]
            )
            df = pd.merge(df_header, df_values, sort=False, left_index=True, right_index=True)
            df.set_index(keys=list(COLUMN_LEVELS[1:]), append=True, inplace=True)
            df = df.T
            df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)
            tables[interval] = df
            progress_logger.increment_progress()
        insert_special_columns(tables, special_columns)
        return tables

    @staticmethod
    def parse_peak_outputs(
        peak_outputs: Dict[str, Dict[int, List[float]]],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        progress_logger: EsoFileLogger,
    ):
        min_peaks = DFTables()
        max_peaks = DFTables()
        for interval, values in peak_outputs.items():
            df_values = create_values_df_from_columns(values, ID_LEVEL)
            df_header = create_header_df(
                header[interval], interval, ID_LEVEL, COLUMN_LEVELS[1:]
            )
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


class RawSqlParser(Parser):
    pass
