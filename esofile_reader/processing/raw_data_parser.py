import contextlib
import math
from abc import abstractmethod, ABC
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple, Callable, Set

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.df.df_functions import create_peak_min_outputs, create_peak_max_outputs
from esofile_reader.df.df_tables import DFTables
from esofile_reader.exceptions import FormatNotSupported
from esofile_reader.mini_classes import Variable
from esofile_reader.processing.esofile_time import convert_raw_date_data
from esofile_reader.processing.progress_logger import GenericLogger
from esofile_reader.processing.raw_data import RawData
from esofile_reader.processing.sql import process_sql_file
from esofile_reader.processing.sql_time import convert_raw_sql_date_data

try:
    from esofile_reader.processing.extensions.esofile import process_eso_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install(pyximport=True, language_level=3)
    from esofile_reader.processing.extensions.esofile import process_eso_file


def align_id_level(df: pd.DataFrame, id_level: pd.Index):
    return df.loc[:, id_level]


def create_header_multiindex(
    header: Dict[int, Variable], outputs_ids: Set[int], names: List[str]
) -> pd.MultiIndex:
    """ Create sorted header MultiIndex. """
    tuples = []
    for id_, variable in header.items():
        if id_ in outputs_ids:
            tuples.append((id_, *variable))
        else:
            import logging

            logging.error(f"Variable: '{id_}' - {variable} is not included in outputs!")
    return pd.MultiIndex.from_tuples(tuples, names=names)


def create_df_from_columns(outputs_dct: Dict[int, List[float]]) -> pd.DataFrame:
    """ Create plain values pd.DataFrame from dictionary. """
    return pd.DataFrame(outputs_dct, dtype=float)


def create_df_from_rows(outputs_rows: List[Tuple[int, int, float]]) -> pd.DataFrame:
    """ Create pd.DataFrame from list of rows. """
    df = pd.DataFrame(outputs_rows, columns=[TIMESTAMP_COLUMN, ID_LEVEL, VALUE_LEVEL])
    return pd.pivot_table(
        df, values=VALUE_LEVEL, index=TIMESTAMP_COLUMN, columns=ID_LEVEL, fill_value=math.nan
    )


def insert_special_columns(
    tables: DFTables, special_data: Dict[str, Optional[Dict[str, List[Any]]]]
) -> None:
    """ Insert special columns (n days, days of week) into given tables. """
    for key, dct in special_data.items():
        if dct:
            for interval, arr in dct.items():
                tables.insert_special_column(interval, key, arr)


def choose_parser(file_path: Path) -> Union["RawEsoParser", "RawSqlParser"]:
    """ Select relevant parser for given file path. """
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


def find_duplicate_variables(variables: Dict[int, Variable]) -> Dict[int, Variable]:
    """ Get dupicate id : variable pairs (original is not returned). """
    rev_dict = defaultdict(list)
    for id_, variable in variables.items():
        rev_dict[variable].append(id_)
    nested_ids = list(filter(lambda v: len(v) > 1, rev_dict.values()))
    return {id_: variables[id_] for ids in nested_ids for id_ in ids[1:]}


def get_unique_variable(variable: Variable, all_variables: Set[Variable]) -> Variable:
    """ Create unique variable using given check list. """

    def create_new_variable():
        new_key = f"{variable.key} ({i})"
        return Variable(variable.table, new_key, variable.type, variable.units)

    i = 1
    new_variable = create_new_variable()
    while new_variable in all_variables:
        i += 1
        new_variable = create_new_variable()
    return new_variable


def update_duplicate_names(header: Dict[str, Dict[int, Variable]]) -> None:
    """ Update possible duplicate variables. """
    for variables in header.values():
        duplicates = find_duplicate_variables(variables)
        for id_, duplicate in duplicates.items():
            new_variable = get_unique_variable(duplicate, set(variables.values()))
            variables[id_] = new_variable


def remove_interval_data(raw_data: RawData, intervals: Union[str, List[str]]) -> None:
    """ Delete all tables of given interval in given raw data. """
    for interval in intervals if isinstance(intervals, list) else [intervals]:
        for attr in raw_data.table_attributes:
            with contextlib.suppress(KeyError, TypeError):
                del attr[interval]


def _cast_to_df(
    df_func: Callable,
    outputs: Dict[str, Any],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str, List[datetime]],
    special_columns: Dict[str, Dict[str, List[Union[str, int]]]],
    progress_logger: GenericLogger,
) -> DFTables:
    """ Create pd.DataFrame tables from plain data structures. """
    tables = DFTables()
    for interval, values in outputs.items():
        df = df_func(values)
        mi = create_header_multiindex(header[interval], set(df.columns), COLUMN_LEVELS)
        df = align_id_level(df, mi.get_level_values(ID_LEVEL))
        df.columns = mi
        df.index = pd.Index(dates[interval], name=TIMESTAMP_COLUMN)
        df = df.dropna(axis=1, how="all")
        tables[interval] = df
        progress_logger.increment_progress()
    insert_special_columns(tables, special_columns)
    return tables


class Parser(ABC):
    @staticmethod
    @abstractmethod
    def process_file(file_path: Path, progress_logger: GenericLogger, ignore_peaks: bool):
        pass

    @staticmethod
    @abstractmethod
    def cast_to_datetime(raw_data: RawData, year: int) -> Dict[str, List[datetime]]:
        pass

    @staticmethod
    @abstractmethod
    def cast_to_df(
        outputs: Dict[str, Any],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        special_columns: Dict[str, Dict[str, List[Union[str, int]]]],
        progress_logger: GenericLogger,
    ) -> DFTables:
        pass

    @staticmethod
    @abstractmethod
    def cast_peak_to_df(
        peak_outputs: Dict[str, Any],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        progress_logger: GenericLogger,
    ):
        pass

    @staticmethod
    def sanitize(raw_data: RawData) -> None:
        if raw_data.is_sizing_environment():
            remove_interval_data(raw_data, [M, A, RP])
        update_duplicate_names(raw_data.header)


class RawEsoParser(Parser):
    @staticmethod
    def process_file(file_path, progress_logger, ignore_peaks):
        return process_eso_file(file_path, progress_logger, ignore_peaks=ignore_peaks)

    @staticmethod
    def cast_to_datetime(raw_eso_data, year):
        return convert_raw_date_data(raw_eso_data.dates, raw_eso_data.days_of_week, year)

    @staticmethod
    def cast_to_df(
        outputs: Dict[str, Dict[int, List[float]]],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        special_columns: Dict[str, Dict[str, List[Union[str, int]]]],
        progress_logger: GenericLogger,
    ) -> DFTables:
        return _cast_to_df(
            create_df_from_columns, outputs, header, dates, special_columns, progress_logger
        )

    @staticmethod
    def cast_peak_to_df(
        peak_outputs: Dict[str, Dict[int, List[float]]],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        progress_logger: GenericLogger,
    ):
        min_peaks = DFTables()
        max_peaks = DFTables()
        for interval, values in peak_outputs.items():
            df = create_df_from_columns(values)
            mi = create_header_multiindex(header[interval], set(df.columns), COLUMN_LEVELS)
            df = align_id_level(df, mi.get_level_values(ID_LEVEL))
            df.columns = mi
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
    @staticmethod
    def process_file(file_path, progress_logger, ignore_peaks):
        return process_sql_file(file_path, progress_logger)

    @staticmethod
    def cast_to_datetime(raw_sql_data, year):
        return convert_raw_sql_date_data(raw_sql_data.dates)

    @staticmethod
    def cast_to_df(
        outputs: Dict[str, List[Tuple[int, int, float]]],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        special_columns: Dict[str, Dict[str, List[Union[str, int]]]],
        progress_logger: GenericLogger,
    ) -> DFTables:
        return _cast_to_df(
            create_df_from_rows, outputs, header, dates, special_columns, progress_logger
        )

    @staticmethod
    def cast_peak_to_df(
        peak_outputs: Dict[str, Dict[int, List[float]]],
        header: Dict[str, Dict[int, Variable]],
        dates: Dict[str, List[datetime]],
        progress_logger: GenericLogger,
    ):
        # sql outputs do not support peaks
        return None
