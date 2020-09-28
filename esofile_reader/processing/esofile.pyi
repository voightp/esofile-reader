from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, TextIO, Optional, Union

import pandas as pd

from esofile_reader.mini_classes import Variable, IntervalTuple
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.search_tree import Tree
from esofile_reader.tables.df_tables import DFTables


def get_eso_file_version(raw_version: str) -> int: ...


def get_eso_file_timestamp(timestamp: str) -> datetime: ...


def process_statement_line(line: str) -> Tuple[int, datetime]: ...


def process_header_line(line: str) -> Tuple[int, str, str, str, str]: ...


def read_header(
    eso_file: TextIO, progress_logger: EsoFileProgressLogger
) -> Dict[str, Dict[int, Variable]]: ...


def process_sub_monthly_interval_lines(
    line_id: int, data: List[str]
) -> Tuple[str, IntervalTuple, str]: ...


def process_monthly_plus_interval_lines(
    line_id: int, data: List[str]
) -> Tuple[str, IntervalTuple, Optional[int]]: ...


def read_body(
    eso_file: TextIO,
    highest_interval_id: int,
    header: Dict[str, Dict[int, Variable]],
    ignore_peaks: bool,
    progress_logger: EsoFileProgressLogger
) -> Tuple[
    List[str],
    List[Dict[str, dict]],
    List[Optional[Dict[str, dict]]],
    List[Dict[str, list]],
    List[Dict[str, list]],
    List[Dict[str, list]],
    Dict[str, Dict[int, Variable]]
]: ...


def create_values_df(outputs_dct: Dict[int, List[float]], index_name: str) -> pd.DataFrame: ...


def create_header_df(
    header: Dict[int, Variable], interval: str, index_name: str, columns: List[str]
) -> pd.DataFrame: ...


def generate_peak_tables(
    raw_peak_outputs: Dict[str, Dict[int, List[float]]],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str,],
    progress_logger: EsoFileProgressLogger,
) -> Dict[str, DFTables]: ...


def generate_df_tables(
    raw_outputs: Dict[str, Dict[int, List[float]]],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str,],
    other_data: Dict[str, Dict[str, list]],
    progress_logger: EsoFileProgressLogger,
) -> DFTables: ...


def remove_duplicates(
    duplicates: Dict[int, Variable],
    header: Dict[str, Dict[int, Variable]],
    outputs: Dict[str, Dict[int, List[float]]]
) -> None: ...


def count_tables(outputs: List[Optional[Dict[str, dict]]]) -> int: ...


def process_file_content(
    all_outputs: List[Dict[str, dict]],
    all_peak_outputs: List[Optional[Dict[str, dict]]],
    all_dates: List[Dict[str, list]],
    all_cumulative_days: List[Dict[str, list]],
    all_days_of_week: List[Dict[str, list]],
    original_header: Dict[str, Dict[int, Variable]],
    year: int,
    progress_logger: EsoFileProgressLogger
) -> Tuple[List[DFTables], List[Optional[Dict[str, DFTables]]], List[Tree]]: ...


def read_file(
    file: TextIO, progress_logger: EsoFileProgressLogger, ignore_peaks: bool = True
) -> Tuple[
    List[str],
    List[Dict[str, dict]],
    List[Optional[Dict[str, dict]]],
    List[Dict[str, list]],
    List[Dict[str, list]],
    List[Dict[str, list]],
    Dict[str, Dict[int, Variable]]
]: ...


def count_lines(file_path: Union[str, Path]): ...


def preprocess_file(
    file_path: Union[str, Path], progress_logger: EsoFileProgressLogger
) -> None: ...


def process_eso_file(
    file_path: Union[str, Path],
    progress_logger: EsoFileProgressLogger,
    ignore_peaks: bool = True,
    year: int = 2002
) -> Tuple[List[str], List[DFTables], List[Optional[Dict[str, DFTables]]], List[Tree]]: ...
