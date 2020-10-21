from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, TextIO, Optional, Union

from esofile_reader.mini_classes import Variable, IntervalTuple
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.processing.raw_outputs import RawOutputData, RawOutputDFData


def get_eso_file_version(raw_version: str) -> int: ...


def get_eso_file_timestamp(timestamp: str) -> datetime: ...


def process_statement_line(line: str) -> Tuple[int, datetime]: ...


def process_header_line(line: str) -> Tuple[int, str, str, str, str]: ...


def read_header(
    eso_file: TextIO, progress_logger: EsoFileProgressLogger
) -> Dict[str, Dict[int, Variable]]: ...


def process_sub_monthly_interval_line(
    line_id: int, data: List[str]
) -> Tuple[str, IntervalTuple, str]: ...


def process_monthly_plus_interval_line(
    line_id: int, data: List[str]
) -> Tuple[str, IntervalTuple, Optional[int]]: ...


def read_body(
    eso_file: TextIO,
    highest_interval_id: int,
    header: Dict[str, Dict[int, Variable]],
    ignore_peaks: bool,
    progress_logger: EsoFileProgressLogger
) -> List[RawOutputData]: ...


def count_tables(all_raw_outputs: List[RawOutputData]) -> int: ...


def process_raw_file_content(
    all_raw_outputs: List[RawOutputData],
    year: int,
    progress_logger: EsoFileProgressLogger
) -> List[RawOutputData]: ...


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
) -> List[RawOutputDFData]: ...
