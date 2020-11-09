from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, TextIO, Optional, Union

from esofile_reader.mini_classes import Variable, EsoTimestamp
from esofile_reader.processing.progress_logger import GenericLogger
from esofile_reader.processing.raw_data import RawEsoData


def get_eso_file_version(raw_version: str) -> int: ...


def get_eso_file_timestamp(timestamp: str) -> datetime: ...


def process_statement_line(line: str) -> Tuple[int, datetime]: ...


def process_header_line(line: str) -> Tuple[int, str, str, str, str]: ...


def read_header(
    eso_file: TextIO, progress_logger: GenericLogger
) -> Dict[str, Dict[int, Variable]]: ...


def process_sub_monthly_interval_line(
    line_id: int, data: List[str]
) -> Tuple[str, EsoTimestamp, str]: ...


def process_monthly_plus_interval_line(
    line_id: int, data: List[str]
) -> Tuple[str, EsoTimestamp, Optional[int]]: ...


def read_body(
    eso_file: TextIO,
    highest_interval_id: int,
    header: Dict[str, Dict[int, Variable]],
    ignore_peaks: bool,
    progress_logger: GenericLogger
) -> List[RawEsoData]: ...


def read_file(
    file: TextIO, progress_logger: GenericLogger, ignore_peaks: bool = True
) -> List[RawEsoData]: ...


def count_lines(file_path: Union[str, Path]) -> int: ...


def preprocess_file(
    file_path: Union[str, Path], progress_logger: GenericLogger
) -> None: ...


def process_eso_file(
    file_path: Union[str, Path],
    progress_logger: GenericLogger,
    ignore_peaks: bool = True,
) -> List[RawEsoData]: ...
