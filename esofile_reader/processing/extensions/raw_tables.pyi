from datetime import datetime
from typing import Dict, List, Optional, Any

from esofile_reader.mini_classes import Variable
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.tables.df_tables import DFTables


def generate_peak_tables(
    raw_peak_outputs: Dict[str, Dict[int, List[float]]],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str, List[datetime]],
    progress_logger: EsoFileProgressLogger,
) -> Dict[str, DFTables]: ...


def generate_df_tables(
    raw_outputs: Dict[str, Dict[int, List[float]]],
    header: Dict[str, Dict[int, Variable]],
    dates: Dict[str, List[datetime]],
    progress_logger: EsoFileProgressLogger,
) -> DFTables: ...


def insert_special_columns(
    tables: DFTables, special_data: Dict[str, Dict[str, List[Any]]]
) -> None: ...


def remove_duplicates(
    duplicates: Dict[int, Variable],
    header: Dict[str, Dict[int, Variable]],
    outputs: Dict[str, Dict[int, List[float]]],
    peak_outputs: Optional[Dict[str, Dict[int, List[float]]]],
) -> None: ...
