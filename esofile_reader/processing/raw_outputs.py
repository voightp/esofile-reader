import contextlib
import logging
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd

from esofile_reader.constants import *
from esofile_reader.exceptions import DuplicateVariable
from esofile_reader.mini_classes import Variable
from esofile_reader.processing.esofile_intervals import process_raw_date_data
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.search_tree import Tree
from esofile_reader.tables.df_functions import create_peak_min_outputs, create_peak_max_outputs
from esofile_reader.tables.df_tables import DFTables


class RawOutputs:
    def __init__(
        self, environment_name: str, header: Dict[str, Dict[int, Variable]], ignore_peaks: bool,
    ):
        self.environment_name = environment_name
        self.header = header
        (
            self.outputs,
            self.peak_outputs,
            self.dates,
            self.cumulative_days,
            self.days_of_week,
        ) = self.initialize_results_bins(ignore_peaks)

    def initialize_results_bins(
        self, ignore_peaks: bool
    ) -> Tuple[
        Dict[str, Dict[int, list]],
        Optional[Dict[str, Dict[int, list]]],
        Dict[str, list],
        Dict[str, list],
        Dict[str, list],
    ]:
        """ Create bins to be populated when reading file 'body'. """
        outputs = defaultdict(dict)
        peak_outputs = defaultdict(dict)
        dates = {}
        cumulative_days = {}
        days_of_week = {}
        for interval, variables in self.header.items():
            dates[interval] = []
            if interval in (M, A, RP):
                cumulative_days[interval] = []
            else:
                days_of_week[interval] = []
            for id_ in variables.keys():
                outputs[interval][id_] = []
                if not ignore_peaks and interval in (D, M, A, RP):
                    peak_outputs[interval][id_] = []
        return outputs, peak_outputs, dates, cumulative_days, days_of_week

    def initialize_next_outputs_step(self, interval: str) -> None:
        for v in self.outputs[interval].values():
            v.append(np.nan)

    def initialize_next_peak_outputs_step(self, interval: str) -> None:
        for v in self.peak_outputs[interval].values():
            v.append(np.nan)

    def get_n_tables(self):
        return len(self.outputs) + 0 if self.peak_outputs is None else len(self.peak_outputs)


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
    progress_logger: EsoFileProgressLogger,
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
    progress_logger: EsoFileProgressLogger,
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
    tables: DFTables, special_data: Dict[str, Dict[str, List[Any]]]
) -> None:
    """ Add other special columns. """
    for key, dict in special_data.items():
        for interval, arr in dict.items():
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
            with contextlib.suppress(KeyError):
                del dct[v.table][id_]


class RawDFOutputs:
    def __init__(
        self,
        environment_name: str,
        tables: DFTables,
        peak_tables: Optional[Dict[str, DFTables]],
        tree: Tree,
    ):
        self.environment_name = environment_name
        self.tables = tables
        self.peak_tables = peak_tables
        self.tree = tree

    @classmethod
    def from_raw_outputs(
        cls, raw_outputs: RawOutputs, progress_logger: EsoFileProgressLogger, year: int
    ) -> "RawDFOutputs":
        # Create a 'search tree' to allow searching for variables
        progress_logger.log_section("generating search tree!")
        header = deepcopy(raw_outputs.header)
        try:
            tree = Tree.from_header_dict(header)
        except DuplicateVariable as e:
            tree = e.clean_tree
        progress_logger.increment_progress()
        progress_logger.log_section("processing dates!")
        dates, n_days = process_raw_date_data(
            raw_outputs.dates, raw_outputs.cumulative_days, year
        )
        if raw_outputs.peak_outputs:
            progress_logger.log_section("generating peak tables!")
            peak_tables = generate_peak_tables(
                raw_outputs.peak_outputs, header, dates, progress_logger
            )
        else:
            peak_tables = None
        progress_logger.log_section("generating tables!")
        tables = generate_df_tables(raw_outputs.outputs, header, dates, progress_logger)
        other_data = {N_DAYS_COLUMN: n_days, DAY_COLUMN: raw_outputs.days_of_week}
        insert_special_columns(tables, other_data)
        return RawDFOutputs(raw_outputs.environment_name, tables, peak_tables, tree)
