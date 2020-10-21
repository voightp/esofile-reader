from collections import defaultdict
from copy import deepcopy
from math import nan
from typing import Dict, Tuple, Optional

from esofile_reader.constants import *
from esofile_reader.exceptions import DuplicateVariable
from esofile_reader.mini_classes import Variable
from esofile_reader.processing.esofile_intervals import process_raw_date_data
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.search_tree import Tree
from esofile_reader.df.df_tables import DFTables

try:
    from esofile_reader.processing.extensions.raw_tables import (
        generate_peak_tables,
        generate_df_tables,
        insert_special_columns,
        remove_duplicates,
    )
except ModuleNotFoundError:
    import pyximport

    pyximport.install(pyximport=True, language_level=3)
    from esofile_reader.processing.extensions.raw_tables import (
        generate_peak_tables,
        generate_df_tables,
        insert_special_columns,
    )


class RawOutputData:
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
            v.append(nan)

    def initialize_next_peak_outputs_step(self, interval: str) -> None:
        for v in self.peak_outputs[interval].values():
            v.append(nan)

    def get_n_tables(self):
        return len(self.outputs) + 0 if self.peak_outputs is None else len(self.peak_outputs)


class RawOutputDFData:
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
        cls, raw_outputs: RawOutputData, progress_logger: EsoFileProgressLogger, year: int
    ) -> "RawOutputDFData":
        # Create a 'search tree' to allow searching for variables
        progress_logger.log_section("generating search tree!")
        header = deepcopy(raw_outputs.header)
        try:
            tree = Tree.from_header_dict(header)
        except DuplicateVariable as e:
            tree = e.clean_tree
            remove_duplicates(
                e.duplicates, header, raw_outputs.outputs, raw_outputs.peak_outputs
            )
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
        return RawOutputDFData(raw_outputs.environment_name, tables, peak_tables, tree)
