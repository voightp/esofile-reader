import contextlib
from collections import defaultdict
from math import nan
from typing import Dict, Tuple, Optional, List

from esofile_reader.constants import *
from esofile_reader.df.df_tables import DFTables
from esofile_reader.exceptions import DuplicateVariable
from esofile_reader.mini_classes import Variable
from esofile_reader.processing.esofile_time import convert_raw_date_data, get_n_days
from esofile_reader.processing.progress_logger import (
    EsoFileProgressLogger,
    GenericProgressLogger,
)
from esofile_reader.search_tree import Tree

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


class RawEsoData:
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

    @classmethod
    def sanitize_output_data(cls, all_raw_outputs: List["RawEsoData"]):
        """ Remove invalid data. """
        for raw_outputs in all_raw_outputs:
            if raw_outputs.is_sizing_environment():
                for interval in (M, A, RP):
                    raw_outputs.remove_interval_data(interval)
        return all_raw_outputs

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

    def remove_interval_data(self, interval: str) -> None:
        attributes = [
            self.header,
            self.outputs,
            self.peak_outputs,
            self.dates,
            self.cumulative_days,
            self.days_of_week,
        ]
        for attr in attributes:
            with contextlib.suppress(KeyError):
                del attr[interval]

    def is_sizing_environment(self) -> bool:
        if self.days_of_week:
            sample_values = next(iter(self.days_of_week.values()))
            return sample_values[0] in ["WinterDesignDay", "SummerDesignDay"]
        else:
            return (
                "summer design day" in self.environment_name.lower()
                or "winter design day" in self.environment_name.lower()
            )


class RawSqlData:
    def __init__(
        self,
        environment_name: str,
        header: Dict[str, Dict[int, Variable]],
        outputs,
        dates: Dict[str, List[Tuple[int, ...]]],
        n_interval_minutes: Dict[str, List[int]],
        days_of_week: Dict[str, List[str]],
    ):
        self.environment_name = environment_name
        self.header = header
        self.outputs = outputs
        self.dates = dates
        self.n_interval_minutes = n_interval_minutes
        self.days_of_week = days_of_week


class RawDFData:
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
    def from_raw_eso_data(
        cls,
        raw_eso_data: RawEsoData,
        progress_logger: EsoFileProgressLogger,
        year: Optional[int],
    ) -> "RawDFData":
        # Create a 'search tree' to allow searching for variables
        progress_logger.log_section("generating search tree!")
        try:
            tree = Tree.from_header_dict(raw_eso_data.header)
        except DuplicateVariable as e:
            tree = e.clean_tree
            remove_duplicates(
                e.duplicates,
                raw_eso_data.header,
                raw_eso_data.outputs,
                raw_eso_data.peak_outputs,
            )
        progress_logger.increment_progress()
        progress_logger.log_section("processing dates!")
        n_days = get_n_days(raw_eso_data.dates, raw_eso_data.cumulative_days)
        dates = convert_raw_date_data(raw_eso_data.dates, raw_eso_data.days_of_week, year)

        if raw_eso_data.peak_outputs:
            progress_logger.log_section("generating peak tables!")
            peak_tables = generate_peak_tables(
                raw_eso_data.peak_outputs, raw_eso_data.header, dates, progress_logger
            )
        else:
            peak_tables = None
        progress_logger.log_section("generating tables!")
        tables = generate_df_tables(
            raw_eso_data.outputs, raw_eso_data.header, dates, progress_logger
        )
        other_data = {N_DAYS_COLUMN: n_days, DAY_COLUMN: raw_eso_data.days_of_week}
        insert_special_columns(tables, other_data)
        return RawDFData(raw_eso_data.environment_name, tables, peak_tables, tree)

    @classmethod
    def from_raw_sql_data(
        cls, raw_sql_data: RawSqlData, progress_logger: GenericProgressLogger
    ) -> "RawDFData":
        pass
