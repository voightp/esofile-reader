import contextlib
import logging
from collections import defaultdict
from math import nan
from typing import Tuple, Dict, List, Optional

from esofile_reader.constants import *
from esofile_reader.mini_classes import Variable


class RawData:
    def __init__(self, environment_name: str, header: Dict[str, Dict[int, Variable]]):
        self.environment_name = environment_name
        self.header = header
        self.outputs = None
        self.dates = None
        self.days_of_week = None
        self.peak_outputs = None

    def sanitize(self):
        """ Remove invalid data. """
        if self.is_sizing_environment():
            for interval in (M, A, RP):
                self.remove_interval_data(interval)

    def is_sizing_environment(self) -> bool:
        if self.days_of_week:
            sample_values = next(iter(self.days_of_week.values()))
            return sample_values[0] in ["WinterDesignDay", "SummerDesignDay"]
        else:
            return (
                "summer design day" in self.environment_name.lower()
                or "winter design day" in self.environment_name.lower()
            )

    def remove_variables(self, variables: Dict[int, Variable]) -> None:
        """ Remove duplicate outputs from results set. """
        for id_, v in variables.items():
            logging.info(f"Duplicate variable found, removing variable: '{id_} - {v}'.")
            for dct in [self.header, self.outputs, self.peak_outputs]:
                if dct:
                    with contextlib.suppress(KeyError):
                        del dct[v.table][id_]

    def get_n_tables(self) -> int:
        return len(self.outputs) + 0 if self.peak_outputs is None else len(self.peak_outputs)

    def remove_interval_data(self, interval: str) -> None:
        attributes = [
            self.header,
            self.outputs,
            self.peak_outputs,
            self.dates,
            self.days_of_week,
        ]
        for attr in attributes:
            with contextlib.suppress(KeyError, TypeError):
                del attr[interval]


class RawEsoData(RawData):
    def __init__(
        self, environment_name: str, header: Dict[str, Dict[int, Variable]], ignore_peaks: bool,
    ):
        super().__init__(environment_name, header)
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

    def remove_interval_data(self, interval: str) -> None:
        super().remove_interval_data(interval)
        with contextlib.suppress(KeyError, TypeError):
            del self.cumulative_days[interval]


class RawSqlData(RawData):
    def __init__(
        self,
        environment_name: str,
        header: Dict[str, Dict[int, Variable]],
        outputs: Dict[str, List[Tuple[int, int, float]]],
        dates: Dict[str, List[Tuple[int, ...]]],
        n_minutes: Dict[str, List[int]],
        days_of_week: Dict[str, List[str]],
    ):
        super().__init__(environment_name, header)
        self.environment_name = environment_name
        self.header = header
        self.outputs = outputs
        self.dates = dates
        self.n_minutes = n_minutes
        self.days_of_week = days_of_week

    def remove_interval_data(self, interval: str) -> None:
        super().remove_interval_data(interval)
        with contextlib.suppress(KeyError, TypeError):
            del self.n_minutes[interval]

    def sanitize(self):
        super().sanitize()
        for interval, variables in self.header.items():
            ids = {r[1] for r in self.outputs[interval]}
            missing_ids = set(variables).difference(ids)
            for id_ in missing_ids:
                del self.header[interval][id_]
