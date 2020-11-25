import contextlib
from collections import defaultdict
from math import nan
from typing import Tuple, Dict, List, Optional, Union

from esofile_reader.mini_classes import Variable
from esofile_reader.processing.eplus import D, M, A, RP


class RawData:
    def __init__(self, environment_name: str, header: Dict[str, Dict[int, Variable]]):
        self.environment_name = environment_name
        self.header = header
        self.outputs = None
        self.dates = None
        self.cumulative_days = None
        self.days_of_week = None
        self.peak_outputs = None

    @property
    def table_attributes(self):
        return [
            self.header,
            self.outputs,
            self.dates,
            self.days_of_week,
            self.cumulative_days,
            self.peak_outputs,
        ]

    def is_sizing_environment(self) -> bool:
        if self.days_of_week:
            sample_values = next(iter(self.days_of_week.values()))
            return sample_values[0] in ["WinterDesignDay", "SummerDesignDay"]
        else:
            return (
                "summer design day" in self.environment_name.lower()
                or "winter design day" in self.environment_name.lower()
            )

    def get_n_tables(self) -> int:
        return (
            len(self.outputs)
            if self.peak_outputs is None
            else len(self.peak_outputs) + len(self.outputs)
        )

    def remove_interval_data(self, intervals: Union[str, List[str]]) -> None:
        for interval in intervals if isinstance(intervals, list) else [intervals]:
            for attr in self.table_attributes:
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
        ) = self.initialize_results_bins(header, ignore_peaks)

    @staticmethod
    def initialize_results_bins(
        header: Dict[str, Dict[int, Variable]], ignore_peaks: bool
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
        for interval, variables in header.items():
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


class RawSqlData(RawData):
    def __init__(
        self,
        environment_name: str,
        header: Dict[str, Dict[int, Variable]],
        outputs: Dict[str, List[Tuple[int, int, float]]],
        dates: Dict[str, List[Tuple[int, ...]]],
        cumulative_days: Dict[str, List[int]],
        days_of_week: Dict[str, List[str]],
    ):
        super().__init__(environment_name, header)
        self.environment_name = environment_name
        self.header = header
        self.outputs = outputs
        self.dates = dates
        self.cumulative_days = cumulative_days
        self.days_of_week = days_of_week
