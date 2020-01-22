from abc import ABC, abstractmethod
from typing import Sequence, List, Dict
from datetime import datetime
from esofile_reader.utils.mini_classes import Variable
import pandas as pd


class BaseOutputs(ABC):
    @abstractmethod
    def set_data(self, interval: str, df: pd.DataFrame):
        """ Store table in database. """
        pass

    @abstractmethod
    def get_available_intervals(self) -> List[str]:
        """ Store table in database. """
        pass

    @abstractmethod
    def get_variables(self, interval: str) -> Dict[int, Variable]:
        """ Get list of variables for given interval. """
        pass

    @abstractmethod
    def get_variable_ids(self, interval: str) -> List[int]:
        """ Get all variable ids for given interval. """
        pass

    @abstractmethod
    def get_all_variable_ids(self) -> List[int]:
        """ Get all variable ids. """
        pass

    @abstractmethod
    def get_all_header_dfs(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_header_df(self, interval: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def rename_variable(self, interval: str, id_, key_name, var_name) -> None:
        pass

    @abstractmethod
    def add_variable(self, variable: str, array: Sequence) -> None:
        pass

    @abstractmethod
    def remove_variables(self, interval: str, ids: Sequence[int]) -> None:
        pass

    @abstractmethod
    def get_number_of_days(self, interval: str, start_date: datetime = None,
                           end_date: datetime = None) -> pd.Series:
        pass

    @abstractmethod
    def get_days_of_week(self, interval: str, start_date: datetime = None,
                         end_date: datetime = None) -> pd.Series:
        pass

    @abstractmethod
    def get_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                    end_date: datetime = None, include_day: bool = False) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_global_max_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_global_min_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        pass
