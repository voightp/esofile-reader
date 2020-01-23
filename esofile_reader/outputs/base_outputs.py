from abc import ABC, abstractmethod
from typing import Sequence, List, Dict
from datetime import datetime
from esofile_reader.utils.mini_classes import Variable
import pandas as pd


class BaseOutputs(ABC):
    """
    An abstract class to define metadata for result storage.

    """

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
    def get_header_df(self, interval: str) -> pd.DataFrame:
        """ Get header information from a single interval."""
        pass

    @abstractmethod
    def get_all_header_dfs(self) -> pd.DataFrame:
        """ Get header information from all intervals as a single df. """
        pass

    @abstractmethod
    def rename_variable(self, interval: str, id_, key_name, var_name) -> None:
        """ Rename given variable. """
        pass

    @abstractmethod
    def add_variable(self, variable: str, array: Sequence) -> None:
        """ Add a new output into specific result table. """
        pass

    @abstractmethod
    def remove_variables(self, interval: str, ids: Sequence[int]) -> None:
        """ Remove given variables. """
        pass

    @abstractmethod
    def get_number_of_days(self, interval: str, start_date: datetime = None,
                           end_date: datetime = None) -> pd.Series:
        """ Get special 'n days' column. """
        pass

    @abstractmethod
    def get_days_of_week(self, interval: str, start_date: datetime = None,
                         end_date: datetime = None) -> pd.Series:
        """ Get special 'day' column. """
        pass

    @abstractmethod
    def get_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                    end_date: datetime = None, include_day: bool = False) -> pd.DataFrame:
        """ Get pd.DataFrame results for given variables. """
        pass

    @abstractmethod
    def get_global_max_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        """ Get pd.DataFrame max results for given variables. """
        pass

    @abstractmethod
    def get_global_min_results(self, interval: str, ids: Sequence[int], start_date: datetime = None,
                               end_date: datetime = None) -> pd.DataFrame:
        """ Get pd.DataFrame min results for given variables. """
        pass
