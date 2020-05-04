from abc import ABC, abstractmethod
from datetime import datetime
from typing import Sequence, List, Dict

import pandas as pd

from esofile_reader.constants import DAY_COLUMN, N_DAYS_COLUMN
from esofile_reader.mini_classes import Variable


class BaseData(ABC):
    """
    An abstract class to define metadata for result storage.

    """

    SPECIAL_COLUMNS = [DAY_COLUMN, N_DAYS_COLUMN]

    @abstractmethod
    def get_available_intervals(self) -> List[str]:
        """ Store table in database. """
        pass

    @abstractmethod
    def get_datetime_index(self, interval: str) -> List[datetime]:
        """ Store table in database. """
        pass

    @abstractmethod
    def get_variables_dct(self, interval: str) -> Dict[int, Variable]:
        """ Get a dict of id: variables pairs for given interval. """
        pass

    @abstractmethod
    def get_all_variables_dct(self) -> Dict[str, Dict[int, Variable]]:
        """ Get a dict of id: variables for all intervals. """
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
    def get_variables_df(self, interval: str) -> pd.DataFrame:
        """ Get header information from a single interval."""
        pass

    @abstractmethod
    def get_all_variables_df(self) -> pd.DataFrame:
        """ Get header information from all intervals as a single df. """
        pass

    @abstractmethod
    def update_variable_name(
            self, interval: str, id_: int, new_key: str, new_type: str
    ) -> None:
        """ Rename given variable. """
        pass

    @abstractmethod
    def insert_variable(self, variable: Variable, array: Sequence) -> None:
        """ Add a new output into specific result table. """
        pass

    @abstractmethod
    def update_variable_results(self, interval: str, id_: int, array: Sequence[float]):
        """ Update given variable values. """
        pass

    @abstractmethod
    def delete_variables(self, interval: str, ids: Sequence[int]) -> None:
        """ Remove given variables. """
        pass

    @abstractmethod
    def get_number_of_days(
            self, interval: str, start_date: datetime = None, end_date: datetime = None
    ) -> pd.Series:
        """ Get special 'n days' column. """
        pass

    @abstractmethod
    def get_days_of_week(
            self, interval: str, start_date: datetime = None, end_date: datetime = None
    ) -> pd.Series:
        """ Get special 'day' column. """
        pass

    @abstractmethod
    def get_all_results(self, interval: str) -> pd.DataFrame:
        """ Get numeric outputs without special columns. """
        pass

    @abstractmethod
    def get_results(
            self,
            interval: str,
            ids: Sequence[int],
            start_date: datetime = None,
            end_date: datetime = None,
            include_day: bool = False,
    ) -> pd.DataFrame:
        """ Get pd.DataFrame results for given variables. """
        pass

    @abstractmethod
    def get_global_max_results(
            self,
            interval: str,
            ids: Sequence[int],
            start_date: datetime = None,
            end_date: datetime = None,
    ) -> pd.DataFrame:
        """ Get pd.DataFrame max results for given variables. """
        pass

    @abstractmethod
    def get_global_min_results(
            self,
            interval: str,
            ids: Sequence[int],
            start_date: datetime = None,
            end_date: datetime = None,
    ) -> pd.DataFrame:
        """ Get pd.DataFrame min results for given variables. """
        pass
