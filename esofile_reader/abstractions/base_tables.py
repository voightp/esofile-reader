from abc import ABC, abstractmethod
from datetime import datetime
from typing import Sequence, List, Dict, Optional

import pandas as pd

from esofile_reader.typehints import Variable


class BaseTables(ABC):
    """
    An abstract class to define metadata for result storage.

    """

    @abstractmethod
    def __copy__(self):
        pass

    @abstractmethod
    def is_simple(self, table: str) -> bool:
        """ Check whether data uses full or simple variable. """
        pass

    @abstractmethod
    def is_index_datetime(self, table: str) -> bool:
        """ Check if given table uses Datetime index. """
        pass

    @abstractmethod
    def get_levels(self, table: str) -> List[str]:
        """ Get multiindex levels. """
        pass

    @abstractmethod
    def get_table_names(self) -> List[str]:
        """ Get all table names. """
        pass

    @abstractmethod
    def get_datetime_index(self, table: str) -> List[datetime]:
        """ Get datetime index of the given variable. """
        pass

    @abstractmethod
    def get_variables_count(self, table: str) -> int:
        """ Get number of variables in given table. """
        pass

    @abstractmethod
    def get_all_variables_count(self) -> int:
        """ Get total number of variables. """
        pass

    @abstractmethod
    def get_variables_dct(self, table: str) -> Dict[int, Variable]:
        """ Get a dict of id: variables pairs for given table. """
        pass

    @abstractmethod
    def get_all_variables_dct(self) -> Dict[str, Dict[int, Variable]]:
        """ Get a dict of id: variables for all tables. """
        pass

    @abstractmethod
    def get_variable_ids(self, table: str) -> List[int]:
        """ Get all variable ids for given table. """
        pass

    @abstractmethod
    def get_all_variable_ids(self) -> List[int]:
        """ Get all variable ids. """
        pass

    @abstractmethod
    def get_variables_df(self, table: str) -> pd.DataFrame:
        """ Get header information from a single table."""
        pass

    @abstractmethod
    def get_all_variables_df(self) -> pd.DataFrame:
        """ Get header information from all tables as a single df. """
        pass

    @abstractmethod
    def update_variable_name(
        self, table: str, id_: int, new_key: str, new_type: str = ""
    ) -> None:
        """ Rename given variable. """
        pass

    @abstractmethod
    def insert_column(self, variable: Variable, array: Sequence) -> Optional[int]:
        """ Add a new output into specific result table. """
        pass

    @abstractmethod
    def insert_special_column(self, table: str, key: str, array: Sequence) -> None:
        """ Add a 'special' variable into specific results table. """
        pass

    @abstractmethod
    def update_variable_values(self, table: str, id_: int, array: Sequence[float]):
        """ Update given variable values. """
        pass

    @abstractmethod
    def delete_variables(self, table: str, ids: Sequence[int]) -> None:
        """ Remove given variables. """
        pass

    @abstractmethod
    def get_special_column(
        self,
        table: str,
        key: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.Series:
        """ Get a special column. """
        pass

    @abstractmethod
    def get_table(self, table: str) -> pd.DataFrame:
        """ Get full table. """
        pass

    @abstractmethod
    def get_special_table(self, table: str) -> pd.DataFrame:
        """ Get table with only 'special' columns. """
        pass

    @abstractmethod
    def get_numeric_table(self, table: str) -> pd.DataFrame:
        """ Get numeric outputs without special columns. """
        pass

    @abstractmethod
    def add_day_to_index(
        self,
        df: pd.DataFrame,
        table: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> None:
        """ Try to add day of week into index. """
        pass

    @abstractmethod
    def get_results_df(
        self,
        table: str,
        ids: Sequence[int],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        include_day: bool = False,
    ) -> pd.DataFrame:
        """ Get pd.DataFrame results for given variables. """
        pass

    @abstractmethod
    def get_global_max_results_df(
        self,
        table: str,
        ids: Sequence[int],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """ Get pd.DataFrame max results for given variables. """
        pass

    @abstractmethod
    def get_global_min_results_df(
        self,
        table: str,
        ids: Sequence[int],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """ Get pd.DataFrame min results for given variables. """
        pass
