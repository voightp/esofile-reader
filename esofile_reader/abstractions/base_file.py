import os
import traceback
from collections import defaultdict
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Dict, Sequence, Optional, Union, List, Tuple

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.convertor import (
    is_daily,
    is_hourly,
    is_timestep,
)
from esofile_reader.df.df_tables import DFTables
from esofile_reader.mini_classes import Variable, SimpleVariable, VariableType, PathLike
from esofile_reader.results_processing.aggregate_results import aggregate_variables
from esofile_reader.results_processing.process_results import get_processed_results
from esofile_reader.search_tree import Tree


def get_file_information(file_path: PathLike) -> Tuple[Path, str, datetime]:
    path = Path(file_path)
    file_name = path.stem
    file_created = datetime.utcfromtimestamp(os.path.getctime(file_path))
    return path, file_name, file_created


class BaseFile:
    """
    Generic class to provide methods to fetch data from result tables.

    Parameters need to be processed externally.

    Attributes
    ----------
    file_path : str or Path
        A full path of the result file.
    file_name : str
        File name identifier.
    file_created : datetime.datetime
        Time and date when of the file generation.
    tables : DFTables
        TableType storage instance.
    search_tree : Tree
        N array tree for efficient id searching.
    file_type : str, default "na"
        Identifier to store original file type.


    """

    ESO = ".eso"
    TOTALS = "totals"
    DIFF = "diff"
    XLSX = ".xlsx"
    CSV = ".csv"
    SQL = ".sql"

    def __init__(
        self,
        file_path: PathLike,
        file_name: str,
        file_created: datetime,
        tables: DFTables,
        search_tree: Tree,
        file_type: str,
    ):
        self.file_path = file_path
        self.file_name = file_name
        self.tables = tables
        self.file_created = file_created
        self.search_tree = search_tree
        self.file_type = file_type

    def __repr__(self):
        return (
            f"File: {self.file_name}"
            f"\n\tClass: {self.__class__.__name__}"
            f"\n\tPath: {self.file_path}"
            f"\n\tName: {self.file_name}"
            f"\n\tCreated: {self.file_created}"
            f"\n\tAvailable tables: [{', '.join(self.table_names)}]"
        )

    def __copy__(self):
        return type(self)(
            self.file_path,
            self.file_name,
            self.file_created,
            copy(self.tables),
            copy(self.search_tree),
            self.file_type,
        )

    def __eq__(self, other: "BaseFile"):
        return self.tables == other.tables

    @property
    def complete(self) -> bool:
        """ Check if the file has been populated. """
        return self.tables is not None and self.search_tree is not None

    @property
    def table_names(self) -> List[str]:
        """ Get all available tables. """
        return self.tables.get_table_names()

    def is_header_simple(self, table: str) -> bool:
        """ Check if header uses Variable or SimpleVariable data. """
        return self.tables.is_simple(table)

    def get_header_dictionary(self, table: str) -> Dict[int, VariableType]:
        """ Get all variables for given table. """
        return self.tables.get_variables_dct(table)

    def get_header_df(self, table: str) -> pd.DataFrame:
        """ Get all variables for given table. """
        return self.tables.get_variables_df(table)

    def get_special_table(self, table: str) -> pd.DataFrame:
        """ Return the file as a single DataFrame (without special columns). """
        return self.tables.get_special_table(table)

    def get_numeric_table(self, table: str) -> pd.DataFrame:
        """ Return the file as a single DataFrame (without special columns). """
        try:
            df = self.tables.get_numeric_table(table)
        except KeyError:
            raise KeyError(f"Cannot find table: '{table}'.\n{traceback.format_exc()}")
        return df

    def rename(self, name: str) -> None:
        """ Set a new file name. """
        self.file_name = name

    def find_table_id_map(
        self,
        variables: Union[VariableType, List[VariableType], List[int]],
        part_match: bool = False,
    ) -> Dict[str, List[int]]:
        """
        Find variable ids for a list of 'Variables'.

        Parameters
        ----------
        variables : list of {Variable, SimpleVariable int}
            A list of 'Variable' named tuples.
        part_match : bool
            Only substring of the part of variable is enough
            to match when searching for variables if this is True.

        Returns
        -------
        dct
            A dictionary with 'tables' as keys and lists of
            ids as values.

        """
        variables = variables if isinstance(variables, list) else [variables]
        out = defaultdict(list)

        if all(map(lambda x: isinstance(x, (Variable, SimpleVariable, int)), variables)):
            if all(map(lambda x: isinstance(x, (Variable, SimpleVariable)), variables)):
                ids = []
                for variable in variables:
                    ids.extend(self.search_tree.find_ids(variable, part_match=part_match))
            else:
                # all inputs are integers
                ids = variables
            header = self.tables.get_all_variables_df()
            # filter values by id, isin cannot be used as it breaks ids order
            df = header.set_index(ID_LEVEL)
            df = df.loc[ids, [TABLE_LEVEL]]
            df.reset_index(inplace=True)
            grouped = df.groupby(TABLE_LEVEL, sort=False, group_keys=False)
            for table, df in grouped:
                out[table] = df[ID_LEVEL].tolist()
        else:
            raise TypeError(
                "Unexpected variable type! This can only be "
                "either integers or 'Variable' / 'SimpleVariable' named tuples."
            )
        return out

    def find_id(
        self, variables: Union[VariableType, List[VariableType]], part_match: bool = False
    ) -> List[int]:
        """ Find ids for a list of 'Variables'. """
        variables = variables if isinstance(variables, list) else [variables]
        ids = []
        for variable in variables:
            ids.extend(self.search_tree.find_ids(variable, part_match=part_match))
        return ids

    def can_convert_rate_to_energy(self, table: str) -> bool:
        """ Check if it's possible to convert rate to energy. """
        try:
            # can convert as there's a 'N DAYS' column
            self.tables.get_special_column(table, N_DAYS_COLUMN)
            return True
        except KeyError:
            # only other option to covert rate to energy is when
            # table reports daily, hourly or timestep results
            index = self.tables.get_datetime_index(table)
            if index is not None:
                return is_daily(index) or is_hourly(index) or is_timestep(index)
        return False

    def _validate_variable_type(
        self, table: str, key: str, units: str, type_: Optional[str] = None
    ) -> VariableType:
        """ Check if an appropriate variable type is requested. """
        var_cls = SimpleVariable if type_ is None else Variable
        table_cls = SimpleVariable if self.is_header_simple(table) else Variable
        if var_cls != table_cls:
            raise TypeError(
                "Cannot create header variable!"
                f" Trying to add {var_cls.__name__} into {table_cls.__name__} table."
            )
        var_args = [table, key, type_, units]
        if var_cls is SimpleVariable:
            var_args.pop(2)
        return var_cls(*var_args)

    def _create_header_variable(
        self, table: str, key: str, units: str, type_: str = None
    ) -> VariableType:
        """ Create unique header variable. """

        def add_num():
            new_key = f"{key} ({i})"
            return self._validate_variable_type(table, new_key, units, type_)

        # check if adding appropriate variable type
        variable = self._validate_variable_type(table, key, units, type_)

        # avoid duplicate variable name
        i = 0
        while self.search_tree.variable_exists(variable):
            i += 1
            variable = add_num()

        return variable

    def rename_variable(
        self,
        variable: VariableType,
        new_key: Optional[str] = None,
        new_type: Optional[str] = None,
    ) -> Tuple[int, VariableType]:
        """ Rename the given 'Variable' using given names. """
        if new_key is None and new_type is None:
            raise ValueError("Cannot rename variable! Type or key are not specified.")
        else:
            # assign original values if one of new ones is not specified
            table, key, units = variable.table, variable.key, variable.units
            new_key = new_key if new_key is not None else key
            if type(variable) is Variable:
                new_type = new_type if new_type is not None else variable.type

            ids = self.find_id(variable)
            if len(ids) == 1:
                id_ = ids[0]
                # create new variable and add it into tree
                new_variable = self._create_header_variable(
                    table, new_key, units, type_=new_type
                )

                # remove current item to avoid item duplicity
                self.search_tree.remove_variables(variable)
                self.search_tree.add_variable(id_, new_variable)

                # rename variable in data set
                if type(variable) is Variable:
                    self.tables.update_variable_name(
                        table, id_, new_variable.key, new_variable.type
                    )
                else:
                    self.tables.update_variable_name(table, id_, new_variable.key)
                return id_, new_variable
            elif len(ids) > 1:
                raise KeyError(
                    f"Cannot rename variable! Too many ids found for variable {variable}"
                )
            else:
                raise KeyError(f"Cannot rename variable! {variable} not found.")

    def insert_variable(
        self, table: str, key: str, units: str, array: Sequence, type_: str = None
    ) -> Optional[Tuple[int, VariableType]]:
        """ Add specified output variable to the file. """
        new_variable = self._create_header_variable(table, key, units, type_=type_)
        id_ = self.tables.insert_column(new_variable, array)
        if id_:
            self.search_tree.add_variable(id_, new_variable)
            return id_, new_variable

    def get_results(self, *args, **kwargs):
        return get_processed_results(self, *args, **kwargs)

    def aggregate_variables(self, *args, **kwargs):
        return aggregate_variables(self, *args, **kwargs)

    def remove_variables(
        self, variables: Union[VariableType, List[VariableType]]
    ) -> Dict[str, List[int]]:
        """ Remove given variables from the file. """
        groups = self.find_table_id_map(
            variables if isinstance(variables, list) else [variables]
        )
        for table, ids in groups.items():
            self.tables.delete_variables(table, ids)
        self.search_tree.remove_variables(variables)
        return groups

    def to_excel(self, path: PathLike, **kwargs) -> None:
        """ Save file as excel. """
        with pd.ExcelWriter(path) as writer:
            for table_name, df in self.tables.items():
                df.to_excel(writer, sheet_name=table_name, **kwargs)
