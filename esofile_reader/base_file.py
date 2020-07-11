import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Sequence, Callable, Optional
from typing import Union, List, Tuple

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.convertor import (
    is_rate_or_energy,
    convert_rate_to_energy,
    convert_units,
    is_daily,
    is_hourly,
    is_timestep
)
from esofile_reader.exceptions import *
from esofile_reader.logger import logger
from esofile_reader.mini_classes import Variable, SimpleVariable
from esofile_reader.processing.esofile_intervals import update_dt_format
from esofile_reader.search_tree import Tree
from esofile_reader.tables.df_tables import DFTables

VariableType = Union[SimpleVariable, Variable]


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
        Data storage instance.
    search_tree : Tree
        N array tree for efficient id searching.
    file_type : str, default "na"
        Identifier to store original file type.


    """

    def __init__(
            self,
            file_path: Union[str, Path],
            file_name: str,
            file_created: datetime,
            tables: DFTables,
            search_tree: Tree,
            file_type: str = "na",
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

    def _add_file_name(self, df: pd.DataFrame, name_position: str) -> pd.DataFrame:
        """ Add file name to index. """
        pos = ["row", "column", "None"]  # 'None' is here only to inform
        if name_position not in pos:
            name_position = "row"
            logger.warning(
                f"Invalid name position!\n'add_file_name' kwarg must "
                f"be one of: '{', '.join(pos)}'.\nSetting 'row'."
            )

        axis = 0 if name_position == "row" else 1

        return pd.concat([df], axis=axis, keys=[self.file_name], names=["file"])

    def _merge_frame(
            self,
            frames: List[pd.DataFrame],
            timestamp_format: str = "default",
            add_file_name: str = "row",
    ) -> pd.DataFrame:
        """ Merge result DataFrames into a single one. """
        if frames:
            # Catch empty frames exception
            df = pd.concat(frames, axis=1, sort=False)

            if add_file_name:
                df = self._add_file_name(df, add_file_name)

            if timestamp_format != "default":
                df = update_dt_format(df, timestamp_format)

            return df
        else:
            logger.warning(
                f"Any of requested variables is not "
                f"included in the results file '{self.file_name}'."
            )

    def _find_pairs(
            self, variables: Union[VariableType, List[VariableType], List[int]],
            part_match: bool = False
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
            self,
            variables: Union[VariableType, List[VariableType]],
            part_match: bool = False
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
            return is_daily(index) or is_hourly(index) or is_timestep(index)

    def get_results(
            self,
            variables: Union[VariableType, List[VariableType], int, List[int]],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            output_type: str = "standard",
            add_file_name: str = "row",
            include_table_name: bool = False,
            include_day: bool = False,
            include_id: bool = False,
            part_match: bool = False,
            units_system: str = "SI",
            rate_units: str = "W",
            energy_units: str = "J",
            timestamp_format: str = "default",
            rate_to_energy_dct: Dict[str, bool] = RATE_TO_ENERGY_DCT,
    ) -> pd.DataFrame:
        """
        Return a pandas.DataFrame object with results for given variables.

        This function extracts requested set of outputs from the file
        and converts to specified units if requested.

        Parameters
        ----------
        variables : Variable or list of (Variable)
            Requested variables.
        start_date : datetime like object, default None
            A start date for requested results.
        end_date : datetime like object, default None
            An end date for requested results.
        output_type : {'standard', global_max','global_min'}
            Requested type_ of results.
        add_file_name : ('row','column',None)
            Specify if file name should be added into results df.
        include_table_name : bool
            Decide if 'table' information should be included on
            the results df.
        include_day : bool
            Add day of week into index, this is applicable only for 'timestep',
            'hourly' and 'daily' outputs.
        include_id : bool
            Decide if variable 'id' should be included on the results df.
        part_match : bool
            Only substring of the part of variable is enough
            to match when searching for variables if this is True.
        units_system : {'SI', 'IP'}
            Selected units type_ for requested outputs.
        rate_to_energy_dct : dct
            Defines if 'rate' will be converted to energy.
        rate_units : {'W', 'kW', 'MW', 'Btu/h', 'kBtu/h'}
            Convert default 'Rate' outputs to requested units.
        energy_units : {'J', 'kJ', 'MJ', 'GJ', 'Btu', 'kWh', 'MWh'}
            Convert default 'Energy' outputs to requested units
        timestamp_format : str
            Specified str format of a datetime timestamp.

        Returns
        -------
        pandas.DataFrame
            Results for requested variables.

        """

        def standard():
            return self.tables.get_results(table, ids, start_date, end_date, include_day)

        def global_max():
            return self.tables.get_global_max_results(table, ids, start_date, end_date)

        def global_min():
            return self.tables.get_global_min_results(table, ids, start_date, end_date)

        switch = {
            "standard": standard,
            "global_max": global_max,
            "global_min": global_min,
        }

        if output_type not in switch:
            raise InvalidOutputType(
                f"Invalid output type_ '{output_type}' "
                f"requested.\n'output_type' kwarg must be"
                f" one of '{', '.join(switch.keys())}'."
            )

        if units_system not in ["SI", "IP"]:
            raise InvalidUnitsSystem(
                f"Invalid units system '{units_system}' "
                f"requested.\n'output_type' kwarg must be"
                f" one of '[SI, IP]'."
            )

        frames = []
        pairs = self._find_pairs(variables, part_match=part_match)
        for table, ids in pairs.items():
            df = switch[output_type]()
            if output_type == "standard" and rate_to_energy_dct[table]:
                if self.can_convert_rate_to_energy(table):
                    # convert 'rate' or 'energy' when standard results are requested
                    try:
                        n_days = self.tables.get_special_column(
                            table, N_DAYS_COLUMN, start_date, end_date
                        )
                    except KeyError:
                        n_days = None
                    df = convert_rate_to_energy(df, n_days)

            if units_system != "SI" or rate_units != "W" or energy_units != "J":
                df = convert_units(df, units_system, rate_units, energy_units)

            if not include_id:
                df.columns = df.columns.droplevel(ID_LEVEL)

            if not include_table_name:
                df.columns = df.columns.droplevel(TABLE_LEVEL)

            frames.append(df)

        return self._merge_frame(frames, timestamp_format, add_file_name)

    def _validate_variable_type(self, table: str, key: str, units: str, type_: Optional[str]):
        """ Check if an appropriate variable type is requested. """
        var_cls = SimpleVariable if type_ is None else Variable
        table_cls = SimpleVariable if self.is_header_simple(table) else Variable
        if var_cls != table_cls:
            raise TypeError(
                "Cannot create header variable!"
                f"Trying to add {var_cls.__name__} into {table_cls.__name__} table."
            )
        var_args = [table, key, type_, units]
        if var_cls is SimpleVariable:
            var_args.pop(2)
        return var_cls(*var_args)

    def create_header_variable(
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
    ) -> Tuple[int, Union[Variable, SimpleVariable]]:
        """ Rename the given 'Variable' using given names. """
        if new_key is None and new_type is None:
            logger.warning("Cannot rename variable! Type and key are not specified.")
        else:
            # assign original values if one of new ones is not specified
            table, key, units = variable.table, variable.key, variable.units
            new_key = new_key if new_key is not None else key
            if type(variable) is Variable:
                new_type = new_type if new_type is not None else variable.type

            id_ = self.find_id(variable)[0]

            # create new variable and add it into tree
            new_variable = self.create_header_variable(table, new_key, new_type, units)

            # remove current item to avoid item duplicity
            self.search_tree.remove_variable(variable)
            self.search_tree.add_variable(id_, new_variable)

            # rename variable in data set
            self.tables.update_variable_name(table, id_, new_variable.key, new_variable.type)
            return id_, new_variable

    def insert_variable(
            self, table: str, key: str, units: str, array: Sequence, type_: str = None
    ) -> Optional[Tuple[int, VariableType]]:
        """ Add specified output variable to the file. """
        new_variable = self.create_header_variable(table, key, units, type_=type_)
        id_ = self.tables.insert_column(new_variable, array)
        if id_:
            self.search_tree.add_variable(id_, new_variable)
            return id_, new_variable

    def aggregate_variables(
            self,
            variables: Union[VariableType, List[VariableType]],
            func: Union[str, Callable],
            new_key: str = "Custom Key",
            new_type: str = "Custom Variable",
            part_match: bool = False,
    ) -> Optional[Tuple[int, VariableType]]:
        """
        Aggregate given variables using given function.

        A new 'Variable' with specified key and variable names
        will be added into the file.

        Parameters
        ----------
        variables : list of Variable
            A list of 'Variable' named tuples.
        func: func, func name
            Function to use for aggregating the data.
            It can be specified as np.mean, 'mean', 'sum', etc.
        new_key: str, default 'Custom Key'
            Specific key for a new variable. If this would not be
            unique, unique number is added automatically.
        new_type: str, default 'Custom Variable'
            Specific variable name for a new variable. If all the
            input 'Variables' share the same variable name, this
            will be used if nto specified otherwise.
        part_match : bool
            Only substring of the part of variable is enough
        to match when searching for variables if this is True.

        Returns
        -------
        int, Variable or None
            A numeric id of the new added variable. If the variable
            could not be added, None is returned.

        """
        groups = self._find_pairs(variables, part_match=part_match)

        if not groups:
            raise CannotAggregateVariables("Cannot find variables!")

        if len(groups.keys()) > 1:
            raise CannotAggregateVariables("Cannot aggregate variables from different tables!")

        table, ids = list(groups.items())[0]

        df = self.tables.get_results(table, ids)
        variables = df.columns.get_level_values(TYPE_LEVEL).tolist()
        units = df.columns.get_level_values(UNITS_LEVEL).tolist()

        if len(set(units)) == 1:
            # no processing required
            units = units[0]
        elif is_rate_or_energy(units):
            # it's needed to assign multi index to convert energy
            if self.can_convert_rate_to_energy(table):
                try:
                    n_days = self.tables.get_special_column(table, N_DAYS_COLUMN)
                except KeyError:
                    # n_days is not required for daily, hourly and timestep intervals
                    n_days = None
                df = convert_rate_to_energy(df, n_days)
                units = next(u for u in units if u in ("J", "J/m2"))
            else:
                raise CannotAggregateVariables(
                    "Cannot aggregate variables. Variables use different units!"
                )
        else:
            raise CannotAggregateVariables(
                "Cannot aggregate variables. Variables use different units!"
            )

        sr = df.aggregate(func, axis=1)

        # use original names if defaults are kept
        if new_type == "Custom Variable":
            if all(map(lambda x: x == variables[0], variables)):
                new_type = variables[0]
        if new_key == "Custom Key":
            func_name = func.__name__ if callable(func) else func
            new_key = f"{new_key} - {func_name}"

        # return value can be either tuple (id, Variable) or None
        out = self.insert_variable(table, new_key, new_type, units, sr)

        return out

    def remove_variables(
            self, variables: Union[VariableType, List[VariableType]]
    ) -> Dict[str, List[int]]:
        """ Remove given variables from the file. """
        groups = self._find_pairs(variables if isinstance(variables, list) else [variables])
        for table, ids in groups.items():
            self.tables.delete_variables(table, ids)
        self.search_tree.remove_variables(variables)
        return groups
