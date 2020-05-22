import logging
import traceback
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Union, Tuple, Sequence, Callable, Optional

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.convertor import (
    rate_and_energy_units,
    convert_rate_to_energy,
    convert_units,
)
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import Variable
from esofile_reader.processor.interval_processor import update_dt_format


class BaseFile:
    """
    A base class works as a base for various result file formats.


    Attributes
    ----------
    file_path : str
        A full path of the result file.
    file_created : datetime
        Time and date when of the file generation..
    data : {DFData, SQLData}
        A class to store results data
    search_tree : Tree
        N array tree for efficient id searching.


    Notes
    -----
    This class cannot be used directly!

    Method 'populate_content' is only a signature method to
    be implemented in subclasses.

    """

    def __init__(self):
        self.file_path = None
        self.file_name = None
        self.data = None
        self.file_created = None
        self.search_tree = None

    def __repr__(self):
        return (
            f"File: {self.file_name}"
            f"\n\tClass: {self.__class__.__name__}"
            f"\n\tPath: {self.file_path}"
            f"\n\tCreated: {self.file_created}"
            f"\n\tAvailable intervals: [{', '.join(self.available_intervals)}]"
        )

    @property
    def complete(self) -> bool:
        """ Check if the file has been populated. """
        return self.data and self.search_tree

    @property
    def available_intervals(self) -> List[str]:
        """ Get all available intervals. """
        return self.data.get_available_intervals()

    def is_header_simple(self, interval: str) -> bool:
        """ Check if header uses Variablesor SimpleVariable data. """
        return self.data.is_simple(interval)

    def get_header_dictionary(self, interval: str) -> Dict[int, Variable]:
        """ Get all variables for given interval. """
        return self.data.get_variables_dct(interval)

    def get_header_df(self, interval: str) -> pd.DataFrame:
        """ Get all variables for given interval. """
        return self.data.get_variables_df(interval)

    def rename(self, name: str) -> None:
        """ Set a new file name. """
        self.file_name = name

    def populate_content(self, *args, **kwargs) -> None:
        """ Populate instance attributes. """
        pass

    def _add_file_name(self, df: pd.DataFrame, name_position: str) -> pd.DataFrame:
        """ Add file name to index. """
        pos = ["row", "column", "None"]  # 'None' is here only to inform
        if name_position not in pos:
            name_position = "row"
            logging.warning(
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
            logging.warning(
                f"Any of requested variables is not "
                f"included in the Eso file '{self.file_name}'."
            )

    def _find_pairs(
            self, variables: Union[Variable, List[Variable], List[int]],
            part_match: bool = False
    ) -> Dict[str, List[int]]:
        """
        Find variable ids for a list of 'Variables'.

        Parameters
        ----------
        variables : list of {Variable, int}
            A list of 'Variable' named tuples.
        part_match : bool
            Only substring of the part of variable is enough
            to match when searching for variables if this is True.

        Returns
        -------
        dct
            A dictionary with 'intervals' as keys and lists of
            ids as values.

        """
        variables = variables if isinstance(variables, list) else [variables]
        out = defaultdict(list)

        if all(map(lambda x: isinstance(x, (Variable, int)), variables)):
            if all(map(lambda x: isinstance(x, Variable), variables)):
                ids = []
                for variable in variables:
                    ids.extend(self.search_tree.find_ids(variable, part_match=part_match))
            else:
                # all inputs are integers
                ids = variables
            header = self.data.get_all_variables_df()
            # filter values by id, isin cannot be used as it breaks ids order
            df = header.set_index(ID_LEVEL)
            df = df.loc[ids, [INTERVAL_LEVEL]]
            df.reset_index(inplace=True)
            grouped = df.groupby(INTERVAL_LEVEL, sort=False, group_keys=False)
            for interval, df in grouped:
                out[interval] = df[ID_LEVEL].tolist()
        else:
            raise TypeError(
                "Unexpected variable type! This can only be "
                "either integers or 'Variable' named tuples."
            )
        return out

    def find_ids(
            self, variables: Union[Variable, List[Variable]], part_match: bool = False
    ) -> List[int]:
        """ Find ids for a list of 'Variables'. """
        variables = variables if isinstance(variables, list) else [variables]
        ids = []
        for variable in variables:
            ids.extend(self.search_tree.find_ids(variable, part_match=part_match))
        return ids

    def get_results(
            self,
            variables: Union[Variable, List[Variable], int, List[int]],
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            output_type: str = "standard",
            add_file_name: str = "row",
            include_interval: bool = False,
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
        include_interval : bool
            Decide if 'interval' information should be included on
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
            return self.data.get_results(interval, ids, start_date, end_date, include_day)

        def global_max():
            return self.data.get_global_max_results(interval, ids, start_date, end_date)

        def global_min():
            return self.data.get_global_min_results(interval, ids, start_date, end_date)

        res = {
            "standard": standard,
            "global_max": global_max,
            "global_min": global_min,
        }

        if output_type not in res:
            raise InvalidOutputType(
                f"Invalid output type_ '{output_type}' "
                f"requested.\n'output_type' kwarg must be"
                f" one of '{', '.join(res.keys())}'."
            )

        if units_system not in ["SI", "IP"]:
            raise InvalidUnitsSystem(
                f"Invalid units system '{units_system}' "
                f"requested.\n'output_type' kwarg must be"
                f" one of '[SI, IP]'."
            )

        frames = []
        pairs = self._find_pairs(variables, part_match=part_match)
        for interval, ids in pairs.items():
            df = res[output_type]()

            if interval != RANGE:
                # convert 'rate' or 'energy' when standard results are requested
                if output_type == "standard" and rate_to_energy_dct[interval]:
                    try:
                        n_days = self.data.get_number_of_days(interval, start_date, end_date)
                    except KeyError:
                        n_days = None

                    df = convert_rate_to_energy(df, interval, n_days)

            if units_system != "SI" or rate_units != "W" or energy_units != "J":
                df = convert_units(df, units_system, rate_units, energy_units)

            if not include_id:
                df.columns = df.columns.droplevel(ID_LEVEL)

            if not include_interval:
                df.columns = df.columns.droplevel(INTERVAL_LEVEL)

            frames.append(df)

        return self._merge_frame(frames, timestamp_format, add_file_name)

    def create_header_variable(self, interval: str, key: str, var: str, units: str) -> Variable:
        """ Create unique header variable. """

        def add_num():
            new_key = f"{key} ({i})"
            return Variable(interval, new_key, var, units)

        variable = Variable(interval, key, var, units)

        i = 0
        while self.search_tree.variable_exists(variable):
            i += 1
            variable = add_num()

        return variable

    def rename_variable(
            self, variable: Variable, new_type: str = "", new_key: str = ""
    ) -> Tuple[int, Variable]:
        """ Rename the given 'Variable' using given names. """
        ids = self.find_ids(variable)
        interval, key, type_, units = variable

        new_type = type_ if not new_type else new_type
        new_key = key if not new_key else new_key

        if (not new_type and not new_key) or (key == new_key and type_ == new_type):
            logging.warning(
                "Cannot rename variable! Type and key are "
                "not specified or are the same as original variable."
            )
        elif ids:
            # remove current item to avoid item duplicity
            self.search_tree.remove_variable(variable)

            # create new variable and add it into tree
            new_var = self.create_header_variable(interval, new_key, new_type, units)
            self.search_tree.add_variable(ids[0], new_var)

            # rename variable in data set
            self.data.update_variable_name(interval, ids[0], new_var.key, new_var.type)
            return ids[0], new_var
        else:
            logging.warning("Cannot rename variable! Original variable not found!")

    def add_output(
            self, interval: str, key: str, type: str, units: str, array: Sequence
    ) -> Tuple[int, Variable]:
        """ Add specified output variable to the file. """
        new_var = self.create_header_variable(interval, key, type, units)
        id_ = self.data.insert_variable(new_var, array)
        if id_:
            self.search_tree.add_variable(id_, new_var)
            return id_, new_var

    def aggregate_variables(
            self,
            variables: Union[Variable, List[Variable]],
            func: Union[str, Callable],
            new_key: str = "Custom Key",
            new_type: str = "Custom Variable",
            part_match: bool = False,
    ) -> Tuple[int, Variable]:
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
            raise CannotAggregateVariables(
                "Cannot aggregate variables from different intervals!"
            )

        interval, ids = list(groups.items())[0]

        df = self.data.get_results(interval, ids)
        variables = df.columns.get_level_values(TYPE_LEVEL).tolist()
        units = df.columns.get_level_values(UNITS_LEVEL).tolist()

        if len(set(units)) == 1:
            # no processing required
            units = units[0]
        elif rate_and_energy_units(units) and interval != RANGE:
            # it's needed to assign multi index to convert energy
            try:
                n_days = self.data.get_number_of_days(interval)
            except KeyError:
                n_days = None
                if interval in [M, A, RP]:
                    raise CannotAggregateVariables(
                        f"Cannot aggregate variables. " f"'{N_DAYS_COLUMN}' is not available!"
                    )
            df = convert_rate_to_energy(df, interval, n_days)
            units = next(u for u in units if u in ("J", "J/m2"))
        else:
            raise CannotAggregateVariables(
                "Cannot aggregate variables. " "Variables use different units!"
            )

        sr = df.aggregate(func, axis=1)

        if new_type == "Custom Variable":
            if all(map(lambda x: x == variables[0], variables)):
                new_type = variables[0]

        if new_key == "Custom Key":
            func_name = func.__name__ if callable(func) else func
            new_key = f"{new_key} - {func_name}"

        # results can be either tuple (id, Variable) or None
        out = self.add_output(interval, new_key, new_type, units, sr)

        return out

    def remove_outputs(
            self, variables: Union[Variable, List[Variable]]
    ) -> Dict[str, List[int]]:
        """ Remove given variables from the file. """
        variables = variables if isinstance(variables, list) else [variables]

        groups = self._find_pairs(variables)
        for interval, ids in groups.items():
            self.data.delete_variables(interval, ids)

        # clean up the tree
        self.search_tree.remove_variables(variables)

        return groups

    def get_numeric_table(self, interval: str) -> pd.DataFrame:
        """ Return the file as a single DataFrame (without special columns). """
        try:
            df = self.data.get_numeric_table(interval)
        except KeyError:
            raise KeyError(f"Cannot find interval: '{interval}'.\n{traceback.format_exc()}")
        return df
