import re
from typing import Type, Dict, Generator, List

import pandas as pd

from esofile_reader.base_file import BaseFile, IncompleteFile
from esofile_reader.constants import N_DAYS_COLUMN, DAY_COLUMN, AVERAGED_UNITS, \
    SUMMED_UNITS, IGNORED_UNITS
from esofile_reader.diff_file import DiffFile
from esofile_reader.outputs.df_data import DFData
from esofile_reader.utils.search_tree import Tree
from esofile_reader.utils.utils import incremental_id_gen
from esofile_reader.utils.mini_classes import Variable

variable_groups = {
    "AFN Zone", "Air System", "Baseboard", "Boiler", "Cooling Coil", "Chiller",
    "Chilled Water Thermal Storage Tank", "Cooling Tower", "Earth Tube",
    "Exterior Lights", "Debug Surface Solar Shading Model", "Electric Load Center",
    "Environmental Impact", "Facility Total", "Facility", "Fan", "Generator",
    "HVAC System", "Heat Exchanger", "Heating Coil", "Humidifier", "Inverter",
    "Lights", "Other Equipment", "People", "Pump", "Schedule", "Site", "Surface",
    "System Node", "VRF Heat Pump", "Water Heater", "Water to Water Heat Pump",
    "Water Use Equipment", "Zone", }

subgroups = {
    "_PARTITION_": "Partitions",
    "_WALL_": "Walls",
    "_ROOF_": "Roofs",
    "_FLOOR_": "Floors",
    "_EXTFLOOR_": "External floors",
    "_GROUNDFLOOR_": "Ground floors",
    "_CEILING_": "Ceilings",
}


class TotalsFile(BaseFile):
    """
    This class handles 'Totals' generation.

    """

    def __init__(self, result_file: Type[BaseFile]):
        super().__init__()
        self.populate_content(result_file)

    @staticmethod
    def _get_group_key(string: str, groups: set) -> str:
        """ """
        for g in groups:
            if re.match(f"^{g}.*", string):
                return g
        else:
            print(f"{string} not found!")

    @staticmethod
    def _get_keyword(string: str, keywords: Dict[str, str]) -> str:
        """ Return value if key is included in 'word'. """
        if any(map(lambda x: x in string, keywords)):
            return next(v for k, v in keywords.items() if k in string)

    @staticmethod
    def _calculate_totals(df: pd.DataFrame) -> pd.DataFrame:
        """ Handle totals generation."""
        cnd = df.index.get_level_values("units").isin(AVERAGED_UNITS)
        mi_df = df.index.to_frame(index=False)
        mi_df.drop_duplicates(inplace=True)

        # split df into averages and sums
        avg_df = df.loc[cnd]
        sum_df = df.loc[~cnd]

        # group variables and apply functions
        avg_df = avg_df.groupby(by="group_id", sort=False).mean()
        sum_df = sum_df.groupby(by="group_id", sort=False).sum()

        # index gets lost in 'groupby'
        df = pd.concat([avg_df, sum_df])
        df.reset_index(inplace=True, drop=False)
        df = pd.merge(mi_df, df, on="group_id")
        df.set_index(["group_id", "interval", "key", "variable", "units"], inplace=True)

        df.index.set_names("id", level="group_id", inplace=True)
        df.columns.set_names("timestamp", inplace=True)

        return df.T

    def _get_grouped_vars(self, id_gen: Generator[int, None, None],
                          variables: Dict[int, List[Variable]]) -> pd.DataFrame:
        """ Group header variables. """
        groups = {}
        rows, index = [], []
        for id_, var in variables.items():
            interval, key, variable, units = var

            # variable can be grouped only if it's included as avg or sum
            group = units in SUMMED_UNITS or units in AVERAGED_UNITS

            # init group string to be the same as variable
            gr_str = variable
            w = self._get_keyword(key, subgroups)

            if key == "Cumulative Meter" or key == "Meter":
                if "#" in variable:
                    # use last substring as a key
                    variable = variable.split()[-1]
                    gr_str = variable + " " + gr_str
            elif w:
                gr_str = w + " " + gr_str
                key = w  # assign a new key based on subgroup keyword
            else:
                # assign key based on 'Variable' category
                # the category is missing, use a first word in 'Variable' string
                if group:
                    key = self._get_group_key(variable, variable_groups)
                    if not key:
                        key = variable.split(maxsplit=1)[0]

            if gr_str in groups:
                group_id = groups[gr_str]
            elif group:
                group_id = next(id_gen)
                groups[gr_str] = group_id
            else:
                group_id = next(id_gen)

            index.append(id_)
            rows.append((group_id, interval, key, variable, units))

        cols = ["group_id", "interval", "key", "variable", "units"]

        return pd.DataFrame(rows, columns=cols, index=index)

    def process_totals(self, file: Type[BaseFile]):
        """ Process 'Totals' outputs. """
        header = {}
        outputs = DFData()
        id_gen = incremental_id_gen()

        for interval in file.available_intervals:
            variable_dct = file.data.get_variables_dct(interval)
            header_df = self._get_grouped_vars(id_gen, variable_dct)

            out = file.data.get_all_results(interval)
            out = out.loc[:, ~out.columns.get_level_values("units").isin(IGNORED_UNITS)]
            out.columns = out.columns.droplevel(["interval", "key", "variable", "units"])

            df = pd.merge(left=header_df, right=out.T, left_index=True, right_index=True)
            df.reset_index(drop=True, inplace=True)
            df.set_index(["group_id", "interval", "key", "variable", "units"], inplace=True)
            df = self._calculate_totals(df)

            for s in [N_DAYS_COLUMN, DAY_COLUMN]:
                try:
                    col = out[s]
                    df.insert(0, s, col)
                except KeyError:
                    pass

            outputs.populate_table(interval, df)

        tree = Tree()
        tree.populate_tree(header)

        return outputs, tree

    def populate_content(self, file: Type[BaseFile]):
        """ Generate 'Totals' related data based on input 'ResultFile'. """
        self.file_path = file.file_path
        self.file_name = f"{file.file_name} - totals"
        self.file_created = file.file_created  # use base file timestamp

        self.data, self._search_tree = self.process_totals(file)

    def generate_diff(self, other_file: Type[BaseFile]):
        """ Generate 'Diff' results file. """
        return DiffFile(self, other_file)
