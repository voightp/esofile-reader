import logging
import re
from typing import Dict, Generator, List

import pandas as pd

from esofile_reader.base_file import BaseFile
from esofile_reader.constants import *
from esofile_reader.data.df_data import DFData
from esofile_reader.diff_file import DiffFile
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import Variable, ResultsFile
from esofile_reader.search_tree import Tree


class TotalsFile(BaseFile):
    """
    This class handles 'Totals' generation.

    """

    VARIABLE_GROUPS = {
        "AFN Zone",
        "Air System",
        "Baseboard",
        "Boiler",
        "Cooling Coil",
        "Chiller",
        "Chilled Water Thermal Storage Tank",
        "Cooling Tower",
        "Earth Tube",
        "Exterior Lights",
        "Debug Surface Solar Shading Model",
        "Electric Load Center",
        "Environmental Impact",
        "Facility Total",
        "Facility",
        "Fan",
        "Generator",
        "HVAC System",
        "Heat Exchanger",
        "Heating Coil",
        "Humidifier",
        "Inverter",
        "Lights",
        "Other Equipment",
        "People",
        "Pump",
        "Refrigeration Zone Air Chiller",
        "Refrigeration Air Chiller System",
        "Refrigeration Zone Case and Walk In",
        "Schedule",
        "Site",
        "Surface",
        "System Node",
        "VRF Heat Pump",
        "Water Heater",
        "Water to Water Heat Pump",
        "Water Use Equipment",
        "Zone",
    }

    SUBGROUPS = {
        "_WIN": "Windows",
        "_HOLE": "Holes",
        "_DOOR": "Doors",
        "_VENT": "Vents",
        "_PARTITION_": "Partitions",
        "_WALL_": "Walls",
        "_ROOF_": "Roofs",
        "_FLOOR_": "Floors",
        "_EXTFLOOR_": "External floors",
        "_GROUNDFLOOR_": "Ground floors",
        "_CEILING_": "Ceilings",
    }

    IGNORED_VARIABLES = {
        "Performance Curve Input Variable",
        "Performance Curve Output Value",
    }

    def __init__(self, result_file: ResultsFile):
        super().__init__()
        self.populate_content(result_file)

    @staticmethod
    def _get_group_key(string: str, groups: set) -> str:
        """ """
        for g in groups:
            if re.match(f"^{g}.*", string):
                return g
        else:
            logging.info(f"{string} not found!")

    @staticmethod
    def _get_keyword(string: str, keywords: Dict[str, str]) -> str:
        """ Return value if key is included in 'word'. """
        if any(map(lambda x: x in string, keywords)):
            return next(v for k, v in keywords.items() if k in string)

    @staticmethod
    def _calculate_totals(df: pd.DataFrame) -> pd.DataFrame:
        """ Handle totals generation."""
        cnd = df.index.get_level_values(UNITS_LEVEL).isin(AVERAGED_UNITS)
        mi_df = df.index.to_frame(index=False)
        mi_df.drop_duplicates(inplace=True)

        # split df into averages and sums
        avg_df = df.loc[cnd]
        sum_df = df.loc[~cnd]

        # group variables and apply functions
        avg_df = avg_df.groupby(by=GROUP_ID_LEVEL, sort=False).mean()
        sum_df = sum_df.groupby(by=GROUP_ID_LEVEL, sort=False).sum()

        # index gets lost in 'groupby'
        df = pd.concat([avg_df, sum_df])
        df.reset_index(inplace=True, drop=False)
        df = pd.merge(mi_df, df, on=GROUP_ID_LEVEL)
        df.set_index([GROUP_ID_LEVEL, *COLUMN_LEVELS[1:]], inplace=True)

        df.index.set_names(ID_LEVEL, level=GROUP_ID_LEVEL, inplace=True)

        return df.T

    def _get_grouped_vars(
            self, id_gen: Generator[int, None, None], variables: Dict[int, List[Variable]]
    ) -> pd.DataFrame:
        """ Group header variables. """
        groups = {}
        rows, index = [], []
        for id_, var in variables.items():
            table, key, type, units = var

            # variable can be grouped only if it's included as avg or sum
            group = units in SUMMED_UNITS or units in AVERAGED_UNITS

            # init group string to be the same as variable
            gr_str = type
            w = self._get_keyword(key, self.SUBGROUPS)

            if group:
                if key == "Cumulative Meter" or key == "Meter":
                    if "#" in type:
                        # use last substring as a key
                        gr_str = type.split("#")[-1]
                        type = gr_str
                elif w:
                    gr_str = w + " " + gr_str
                    key = w  # assign a new key based on subgroup keyword
                else:
                    # assign key based on 'Variable' category
                    # the category is missing, use a first word in 'Variable' string
                    key = self._get_group_key(type, self.VARIABLE_GROUPS)
                    if not key:
                        key = type.split(maxsplit=1)[0]

                if gr_str in groups:
                    # variable group already exist, get id of the existing group
                    group_id = groups[gr_str]
                else:
                    # variable group does not exist yet, create new group id
                    # and store group reference for consequent variables
                    group_id = next(id_gen)
                    groups[gr_str] = group_id
            else:
                # units cannot be grouped, create an independent variable
                group_id = next(id_gen)

            index.append(id_)
            rows.append((group_id, table, key, type, units))

        cols = [GROUP_ID_LEVEL, *COLUMN_LEVELS[1:]]

        return pd.DataFrame(rows, columns=cols, index=index)

    def process_totals(self, file: ResultsFile):
        """ Process 'Totals' outputs. """

        def ignored_ids(df):
            srs = []
            sr = df.columns.get_level_values(TYPE_LEVEL)

            for w in self.IGNORED_VARIABLES:
                srs.append(sr.str.contains(w))

            cond1 = pd.DataFrame(srs).apply(lambda x: x.any()).tolist()
            cond2 = df.columns.get_level_values(UNITS_LEVEL).isin(IGNORED_UNITS)

            return df.loc[:, cond1 | cond2].columns.get_level_values(ID_LEVEL)

        data = DFData()
        id_gen = incremental_id_gen(start=1)

        for table in file.table_names:
            out = file.data.get_numeric_table(table)

            # find invalid ids
            ids = ignored_ids(out)

            # filter variables based on ignored units and variables
            out = out.loc[:, ~out.columns.get_level_values(ID_LEVEL).isin(ids)]

            if out.empty:
                # ignore empty tables
                continue

            # leave only 'id' column as header data will be added
            out.columns = out.columns.droplevel(COLUMN_LEVELS[1:])

            # get header variables and filter them
            variable_dct = file.data.get_variables_dct(table)
            variable_dct = {k: v for k, v in variable_dct.items() if k not in ids}

            header_df = self._get_grouped_vars(id_gen, variable_dct)

            # join header data and numeric outputs
            df = pd.merge(
                how="inner", left=header_df, right=out.T, left_index=True, right_index=True,
            )

            # create new totals DataFrame
            df.reset_index(drop=True, inplace=True)
            df.set_index([GROUP_ID_LEVEL, *COLUMN_LEVELS[1:]], inplace=True)
            df = self._calculate_totals(df)

            # restore index
            df.index = out.index
            data.populate_table(table, df)

            for c in [N_DAYS_COLUMN, DAY_COLUMN]:
                try:
                    c1 = file.data.get_special_column(table, c)
                    c2 = file.data.get_special_column(table, c)
                    if c1.equals(c2):
                        data.insert_special_column(table, c, c1)
                except KeyError:
                    pass

        tree = Tree()
        tree.populate_tree(data.get_all_variables_dct())

        return data, tree

    def populate_content(self, file: ResultsFile):
        """ Generate 'Totals' related data based on input 'ResultFile'. """
        self.file_path = file.file_path
        self.file_name = f"{file.file_name} - totals"
        self.file_created = file.file_created  # use base file timestamp

        self.data, self.search_tree = self.process_totals(file)

    def generate_diff(self, other_file: ResultsFile):
        """ Generate 'Diff' results file. """
        return DiffFile(self, other_file)
