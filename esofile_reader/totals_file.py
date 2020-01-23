import pandas as pd
import re

from esofile_reader.base_file import BaseFile, IncompleteFile
from esofile_reader.diff_file import DiffFile
from esofile_reader.utils.mini_classes import Variable
from esofile_reader.outputs.df_outputs import DFOutputs
from esofile_reader.utils.tree import Tree
from esofile_reader.utils.utils import incremental_id_gen
from esofile_reader.constants import N_DAYS_COLUMN, DAY_COLUMN, AVERAGED_UNITS, \
    SUMMED_UNITS, IGNORED_UNITS

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

    def __init__(self, eso_file):
        super().__init__()
        self.populate_content(eso_file)

    @staticmethod
    def _get_group_key(string, groups):
        """ """
        for g in groups:
            if re.match(f"^{g}.*", string):
                return g
        else:
            print(f"{string} not found!")

    @staticmethod
    def _get_keyword(string, keywords):
        """ Return value if key is included in 'word'. """
        if any(map(lambda x: x in string, keywords)):
            return next(v for k, v in keywords.items() if k in string)

    @staticmethod
    def _calculate_totals(df):
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

    def _get_grouped_vars(self, id_gen, variables):
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

    def process_totals(self, file):
        """ Process 'Totals' outputs. """
        header = {}
        outputs = DFOutputs()
        id_gen = incremental_id_gen()

        for interval in file.available_intervals:
            variable_dct = file.data.get_variables(interval)
            header_df = self._get_grouped_vars(id_gen, variable_dct)

            out = file.data.get_only_numeric_data(interval)
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

            outputs.set_data(interval, df)

        tree = Tree()
        tree.populate_tree(header)

        return outputs, tree

    def populate_content(self, eso_file):
        """ Generate 'Totals' related data based on input 'ResultFile'. """
        self.file_path = eso_file.file_path
        self.file_name = f"{eso_file.file_name} - totals"
        self.file_timestamp = eso_file.file_timestamp  # use base file timestamp

        content = self.process_totals(eso_file)

        if content:
            self._complete = True
            (self.data,
             self._search_tree) = content

    def generate_diff(self, other_file):
        """ Generate 'Diff' results file. """
        if self.complete:
            return DiffFile(self, other_file)
        else:
            raise IncompleteFile(f"Cannot generate 'Diff' file, "
                                 f"file {self.file_path} is not complete!")
