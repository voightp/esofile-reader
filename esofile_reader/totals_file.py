import pandas as pd
import re

from esofile_reader.base_file import BaseResultsFile
from esofile_reader.mini_classes import Variable
from esofile_reader.constants import TS, H, D, M, A, RP, RATE_TO_ENERGY_DCT
from esofile_reader.outputs.convertor import rate_to_energy, convert_units
from esofile_reader.outputs.outputs import Outputs
from esofile_reader.outputs.tree import Tree
from esofile_reader.base_file import InvalidOutputType

variable_groups = {
    "AFN Zone", "Air System", "Baseboard", "Boiler", "Cooling Coil", "Chiller", "Chilled Water Thermal Storage Tank",
    "Cooling Tower", "Earth Tube", "Exterior Lights", "Debug Surface Solar Shading Model",
    "Electric Load Center", "Environmental Impact", "Facility Total", "Facility", "Fan", "Generator", "HVAC System",
    "Heat Exchanger", "Heating Coil", "Humidifier", "Inverter", "Lights", "Other Equipment",
    "People", "Pump", "Schedule", "Site", "Surface", "System Node", "VRF Heat Pump", "Water Heater",
    "Water to Water Heat Pump", "Water Use Equipment", "Zone", }

subgroups = {
    "_PARTITION_": "Partitions",
    "_WALL_": "Walls",
    "_ROOF_": "Roofs",
    "_FLOOR_": "Floors",
    "_EXTFLOOR_": "External floors",
    "_GROUNDFLOOR_": "Ground floors",
    "_CEILING_": "Ceilings",
}

summed_units = [
    "J",
    "J/m2"
]

averaged_units = [
    "W",
    "W/m2",
    "C",
    "",
    "W/m2-K",
    "ppm",
    "ach",
    "hr",
]


def incr_id_gen():
    """ Incremental id generator. """
    i = 0
    while True:
        i += 1
        yield i


def get_group_key(string, groups):
    for g in groups:
        if re.match(f"^{g}.*", string):
            return g
    else:
        print(f"{string} not found!")


def get_keyword(string, keywords):
    """ Return value if key is included in 'word'. """
    if any(map(lambda x: x in string, keywords)):
        return next(v for k, v in keywords.items() if k in string)


class TotalsFile(BaseResultsFile):
    """
    This class holds transformed EsoFile.

    Output variables are grouped and aggregated based on

    A structure for line bins is as follows:
    header_dict = {
        interval : {(int)ID : ('Key','Variable','Units')},
    }

    outputs = {
        interval : outputs.Outputs,
    }
    interval key can be 'timestep', 'hourly', 'daily', 'monthly', 'annual', 'runperiod'

    Attributes
    ----------
    file_path : str
        A full path of the ESO file.
    file_timestamp : datetime.datetime
        Time and date when the ESO file has been generated (extracted from original Eso file).
    header : dict of {str : dict of {int : list of str}}
        A dictionary to store E+ header line
        {period : {ID : (key name, variable name, units)}}
    outputs : dict of {str : Outputs subclass}
        A dictionary holding categorized outputs using pandas.DataFrame like classes.

    Parameters
    ----------
    eso_file : EsoFile
        A processed E+ ESO file.

    Raises
    ------
    IncompleteFile


    """

    def __init__(self, eso_file):
        super().__init__()
        self.populate_content(eso_file)

    @staticmethod
    def calculate_totals(df):
        """ Calculate 'building totals'."""
        cnd = df["units"].isin(averaged_units)
        df.drop(["id", "key", "variable", "units"], inplace=True, axis=1)

        # split df into averages and sums
        avg_df = df.loc[cnd]
        sum_df = df.loc[[not b for b in cnd]]

        # group variables and apply functions
        avg_df = avg_df.groupby(by="group_id", sort=False).mean()
        sum_df = sum_df.groupby(by="group_id", sort=False).sum()

        # merge line having ids as columns
        df = pd.concat([avg_df.T, sum_df.T], axis=1)

        df.columns.set_names("id", inplace=True)
        df.index.set_names("timestamp", inplace=True)

        return df

    @staticmethod
    def get_grouped_vars(id_gen, variables):
        """ Group header variables. """
        groups = {}
        rows = []
        for id_, var in variables.items():
            interval, key, variable, units = var
            group = units in summed_units or units in averaged_units  # check if the variables should be grouped

            gr_str = variable  # init group string to be the same as variable
            w = get_keyword(key, subgroups)

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
                    key = get_group_key(variable, variable_groups)
                    if not key:
                        key = variable.split(maxsplit=1)[0]

            if gr_str in groups:
                group_id = groups[gr_str]
            elif group:
                group_id = next(id_gen)
                groups[gr_str] = group_id
            else:
                group_id = next(id_gen)

            rows.append((group_id, id_, interval, key, variable, units))

        cols = ["group_id", "id", "interval", "key", "variable", "units"]
        return pd.DataFrame(rows, columns=cols)

    @staticmethod
    def build_header_dct(header_df):
        """ Reduce the header df to get totals. """

        def merge_vars(df):
            if len(df.index) > 1:
                sr = df.iloc[0]
                return sr
            return df.iloc[0]

        def header_vars(sr):
            return Variable(sr.interval, sr.key, sr.variable, sr.units)

        header_df.drop("id", axis=1, inplace=True)
        header_df = header_df.groupby(by="group_id")
        header_df = header_df.apply(merge_vars)
        header_df = header_df.apply(header_vars, axis=1)

        return header_df.to_dict()

    def _group_outputs(self, grouped_ids, outputs):
        """ Handle numeric outputs. """
        outputs = outputs.get_all_results(transposed=True)
        outputs.reset_index(inplace=True)

        df = pd.merge(left=grouped_ids, right=outputs, on="id")
        df = self.calculate_totals(df)

        try:
            n_days = outputs.get_number_of_days()
            df.insert(0, "n days", n_days)
        except AttributeError:
            pass

        return df

    def process_totals(self, eso_file):
        """ Create building outputs. """
        header = {}
        outputs = {}
        id_gen = incr_id_gen()

        for interval, variables in eso_file.header.items():
            header_df = self.get_grouped_vars(id_gen, variables)

            out = eso_file.outputs[interval]
            out = self._group_outputs(header_df, out)

            outputs[interval] = Outputs(out)
            header[interval] = self.build_header_dct(header_df)

        tree = Tree()
        tree.populate_tree(header)

        return header, outputs, tree

    def populate_content(self, eso_file):
        """ Generate building related line based on input 'EsoFile'. """
        self.file_path = eso_file.file_path
        self.file_name = f"{eso_file.file_name} - totals"
        self._complete = eso_file.complete
        self.file_timestamp = eso_file.file_timestamp
        self.environments = eso_file.environments

        (self.header,
         self.outputs,
         self.header_tree) = self.process_totals(eso_file)
