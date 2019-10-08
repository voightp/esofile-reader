import pandas as pd
import re

from eso_reader.base_eso_file import BaseResultsFile
from eso_reader.mini_classes import Variable
from eso_reader.constants import TS, H, D, M, A, RP, RATE_TO_ENERGY_DCT
from eso_reader.convertor import rate_to_energy, convert_units
from eso_reader.outputs import Hourly, Daily, Monthly, Annual, Runperiod, Timestep
from eso_reader.tree import Tree
from eso_reader.performance import perf
from eso_reader.base_eso_file import InvalidOutputType

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


class BuildingEsoFile(BaseResultsFile):
    """
    The ESO class holds processed EnergyPlus output ESO file data.

    The results are stored in a dictionary using string interval identifiers
    as keys and pandas.DataFrame like classes as values.

    A structure for data bins is as follows:
    header_dict = {
        TS : {(int)ID : ('Key','Variable','Units')},
        H : {(int)ID : ('Key','Variable','Units')},
        D : {(int)ID : ('Key','Variable','Units')},
        M : {(int)ID : ('Key','Variable','Units')},
        A : {(int)ID : ('Key','Variable','Units')},
        RP : {(int)ID : ('Key','Variable','Units')},
    }

    outputs = {
        TS : outputs.Timestep,
        H : outputs.Hourly,
        D : outputs.Daily,
        M : outputs.Monthly,
        A : outputs.Annual,
        RP : outputs.Runperiod,
    }

    Attributes
    ----------
    file_path : str
        A full path of the ESO file.
    file_timestamp : datetime.datetime
        Time and date when the ESO file has been generated (extracted from original Eso file).
    header_dct : dict of {str : dict of {int : list of str}}
        A dictionary to store E+ header data
        {period : {ID : (key name, variable name, units)}}
    outputs_dct : dict of {str : Outputs subclass}
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

        # merge data having ids as columns
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

    @perf
    def results_df(
            self, variables, start_date=None, end_date=None,
            output_type="standard", add_file_name="row", include_interval=False, part_match=False,
            units_system="SI", rate_to_energy_dct=RATE_TO_ENERGY_DCT, rate_units="W",
            energy_units="J", timestamp_format="default"
    ):
        """
        Return a pandas.DataFrame object with results for given variables.

        This function extracts requested set of outputs from the eso file
        and converts to specified units if requested.

        Parameters
        ----------
        variables : Variable or list of (Variable)
            Requested variables..
        start_date : datetime like object, default None
            A start date for requested results.
        end_date : datetime like object, default None
            An end date for requested results.
        output_type : {'standard', ' global_max', 'global_min')
            Requested type of results.
        add_file_name : ('row','column',None)
            Specify if file name should be added into results df.
        include_interval : bool
            Decide if 'interval' information should be included on
            the results df.
        part_match : bool
            Only substring of the part of variable is enough
            to match when searching for variables if this is True.
        units_system : {'SI', 'IP'}
            Selected units type for requested outputs.
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
            return data_set.standard_results(*f_args)

        def global_max():
            return data_set.global_max(*f_args)

        def global_min():
            return data_set.global_min(*f_args)

        res = {
            "standard": standard,
            "global_max": global_max,
            "global_min": global_min,
        }

        if output_type not in res:
            msg = "Invalid output type '{}' requested.\n'output_type'" \
                  "kwarg must be one of '{}'.".format(output_type, ", ".join(res.keys()))
            raise InvalidOutputType(msg)

        frames = []
        groups = self.find_pairs(variables, part_match=part_match)

        for interval, ids in groups.items():
            data_set = self.outputs_dct[interval]

            # Extract specified set of results
            f_args = (ids, start_date, end_date)

            df = res[output_type]()

            if df is None:
                print("Results type '{}' is not applicable for '{}' interval."
                      "\n\tignoring the request...".format(type, interval))
                continue

            df = self.add_header_data(interval, df)

            # convert 'rate' or 'energy' when standard results are requested
            if output_type == "standard" and rate_to_energy_dct:
                is_energy = rate_to_energy_dct[interval]
                if is_energy:
                    # 'energy' is requested for current output
                    df = rate_to_energy(df, data_set, start_date, end_date)

            if units_system != "SI" or rate_units != "W" or energy_units != "J":
                df = convert_units(df, units_system, rate_units, energy_units)

            if include_interval:
                df = pd.concat([df], axis=1, keys=[interval], names=["interval"])

            frames.append(df)

        # Catch empty frames exception
        try:
            # Merge dfs
            df = pd.concat(frames, axis=1, sort=False)
            # Add file name to the index
            if timestamp_format != "default":
                df = self.update_dt_format(df, output_type, timestamp_format)
            if add_file_name:
                df = self.add_file_name(df, add_file_name)
            return df

        except ValueError:
            # raise ValueError("Any of requested variables is not included in the Eso file.")
            print("Any of requested variables is not "
                  "included in the Eso file '{}'.".format(self.file_name))

    def group_outputs(self, grouped_ids, outputs):
        """ Handle numeric outputs. """
        num_days = outputs.get_number_of_days()
        outputs = outputs.get_standard_results_only(transposed=True)
        outputs.reset_index(inplace=True)

        df = pd.merge(left=grouped_ids, right=outputs, on="id")
        df = self.calculate_totals(df)

        if num_days is not None:
            df.insert(0, "num days", num_days)

        return df

    def process_totals(self, eso_file):
        """ Create building outputs. """
        output_cls = {
            TS: Timestep,
            H: Hourly,
            D: Daily,
            M: Monthly,
            A: Annual,
            RP: Runperiod
        }

        header_dct = {}
        outputs_dct = {}
        id_gen = incr_id_gen()

        for interval, vars in eso_file.header_dct.items():
            header_df = self.get_grouped_vars(id_gen, vars)

            outputs = eso_file.outputs_dct[interval]
            outputs = self.group_outputs(header_df, outputs)

            outputs_dct[interval] = output_cls[interval](outputs)
            header_dct[interval] = self.build_header_dct(header_df)

        tree = Tree()
        tree.populate_tree(header_dct)

        return header_dct, outputs_dct, tree

    def populate_content(self, eso_file):
        """ Generate building related data based on input 'EsoFile'. """
        self.file_path = eso_file.file_path
        self.file_name = eso_file.file_name
        self._complete = eso_file.complete
        self.file_timestamp = eso_file.file_timestamp
        self.environments = eso_file.environments

        (self.header_dct,
         self.outputs_dct,
         self.header_tree) = self.process_totals(eso_file)
