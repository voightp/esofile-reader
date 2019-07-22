import pandas as pd
import os
import time

from random import randint
from collections import defaultdict
from functools import reduce

from eso_reader.base_eso_file import BaseEsoFile
from eso_reader.eso_file import EsoFile
from eso_reader.constants import TS, H, D, M, A, RP
from eso_reader.outputs import Hourly, Daily, Monthly, Annual, Runperiod, Timestep
from eso_reader.convertor import rate_to_energy, convert_units
from eso_reader.eso_processor import read_file
from eso_reader.mini_classes import HeaderVariable
from eso_reader.constants import RATE_TO_ENERGY_DCT

categories = {
    "Air System", "Debug Surface Solar Shading Model", "Electric Load Center", "Environmental Impact",
    "Facility", "Generator", "HVAC System", "Inverter", "Lights", "Other Equipment", "People",
    "Schedule", "Site", "Surface", "System Node", "Water Use Equipment", "Zone", }

grouped_units = {
    "W",
    "W/m2",
    "C",
    "J",
    "J/m2"
}

subgroup_keywords = [
    "_PARTITION_",
    "_WALL_",
    "_ROOF_",
]

summed_units = [
    "J",
    "J/m2"
]

averaged_units = [
    "W",
    "W/m2",
    "C"
]


def incr_id_gen():
    """ Incremental id generator. """
    i = 0
    while True:
        i += 1
        yield i


class BuildingEsoFile(BaseEsoFile):
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
    file_path : path like object
        A full path of the ESO file
    exclude_intervals : list of {TS, H, D, M, A, RP}
        A list of interval identifiers which will be ignored. This can
        be used to avoid processing hourly, sub-hourly intervals.
    report_progress : bool, default True
        Processing progress is reported in terminal when set as 'True'.
    ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.
    suppress_errors: bool, default False
        Do not raise IncompleteFile exceptions when processing fails
    Raises
    ------
    IncompleteFile


    """

    def __init__(self, eso_file):
        super().__init__()
        self.populate_content(eso_file)

    def get_keyword(self, word):
        if any(map(lambda x: x in word, subgroup_keywords)):
            return next(w for w in subgroup_keywords if w in word)

    def _get_grouped_vars(self, id_gen, variables):
        """ Group header variables. """
        groups = {}
        rows = []
        for id_, var in variables.items():
            gr_str = var.variable

            w = self.get_keyword(var.key)
            if w:
                gr_str = gr_str + w

            if gr_str in groups:
                group_id = groups[gr_str]
            elif var.units in grouped_units:
                group_id = next(id_gen)
                groups[gr_str] = group_id
            else:
                group_id = next(id_gen)

            rows.append((group_id, id_, *var))

        cols = ["group_id", "id", "key", "variable", "units"]
        return pd.DataFrame(rows, columns=cols)

    def calculate_totals(self, df):
        """ Calculate 'building totals'."""
        cnd = df["units"].isin(averaged_units)
        df.drop(["id", "key", "variable", "units"], inplace=True, axis=1)

        avg_df = df.loc[cnd]
        sum_df = df.loc[[not b for b in cnd]]

        avg_df = avg_df.groupby(by="group_id", sort=False).mean()
        sum_df = sum_df.groupby(by="group_id", sort=False).sum()

        df = pd.concat([avg_df.T, sum_df.T], axis=1)

        df.columns.set_names("id", inplace=True)
        df.index.set_names("timestamp", inplace=True)

        return df

    def generate_outputs(self, grouped_ids, outputs):
        """ Handle numeric outputs. """
        num_days = outputs.get_number_of_days()
        outputs = outputs.get_standard_results_only(transposed=True)
        outputs.reset_index(inplace=True)

        df = pd.merge(left=grouped_ids, right=outputs, on="id")
        df = self.calculate_totals(df)

        if num_days is not None:
            df.insert(0, "num days", num_days)

        return df

    def updaste_header_df(self, header_df):
        """ Reduce the header df to get totals. """

        def update(df):
            if len(df.index) > 1:
                sr = df.iloc[0]
                sr["key"] = "Total"
                return sr
            return df.iloc[0]

        header_df.drop("id", axis=1, inplace=True)
        header_df = header_df.groupby(by="group_id")
        header_df = header_df.apply(update)

        return header_df

    def building_outputs(self, eso_file: EsoFile):
        """ Create building outputs. """
        dct = {
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
            header_df = self._get_grouped_vars(id_gen, vars)

            outputs = eso_file.outputs_dct[interval]
            outputs = self.generate_outputs(header_df, outputs)

            outputs_dct[interval] = dct[interval](outputs)
            header_dct[interval] = self.updaste_header_df(header_df)

        return header_dct, outputs_dct

    def populate_content(self, eso_file):
        """ Generate building related data based on input 'EsoFile'. """
        self.file_path = eso_file.file_path
        self._complete = eso_file.complete
        self.file_timestamp = eso_file.file_timestamp
        self.environments = eso_file.environments

        self.building_outputs(eso_file)

        self.header_dct = None
        self.outputs_dct = None
        self.header_tree = None
