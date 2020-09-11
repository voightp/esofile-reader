import logging
import re
from typing import Dict, Generator, Optional

import pandas as pd

from esofile_reader.constants import *
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import ResultsFileType
from esofile_reader.tables.df_tables import DFTables

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


def _get_group_key(string: str, groups: set) -> str:
    """ """
    for g in groups:
        if re.match(f"^{g}.*", string):
            return g
    else:
        logging.info(f"{string} not found!")


def _get_keyword(string: str, keywords: Dict[str, str]) -> str:
    """ Return value if key is included in 'word'. """
    if any(map(lambda x: x in string, keywords)):
        return next(v for k, v in keywords.items() if k in string)


def calculate_totals(df: pd.DataFrame) -> pd.DataFrame:
    """ Handle totals generation."""
    averaged_arr = df.columns.get_level_values(UNITS_LEVEL).isin(AVERAGED_UNITS)
    mi_df = df.columns.to_frame(index=False)
    mi_df.drop_duplicates(inplace=True)

    # split df into averages and sums
    avg_df = df.loc[:, averaged_arr]
    sum_df = df.loc[:, ~averaged_arr]

    # group variables and apply functions
    avg_df = avg_df.groupby(axis=1, level=df.columns.names, sort=False).mean()
    sum_df = sum_df.groupby(axis=1, level=df.columns.names, sort=False).sum()

    # # index gets lost in 'groupby'
    df = pd.concat([avg_df, sum_df], axis=1)
    df.sort_values(by=ID_LEVEL, axis=1, inplace=True)

    return df


def create_grouped_multiindex(
    id_gen: Generator[int, None, None], columns: pd.MultiIndex
) -> pd.MultiIndex:
    """ Create new multiindex with grouped ids. """
    groups = {}
    muiltiindex_items = []
    for id_, table, key, type_, units in columns:

        # variable can be grouped only if it's included as avg or sum
        group = (units in SUMMED_UNITS or units in AVERAGED_UNITS) and key != SPECIAL
        if group:
            # initialize group string to be the same as type
            gr_str = type_
            w = _get_keyword(key, SUBGROUPS)
            if key == "Cumulative Meter" or key == "Meter":
                if "#" in type_:
                    # use last substring as a key
                    gr_str = type_.split("#")[-1]
                    type_ = gr_str
            elif w:
                gr_str = w + " " + gr_str
                key = w  # assign a new key based on subgroup keyword
            else:
                # assign key based on 'Variable' category
                # the category is missing, use a first word in 'Variable' string
                key = _get_group_key(type_, VARIABLE_GROUPS)
                if not key:
                    key = type_.split(sep=" ", maxsplit=1)[0]

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

        muiltiindex_items.append((group_id, table, key, type_, units))

    return pd.MultiIndex.from_tuples(muiltiindex_items, names=COLUMN_LEVELS)


def filter_invalid_variables(df: pd.DataFrame):
    srs = []
    types = df.columns.get_level_values(TYPE_LEVEL)

    # ignored type may be a substring to catch types such as
    # 'Performance Curve Input Variable 1', 'Performance Curve Input Variable 2'
    for ignored_type in IGNORED_TYPES:
        srs.append(types.str.contains(ignored_type))

    cond1 = ~pd.DataFrame(srs).apply(lambda x: x.any()).to_numpy()
    cond2 = ~df.columns.get_level_values(UNITS_LEVEL).isin(IGNORED_UNITS)

    return df.loc[:, cond1 & cond2]


def process_totals_table(
    numeric_table: pd.DataFrame, id_gen: Generator[int, None, None]
) -> Optional[pd.DataFrame]:
    table = filter_invalid_variables(numeric_table)
    if not table.empty:
        table.columns = create_grouped_multiindex(id_gen, table.columns)
        totals_table = calculate_totals(table)
        return totals_table


def process_totals(file: ResultsFileType) -> DFTables:
    """ Generate 'totals' outputs. """
    tables = DFTables()
    id_gen = incremental_id_gen(start=1)
    for table_name in file.table_names:
        if not file.tables.is_simple(table_name):
            # simple table cannot generate totals
            special_table = file.tables.get_special_table(table_name)
            numeric_table = file.tables.get_numeric_table(table_name)
            totals_table = process_totals_table(numeric_table, id_gen)
            if totals_table is not None:
                tables[table_name] = pd.concat([special_table, totals_table], axis=1)

    return tables
