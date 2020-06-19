from typing import Tuple

import pandas as pd

from esofile_reader.constants import N_DAYS_COLUMN, DAY_COLUMN, ID_LEVEL
from esofile_reader.data.df_data import DFData
from esofile_reader.exceptions import *
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import ResultsFile, Data
from esofile_reader.search_tree import Tree


def calculate_diff(file: ResultsFile, other_file: ResultsFile) -> DFData:
    """ Calculate difference between two results files. """
    data = DFData()
    id_gen = incremental_id_gen()

    for table in file.table_names:
        if table not in other_file.table_names:
            continue

        df1 = file.get_numeric_table(table)
        df2 = other_file.get_numeric_table(table)

        df1.columns = df1.columns.droplevel(ID_LEVEL)
        df2.columns = df2.columns.droplevel(ID_LEVEL)

        index_cond = df1.index.intersection(df2.index).tolist()
        columns_cond = df1.columns.intersection(df2.columns).tolist()

        df = df1.loc[index_cond, columns_cond] - df2.loc[index_cond, columns_cond]
        df.dropna(how="all", inplace=True, axis=1)

        if not df.empty:
            # create new id for each record
            ids = [next(id_gen) for _ in range(len(df.columns))]
            header_df = df.columns.to_frame(index=False)
            header_df.insert(0, ID_LEVEL, ids)

            df.columns = pd.MultiIndex.from_frame(header_df)
            data.populate_table(table, df)

            for c in [N_DAYS_COLUMN, DAY_COLUMN]:
                try:
                    c1 = file.data.get_special_column(table, c).loc[index_cond]
                    c2 = file.data.get_special_column(table, c).loc[index_cond]
                    if c1.equals(c2):
                        data.insert_special_column(table, c, c1)
                except KeyError:
                    pass

    return data


def process_diff(
        first_file: ResultsFile, other_file: ResultsFile
) -> Tuple[Data, Tree]:
    """ Create diff outputs. """
    header = {}
    data = calculate_diff(first_file, other_file)

    tables = data.get_table_names()
    if not tables:
        raise NoSharedVariables(
            f"Cannot generate diff file. Files '{first_file.file_name}' "
            f" and '{other_file.file_name} do not have any shared variables."
        )
    else:
        for table in tables:
            header[table] = data.get_variables_dct(table)

        tree = Tree()
        tree.populate_tree(header)

        return data, tree
