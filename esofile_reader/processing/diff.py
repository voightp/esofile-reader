import pandas as pd

from esofile_reader.constants import N_DAYS_COLUMN, DAY_COLUMN, ID_LEVEL
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import ResultsFileType
from esofile_reader.tables.df_tables import DFTables


def process_diff(file: ResultsFileType, other_file: ResultsFileType) -> DFTables:
    """ Create diff outputs. """
    tables = DFTables()
    id_gen = incremental_id_gen()

    for table in file.table_names:
        try:
            # avoid different column indexes
            if file.tables.is_simple(table) != other_file.tables.is_simple(table):
                continue
        except KeyError:
            # table is not available on the other file
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
            tables[table] = df

            for c in [N_DAYS_COLUMN, DAY_COLUMN]:
                try:
                    c1 = file.tables.get_special_column(table, c).loc[index_cond]
                    c2 = file.tables.get_special_column(table, c).loc[index_cond]
                    if c1.equals(c2):
                        tables.insert_special_column(table, c, c1)
                except KeyError:
                    pass
    return tables
