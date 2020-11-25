import pandas as pd

from esofile_reader.df.df_tables import DFTables
from esofile_reader.df.level_names import ID_LEVEL
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import ResultsFileType
from esofile_reader.processing.progress_logger import BaseLogger


def can_subtract_table(table: str, file: ResultsFileType, other_file: ResultsFileType) -> bool:
    """ Check if tables can be subtracted. """
    try:
        # avoid different column indexes
        if file.tables.is_simple(table) != other_file.tables.is_simple(table):
            return False
    except KeyError:
        # table is not available on the other file
        return False
    return True


def subtract_tables(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """ Get 'difference' DataFrame for matching index and columns. """
    index_cond = df1.index.intersection(df2.index).tolist()
    columns_cond = df1.columns.intersection(df2.columns).tolist()
    df = df1.loc[index_cond, columns_cond] - df2.loc[index_cond, columns_cond]
    df.dropna(how="all", inplace=True, axis=1)
    return df


def get_shared_special_table(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """ Get shared special columns for matching indexes and data. """
    index_cond = df1.index.intersection(df2.index).tolist()
    columns_cond = df1.columns.intersection(df2.columns).tolist()
    df1 = df1.loc[index_cond, columns_cond]
    df2 = df2.loc[index_cond, columns_cond]
    same_columns = [df1.loc[:, c].equals(df2.loc[:, c]) for c in df1.columns]
    return df1.loc[:, same_columns]


def process_diff(
    file: ResultsFileType, other_file: ResultsFileType, logger: BaseLogger
) -> DFTables:
    """ Create diff outputs. """
    logger.log_section("generating file difference")
    logger.set_maximum_progress(len(file.table_names) + 1)
    tables = DFTables()
    id_gen = incremental_id_gen()
    for table in file.table_names:
        if can_subtract_table(table, file, other_file):
            df1 = file.get_numeric_table(table)
            df2 = other_file.get_numeric_table(table)
            df1.columns = df1.columns.droplevel(ID_LEVEL)
            df2.columns = df2.columns.droplevel(ID_LEVEL)
            df = subtract_tables(df1, df2)
            if not df.empty:
                # create new id for each record
                ids = [next(id_gen) for _ in range(len(df.columns))]
                header_df = df.columns.to_frame(index=False)
                header_df.insert(0, ID_LEVEL, ids)
                df.columns = pd.MultiIndex.from_frame(header_df)

                special_df1 = file.get_special_table(table)
                special_df2 = file.get_special_table(table)
                special_df = get_shared_special_table(special_df1, special_df2)

                tables[table] = pd.concat([special_df, df], axis=1, sort=False)
        logger.increment_progress()
    return tables
