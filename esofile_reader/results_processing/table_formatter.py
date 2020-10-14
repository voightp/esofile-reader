import contextlib
import logging

import numpy as np
import pandas as pd

from esofile_reader.constants import *


class TableFormatter:
    """
    Define format of the table extracted via 'get_results_df' method.

    Parameters
    ----------
    file_name_position : ('row','column',None)
        Specify if file name should be added into results df.
    include_table_name : bool
        Decide if 'table' information should be included on
        the results df.
    include_day : bool
        Add day of week into index, this is applicable only for 'timestep',
        'hourly' and 'daily' outputs.
    include_id : bool
        Decide if variable 'id' should be included on the results df.
    timestamp_format : str
        Specified str format of a datetime timestamp.

    """

    def __init__(
        self,
        file_name_position: str = "row",
        include_table_name: bool = False,
        include_day: bool = False,
        include_id: bool = False,
        timestamp_format: str = "default",
    ):
        self.file_name_position = file_name_position
        self.include_table_name = include_table_name
        self.include_day = include_day
        self.include_id = include_id
        self.timestamp_format = timestamp_format

    def update_datetime_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Set specified 'datetime' str format. """
        # update DataFrame index
        if TIMESTAMP_COLUMN in df.index.names:
            ts_index = df.index.get_level_values(TIMESTAMP_COLUMN)
            if isinstance(ts_index, pd.DatetimeIndex):
                new_index = ts_index.strftime(self.timestamp_format)
                if isinstance(df.index, pd.MultiIndex):
                    df.index.set_levels(new_index, level=TIMESTAMP_COLUMN, inplace=True)
                else:
                    df.index = pd.Index(new_index, name=TIMESTAMP_COLUMN)
        # update DataFrame columns
        cond = (df.dtypes == np.dtype("datetime64[ns]")).to_list()
        df.loc[:, cond] = df.loc[:, cond].applymap(lambda x: x.strftime(self.timestamp_format))
        return df

    def add_file_name_level(self, name: str, df: pd.DataFrame) -> pd.DataFrame:
        """ Add file name to index. """
        pos = ["row", "column", "None"]  # 'None' is here only to inform
        if self.file_name_position not in pos:
            self.file_name_position = "row"
            logging.warning(
                f"Invalid name position!\n'add_file_name' kwarg must "
                f"be one of: '{', '.join(pos)}'.\nSetting 'row'."
            )

        axis = 0 if self.file_name_position == "row" else 1
        return pd.concat([df], axis=axis, keys=[name], names=["file"])

    def format_table(self, df: pd.DataFrame, file_name: str):
        """ Modify table columns and index levels. """
        if not self.include_id:
            df.columns = df.columns.droplevel(ID_LEVEL)
        if not self.include_table_name:
            df.columns = df.columns.droplevel(TABLE_LEVEL)
        if not self.include_day:
            with contextlib.suppress(KeyError):
                df.index = df.index.droplevel(DAY_COLUMN)
        if self.file_name_position:
            df = self.add_file_name_level(file_name, df)
        if self.timestamp_format != "default":
            df = self.update_datetime_format(df)
        return df
