import os
from datetime import datetime
from pathlib import Path
from typing import Union, List, Tuple

import numpy as np
import pandas as pd
from openpyxl import load_workbook

from esofile_reader.base_file import BaseFile
from esofile_reader.constants import *
from esofile_reader.data.df_data import DFData
from esofile_reader.exceptions import InsuficientHeaderInfo
from esofile_reader.processor.monitor import DefaultMonitor


class ExcelFile(BaseFile):
    """ Create results file based on excel data. """
    HEADER_LIMIT = 10

    def __init__(self, file_path: Union[str, Path], monitor: DefaultMonitor = None):
        super().__init__()
        self.file_path = file_path
        self.populate_content(monitor)

    @staticmethod
    def is_data_row(sr):
        print(sr)
        return all(sr.apply(lambda x: isinstance(x, (int, float))))

    def parse_header(
            self, df: pd.DataFrame, force_index: bool = False,
    ) -> Tuple[pd.DataFrame, int, bool]:
        """ Extract header related information from excel worksheet. """
        index_names = [TIMESTAMP_COLUMN, RANGE]
        column_levels = [
            ID_LEVEL,
            INTERVAL_LEVEL,
            KEY_LEVEL,
            TYPE_LEVEL,
            UNITS_LEVEL,
        ]
        first_column = df.iloc[:, 0].tolist()

        # try to find out if DataFrame includes index column
        if force_index:
            index_column = True
        else:
            # check if DataFrame include index column
            index_column = False
            for cell in first_column:
                conditions = [
                    cell in index_names,
                    isinstance(cell, (datetime, np.datetime64)),
                ]
                if any(conditions):
                    index_column = True
                    break

        # check if DataFrame has a 'template' structure
        if index_column and KEY_LEVEL in first_column and UNITS_LEVEL in first_column:
            is_template = True
        else:
            is_template = False

        levels = {}
        i = 0
        for _, sr in df.iterrows():
            if index_column:
                ix = sr.iloc[0]
                row = sr.iloc[1:]
                if row.dropna(how="all").empty:
                    # ignore rows without any value
                    pass
                elif is_template:
                    if ix in index_names:
                        # hit either RANGE or TIMESTAMP key
                        i += 1
                        break
                    elif isinstance(ix, (datetime, int)):
                        # hit integer range or datetime timestamp
                        break
                    elif ix in column_levels:
                        if ix == ID_LEVEL:
                            # ID will be re-generated to avoid necessary validation
                            pass
                        else:
                            levels[ix] = row
                    else:
                        print(f"Unexpected column identifier: {ix}"
                              f"Only {', '.join(COLUMN_LEVELS)} are allowed.")
                else:
                    if self.is_data_row(row.fillna(0)):
                        # hit actual data rows
                        break
                    else:
                        levels[i] = row
            else:
                if self.is_data_row(sr.fillna(0)):
                    break
                levels[i] = sr.values
            i += 1
        else:
            raise InsuficientHeaderInfo(
                "Failed to automatically retrieve header information from DataFrame!"
            )

        # validate gathered data
        ordered_levels = {}
        if is_template:
            if KEY_LEVEL not in levels or UNITS_LEVEL not in levels:
                raise InsuficientHeaderInfo(
                    f"Cannot process header!"
                    f" '{KEY_LEVEL}' and '{UNITS_LEVEL}' levels must be included."
                )
            # reorder levels using standard order
            ordered_levels = {lev: levels[lev] for lev in column_levels if lev in levels}
        else:
            n = len(levels)
            msg = " expected levels are either:\n\t1 - key\n\tunits" \
                  "\n..for two level header or:\n\t1 - key\n\ttype\nunits" \
                  " for three level header."
            if n < 2:
                raise InsuficientHeaderInfo(
                    f"Not enough information to create header. "
                    f"There's only {n} but {msg}"
                )
            elif n > 3:
                raise InsuficientHeaderInfo(
                    f"Too many header levels - {n}\n{msg}"
                )
            keys = [KEY_LEVEL, TYPE_LEVEL, UNITS_LEVEL]
            if n == 2:
                keys.remove(TYPE_LEVEL)
            for key, row in zip(keys, list(levels.values())):
                ordered_levels[key] = row

        # transpose DataFrame to get items in columns
        header_df = pd.DataFrame(ordered_levels).T

        return header_df, i, index_column

    def process_excel(
            self,
            file_path: Union[str, Path],
            sheet_names: List[str] = None,
            force_index: bool = False,
    ):
        """ Create results file data based on given excel workbook."""
        wb = load_workbook(filename=file_path, read_only=True)
        if not sheet_names:
            sheet_names = wb.sheetnames

        df_data = DFData()
        for name in sheet_names:
            ws = wb[name]
            df = pd.DataFrame(ws.values)

            # remove empty rows and columns
            df.dropna(axis=0, how="all", inplace=True)
            df.dropna(axis=1, how="all", inplace=True)

            header_df, skiprows, index_column = self.parse_header(
                df.iloc[:self.HEADER_LIMIT, :], force_index=force_index
            )
            print(header_df)
            df = df.iloc[skiprows:, :]
            if index_column:
                df.set_index(keys=0, inplace=True)
                if isinstance(df.index, pd.DatetimeIndex):
                    df.index = df.index.round(freq="S")
                    df.index.rename(TIMESTAMP_COLUMN, inplace=True)
                else:
                    df.index.rename(RANGE)
            print(df)

            # names = list(column_rows.keys())
            # header = list(column_rows.values())
            # index_col = 0 if index_column else False
            #
            # df = pd.read_excel(
            #     file_path,
            #     sheet_name=name,
            #     header=header,
            #     # index_col=index_col,
            #     engine="openpyxl",
            #     parse_dates=True,
            #     # skiprows=skiprows
            # )
            # print(df)

            # create table for each interval name
            if INTERVAL_LEVEL in df.columns.names:
                grouped = df.groupby(axis=1, level=INTERVAL_LEVEL, sort=False)
                # print(grouped)

        return False, False

    def populate_content(self, monitor: DefaultMonitor = None) -> None:
        self.file_name = Path(self.file_path).stem
        self.file_created = datetime.utcfromtimestamp(os.path.getctime(self.file_path))
        self.data, self.search_tree = self.process_excel(self.file_path)
