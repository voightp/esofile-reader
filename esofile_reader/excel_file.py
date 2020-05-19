import os
from datetime import datetime
from pathlib import Path
from typing import Union, List, Tuple

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

    def parse_header(
            self, df: pd.DataFrame, is_template: bool = True, n_header_rows: int = 2
    ) -> Tuple[pd.DataFrame, int]:
        """ Extract header related information from excel worksheet. """
        column_rows = {
            ID_LEVEL: None,
            INTERVAL_LEVEL: None,
            KEY_LEVEL: None,
            TYPE_LEVEL: None,
            UNITS_LEVEL: None
        }
        index_names = [TIMESTAMP_COLUMN, RANGE]
        index_name = None
        i = 0
        if is_template:
            for _, sr in df.iterrows():
                ix = sr.iloc[0]
                row = sr.iloc[1:]
                if ix in index_names:
                    if not any(row):
                        # line is empty, skip to next one
                        i += 1
                    break
                if isinstance(ix, (datetime, int)):
                    break
                try:
                    column_rows[ix] = row
                except KeyError:
                    raise KeyError(
                        f"Unexpected column name! "
                        f"Only {', '.join(column_rows.keys())} are allowed.")
                i += 1
            # only rows with defined levels will be processed
            column_rows = {k: v for k, v in column_rows.items() if v is not None}

            # at least key and level names are needed to identify variables
            if not KEY_LEVEL in column_rows.keys() and UNITS_LEVEL in column_rows.keys():
                raise InsuficientHeaderInfo(
                    f"Cannot process header!"
                    f" '{KEY_LEVEL}' and '{UNITS_LEVEL}' levels must be included."
                )
        else:
            if n_header_rows not in (2, 3):
                raise InsuficientHeaderInfo(
                    "Only two or three header row are accepted."
                    " Expected combinations are either:\n\t1 - key\n\tunits"
                    "\n..for two header rows or:\n\t1 - key\n\ttype\nunits"
                )
            keys = [KEY_LEVEL, TYPE_LEVEL, UNITS_LEVEL]
            if n_header_rows == 2:
                keys.remove(TYPE_LEVEL)
            for i, key in enumerate(keys):
                column_rows[key] = df.iloc[i, :]

        # transpose DataFrame to get items in columns
        header_df = pd.DataFrame(column_rows).T

        return header_df, i

    def process_excel(
            self,
            file_path: Union[str, Path],
            sheet_names: List[str] = None,
            is_template: bool = True,
    ):
        """ Create results file data based on given excel workbook."""
        wb = load_workbook(filename=file_path, read_only=True)
        if not sheet_names:
            sheet_names = wb.sheetnames

        df_data = DFData()
        for name in sheet_names:
            ws = wb[name]
            df = pd.DataFrame(ws.values)
            header_df, skiprows = self.parse_header(
                df.iloc[:self.HEADER_LIMIT, :], is_template=is_template
            )
            df = df.iloc[skiprows:, :]
            if is_template:
                df.set_index(keys=0, inplace=True)
                if isinstance(df.index, pd.DatetimeIndex):
                    df.index = df.index.round(freq="S")

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
