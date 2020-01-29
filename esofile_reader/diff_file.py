from datetime import datetime

import pandas as pd

from esofile_reader.base_file import BaseFile
from esofile_reader.constants import N_DAYS_COLUMN, DAY_COLUMN
from esofile_reader.outputs.df_data import DFData
from esofile_reader.utils.search_tree import Tree
from esofile_reader.utils.utils import incremental_id_gen


class DiffFile(BaseFile):
    """
    A class to create results based on intersection
    of two results sets.

    """

    def __init__(self, first_file, other_file):
        super().__init__()
        self.populate_content(first_file, other_file)

    @staticmethod
    def calculate_diff(first_file, other_file, absolute=False,
                       include_id=False, include_interval=False):
        """ Calculate difference between two results files. """
        diff = DFData()
        id_gen = incremental_id_gen()

        for interval in first_file.available_intervals:
            df1 = first_file.as_df(interval)

            if interval not in other_file.available_intervals:
                continue

            df2 = other_file.as_df(interval)

            df1.columns = df1.columns.droplevel("id")
            df2.columns = df2.columns.droplevel("id")

            if not include_interval:
                df1.columns = df1.columns.droplevel("interval")
                df2.columns = df2.columns.droplevel("interval")

            try:
                df = df1 - df2
                df.dropna(how="all", inplace=True, axis=1)

                if not df.empty:
                    if absolute:
                        df = df.abs()

                    if include_id:
                        # create new id for each record
                        ids = [next(id_gen) for _ in range(len(df.columns))]
                        header_df = df.columns.to_frame(index=False)
                        header_df.insert(0, "id", ids)

                        df.columns = pd.MultiIndex.from_frame(header_df)

            except MemoryError:
                raise MemoryError("Cannot subtract output DataFrames!"
                                  "\nRunning out of memory!")

            for c in [N_DAYS_COLUMN, DAY_COLUMN]:
                try:
                    c1 = first_file.data.get_special_column(c, interval)
                    c2 = other_file.data.get_special_column(c, interval)
                    if c1.equals(c2):
                        df.insert(0, c, c1)
                except KeyError:
                    pass

            diff.set_data(interval, df)

        return diff

    def process_diff(self, first_file, other_file, absolute=False):
        """ Create diff outputs. """
        header = {}
        outputs = self.calculate_diff(first_file, other_file, absolute=absolute,
                                      include_id=True, include_interval=True)

        for interval in outputs.get_available_intervals():
            header[interval] = outputs.get_variables_dct(interval)

        tree = Tree()
        tree.populate_tree(header)

        return outputs, tree

    def populate_content(self, first_file, other_file):
        """ Populate file content. """
        self.file_path = None
        self.file_name = f"{first_file.file_name} - {other_file.file_name} - diff"
        self.file_created = datetime.utcnow()

        content = self.process_diff(first_file, other_file)

        if content:
            self._complete = True
            (self.data,
             self._search_tree) = content
