from datetime import datetime
from typing import Type, Union, Tuple
import pandas as pd

from esofile_reader.base_file import BaseFile
from esofile_reader.constants import N_DAYS_COLUMN, DAY_COLUMN
from esofile_reader.outputs.df_data import DFData
from esofile_reader.outputs.base_data import BaseData
from esofile_reader.utils.search_tree import Tree
from esofile_reader.utils.utils import incremental_id_gen


class DiffFile(BaseFile):
    """
    A class to create results based on intersection
    of two results sets.

    """

    def __init__(self, first_file: Type[BaseFile], other_file: Type[BaseFile]):
        super().__init__()
        self.populate_content(first_file, other_file)

    @staticmethod
    def calculate_diff(file: Type[BaseFile], other_file: Type[BaseFile]) -> DFData:
        """ Calculate difference between two results files. """
        diff = DFData()
        id_gen = incremental_id_gen()

        for interval in file.available_intervals:
            df1 = file.as_df(interval)

            if interval not in other_file.available_intervals:
                continue

            df2 = other_file.as_df(interval)

            df1.columns = df1.columns.droplevel("id")
            df2.columns = df2.columns.droplevel("id")

            df = df1 - df2
            df.dropna(how="all", inplace=True, axis=1)

            if not df.empty:
                # create new id for each record
                ids = [next(id_gen) for _ in range(len(df.columns))]
                header_df = df.columns.to_frame(index=False)
                header_df.insert(0, "id", ids)

                df.columns = pd.MultiIndex.from_frame(header_df)

            try:
                c1 = file.data.get_number_of_days(interval)
                c2 = other_file.data.get_number_of_days(interval)
                if c1.equals(c2):
                    df.insert(0, N_DAYS_COLUMN, c1)
            except KeyError:
                pass

            try:
                c1 = file.data.get_days_of_week(interval)
                c2 = other_file.data.get_days_of_week(interval)
                if c1.equals(c2):
                    df.insert(0, DAY_COLUMN, c1)
            except KeyError:
                pass

            diff.populate_table(interval, df)

        return diff

    def process_diff(self, first_file: Type[BaseFile], other_file: Type[BaseFile]) -> Tuple[Type[BaseData], Tree]:
        """ Create diff outputs. """
        header = {}
        data = self.calculate_diff(first_file, other_file)

        for interval in data.get_available_intervals():
            header[interval] = data.get_variables_dct(interval)

        tree = Tree()
        tree.populate_tree(header)

        return data, tree

    def populate_content(self, first_file: Type[BaseFile], other_file: Type[BaseFile]) -> None:
        """ Populate file content. """
        self.file_path = None
        self.file_name = f"{first_file.file_name} - {other_file.file_name} - diff"
        self.file_created = datetime.utcnow()

        content = self.process_diff(first_file, other_file)

        if content:
            self._complete = True
            (self.data,
             self._search_tree) = content
