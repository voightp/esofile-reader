from esofile_reader.base_file import BaseFile
from esofile_reader.constants import N_DAYS_COLUMN, DAY_COLUMN
from esofile_reader.utils.mini_classes import Variable
from esofile_reader.utils.utils import incremental_id_gen
from esofile_reader.utils.tree import Tree
from esofile_reader.outputs.outputs import Outputs
from datetime import datetime

import pandas as pd


def calculate_diff(first_file, other_file, absolute=False,
                   include_id=False, include_interval=False):
    """ Calculate difference between two results files. """
    diff = {}
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

                diff[interval] = Outputs(df)

        except MemoryError:
            raise MemoryError("Cannot subtract output DataFrames!"
                              "\nRunning out of memory!")

        for c in [N_DAYS_COLUMN, DAY_COLUMN]:
            try:
                if other_file.data_set(interval)[c].equals(first_file.data_set(interval)[c]):
                    df.insert(0, c, first_file.data_set(interval)[c])
            except KeyError:
                pass

    return diff


class DiffFile(BaseFile):
    def __init__(self, first_file, other_file):
        super().__init__()
        self.populate_content(first_file, other_file)

    @staticmethod
    def build_header_dict(header_df):
        """ Transform header df into header dict. """

        def header_vars(sr):
            return Variable(sr.interval, sr.key, sr.variable, sr.units)

        # drop special columns, those are included on outputs df
        cond = [id_ not in (DAY_COLUMN, N_DAYS_COLUMN) for id_ in header_df.id]
        header_df = header_df.loc[cond, :]

        header_df.set_index(keys="id", inplace=True)
        header_df = header_df.apply(header_vars, axis=1)

        return header_df.to_dict()

    @staticmethod
    def process_diff(first_file, other_file):
        """ Create diff outputs. """
        header = {}
        outputs = calculate_diff(first_file, other_file, include_id=True, include_interval=True)

        for interval, df in outputs.items():
            header[interval] = df.header_variables_dct

        tree = Tree()
        tree.populate_tree(header)

        return outputs, tree

    def populate_content(self, first_file, other_file):
        """ Populate file content. """
        self.file_path = None
        self.file_name = f"{first_file.file_name} - {other_file.file_name} - diff"
        self.file_timestamp = datetime.utcnow().timestamp()

        content = self.process_diff(first_file, other_file)

        if content:
            self._complete = True
            (self._outputs,
             self.header_tree) = content
