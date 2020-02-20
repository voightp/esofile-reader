from unittest import TestCase
import pandas as pd
from esofile_reader.data.pqt_data import ParquetFrame
from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import numpy as np

# global incrementor to create unique parquet file for each test
i = 0


class TestParquetFrame(TestCase):

    def setUp(self) -> None:
        test_variables = [
            (1, "daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
            (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
            (3, "daily", "BLOCK1:ZONE3", "Zone Temperature", "C"),
            (4, "daily", "BLOCK1:ZONE1", "Heating Load", "W"),
            (5, "daily", "BLOCK1:ZONE1_WALL_3_0_0_0_0_0_WIN", "Window Gain", "W"),
            (6, "daily", "BLOCK1:ZONE1_WALL_4_0_0_0_0_0_WIN", "Window Gain", "W"),
            (0, "daily", "BLOCK1:ZONE1_WALL_5_0_0_0_0_0_WIN", "Window Gain", "W"),
            (8, "daily", "BLOCK1:ZONE1_WALL_6_0_0_0_0_0_WIN", "Window Lost", "W"),
            (9, "daily", "BLOCK1:ZONE1_WALL_5_0_0", "Wall Gain", "W"),
            (10, "daily", "BLOCK1:ZONE2_WALL_4_8_9", "Wall Gain", "W"),
            (11, "daily", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            (12, "daily", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
            (13, "daily", "Some Flow 1", "Mass Flow", "kg/s"),
            (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s"),
        ]
        names = ["id", "interval", "key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(test_variables, names=names)
        test_index = pd.DatetimeIndex(
            pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp"
        )
        self.test_df = pd.DataFrame([
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
        ], columns=test_columns, index=test_index)

        ParquetFrame.CHUNK_SIZE = 3
        global i
        self.pqf = ParquetFrame(self.test_df, f"test-{i}")
        i += 1

    def tearDown(self) -> None:
        self.pqf = None

    def test_index(self):
        assert_index_equal(
            pd.DatetimeIndex(pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp"),
            self.pqf.index
        )

    def test_index_setter(self):
        new_index = pd.DatetimeIndex(pd.date_range("2003-1-1", freq="d", periods=3), name="timestamp")
        self.pqf.index = new_index
        assert_index_equal(new_index, self.pqf.index)

        for path in self.pqf.chunk_paths:
            tbl = pq.read_pandas(path).to_pandas()
            assert_index_equal(new_index, tbl.index)

    def test_columns(self):
        assert_index_equal(
            self.test_df.columns,
            self.pqf.columns
        )

    def test_columns_setter(self):
        new_variables = [
            (1, "daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
            (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
            (3, "daily", "THIS IS", "NEW", "VARIABLE"),
            (4, "daily", "BLOCK1:ZONE1", "Heating Load", "W"),
            (5, "daily", "BLOCK1:ZONE1_WALL_3_0_0_0_0_0_WIN", "Window Gain", "W"),
            (6, "daily", "BLOCK1:ZONE1_WALL_4_0_0_0_0_0_WIN", "Window Gain", "W"),
            (0, "daily", "BLOCK1:ZONE1_WALL_5_0_0_0_0_0_WIN", "Window Gain", "W"),
            (8, "daily", "BLOCK1:ZONE1_WALL_6_0_0_0_0_0_WIN", "Window Lost", "W"),
            (9, "daily", "BLOCK1:ZONE1_WALL_5_0_0", "Wall Gain", "W"),
            (10, "daily", "THIS IS ALSO", "ANOTHER NEW", "VARIABLE"),
            (11, "daily", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            (12, "daily", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
            (13, "daily", "Some Flow 1", "Mass Flow", "kg/s"),
            (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s"),
        ]
        names = ["id", "interval", "key", "variable", "units"]
        new_columns = pd.MultiIndex.from_tuples(new_variables, names=names)
        self.pqf.columns = new_columns

        assert_index_equal(
            new_columns,
            self.pqf.columns
        )

        assert_index_equal(
            new_columns,
            self.pqf.get_df().columns
        )

    def test_columns_setter_invalid_class(self):
        with self.assertRaises(IndexError):
            self.pqf.columns = list("abcdefghijklmn")

    def test_columns_setter_invalid_count(self):
        with self.assertRaises(IndexError):
            self.pqf.columns = pd.Index(list("abcdefghijklm"))

    def test_column_indexing_sr(self):
        # the indexing behaviour works a bit strange here.
        # default 'self.test_df[2]' would return pd.DataFrame
        # with truncated multiindex
        assert_series_equal(
            self.test_df[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")],
            self.pqf[2]
        )

    def test_column_indexing_df(self):
        assert_frame_equal(self.test_df[[2]], self.pqf[[2]])

    def test_column_indexing_multiple(self):
        assert_frame_equal(self.test_df[[2, 5, 8]], self.pqf[[2, 5, 8]])

    def test_column_indexing_mi(self):
        assert_series_equal(
            self.test_df[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")],
            self.pqf[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")]
        )

    def test_column_indexing_mi_list(self):
        assert_frame_equal(
            self.test_df[[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")]],
            self.pqf[[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")]]
        )

    def test_column_indexing_invalid(self):
        with self.assertRaises(KeyError):
            print(self.pqf["invalid"])

    def test_column_indexing_invalid_tuple(self):
        with self.assertRaises(KeyError):
            print(self.pqf[("invalid")])

    def test_loc_slice_rows(self):
        assert_frame_equal(
            self.test_df.loc[datetime(2002, 1, 1): datetime(2002, 1, 2)],
            self.pqf.loc[datetime(2002, 1, 1): datetime(2002, 1, 2)]
        )

    def test_loc(self):
        assert_frame_equal(
            self.test_df.loc[datetime(2002, 1, 1): datetime(2002, 1, 2), [2]],
            self.pqf.loc[datetime(2002, 1, 1): datetime(2002, 1, 2), [2]]
        )

    def test_setter_new_var(self):
        new_col = [1, 2, 3]
        new_var = (20, "daily", "new", "dummy", "variable")
        self.test_df[new_var] = new_col
        self.pqf[new_var] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_setter_update_var(self):
        new_col = [1, 2, 3]
        new_var = (20, "daily", "new", "dummy", "variable")
        self.test_df[new_var] = new_col
        self.pqf[new_var] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_loc_setter(self):
        new_col = [1, 2, 3]
        var = (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s")
        self.test_df.loc[:, var] = new_col
        self.pqf.loc[:, var] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_loc_setter_boolean_arr(self):
        new_col = [1, 2, 3]
        arr = [False] * 7 + [True] + [False] * 6
        self.test_df.loc[:, arr] = new_col
        self.pqf.loc[:, arr] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_loc_sliced_setter(self):
        new_col = [1, 2]
        var = (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s")
        self.test_df.loc[datetime(2002, 1, 1): datetime(2002, 1, 2), var] = new_col
        self.pqf.loc[datetime(2002, 1, 1): datetime(2002, 1, 2), var] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_store_parquet(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        self.pqf.store_parquet("test_parquet.parquet", df)
        self.assertTrue(Path(self.pqf.root_path, "test_parquet.parquet").exists())

    def test_update_parquet(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        self.pqf.store_parquet("test_parquet.parquet", df)
        self.assertTrue(Path(self.pqf.root_path, "test_parquet.parquet").exists())

    def get_full_df(self):
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_store_df(self):
        # save each column as an independent parquet
        ParquetFrame.CHUNK_SIZE = 1
        self.pqf = ParquetFrame(self.test_df, "some_name")
        self.assertEqual(14, len(list(self.pqf.root_path.iterdir())))
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_insert_column(self):
        self.pqf.insert_column(((100, "this", "is", "dummy", "variable")), ["a", "b", "c"])
        assert_series_equal(
            pd.Series(
                ["a", "b", "c"],
                name=(100, "this", "is", "dummy", "variable"),
                index=pd.Index(pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp")
            ),
            self.pqf[100]
        )

    def test_drop(self):
        self.test_df.drop(columns=[6, 10], inplace=True, level="id")
        self.pqf.drop(columns=[6, 10], inplace=True, level="id")
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_drop_all(self):
        self.test_df.drop(
            columns=self.test_df.columns.get_level_values("id").tolist(), inplace=True, level="id"
        )

        self.pqf.drop(
            columns=self.pqf.columns.get_level_values("id").tolist(), inplace=True, level="id"
        )

        self.assertTrue(self.pqf.get_df().empty)
        assert_frame_equal(self.test_df, self.pqf.get_df(), check_column_type=False)

        # add dummy variable to check frame
        self.test_df["foo"] = [1, 2, 3]
        self.pqf["foo"] = [1, 2, 3]
        assert_frame_equal(self.test_df, self.pqf.get_df(), check_column_type=False)
