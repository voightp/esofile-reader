from datetime import datetime
from pathlib import Path
from unittest import TestCase

import pandas as pd
import pyarrow.parquet as pq
from pandas.testing import assert_frame_equal, assert_index_equal

from esofile_reader.data.pqt_data import ParquetFrame

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
        names = ["id", "interval", "key", "type", "units"]
        test_columns = pd.MultiIndex.from_tuples(test_variables, names=names)
        test_index = pd.DatetimeIndex(
            pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp"
        )
        self.test_df = pd.DataFrame(
            [
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            ],
            columns=test_columns,
            index=test_index,
        )

        ParquetFrame.CHUNK_SIZE = 3
        global i
        self.pqf = ParquetFrame.from_df(self.test_df, f"test-{i}")
        i += 1

    def tearDown(self) -> None:
        self.pqf.clean_up()
        self.pqf = None

    def test_name(self):
        global i
        self.assertEqual(f"table-test-{i - 1}", self.pqf.name)

    def test_index(self):
        assert_index_equal(
            pd.DatetimeIndex(pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp"),
            self.pqf.index,
        )

    def test_index_setter(self):
        new_index = pd.DatetimeIndex(
            pd.date_range("2003-1-1", freq="d", periods=3), name="timestamp"
        )
        self.pqf.index = new_index
        assert_index_equal(new_index, self.pqf.index)

        for path in self.pqf.chunk_paths:
            tbl = pq.read_pandas(path).to_pandas()
            assert_index_equal(new_index, tbl.index)

    def test_columns(self):
        assert_index_equal(self.test_df.columns, self.pqf.columns)

    def test_columns_setter(self):
        new_variables = [
            (1, "daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
            (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
            (3, "daily", "THIS IS", "NEW", "VARIABLE"),
            (4, "daily", "BLOCK1:ZONE1", "Heating Load", "W"),
            (5, "daily", "BLOCK1:ZONE1_WALL_3_0_0_0_0_0_WIN", "Window Gain", "W"),
            (6, "this", "is", "new", "index"),
            (0, "daily", "BLOCK1:ZONE1_WALL_5_0_0_0_0_0_WIN", "Window Gain", "W"),
            (8, "daily", "BLOCK1:ZONE1_WALL_6_0_0_0_0_0_WIN", "Window Lost", "W"),
            (9, "daily", "BLOCK1:ZONE1_WALL_5_0_0", "Wall Gain", "W"),
            (10, "daily", "THIS IS ALSO", "ANOTHER NEW", "VARIABLE"),
            (11, "daily", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            (12, "daily", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
            (13, "daily", "Some Flow 1", "Mass Flow", "kg/s"),
            (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s"),
        ]
        names = ["id", "interval", "key", "type", "units"]
        new_columns = pd.MultiIndex.from_tuples(new_variables, names=names)
        self.pqf.columns = new_columns
        self.test_df.columns = new_columns

        assert_index_equal(new_columns, self.pqf.columns)
        assert_index_equal(new_columns, self.pqf.get_df().columns)

        assert_frame_equal(
            self.pqf[(6, "this", "is", "new", "index")],
            self.test_df[[(6, "this", "is", "new", "index")]]
        )
        assert_frame_equal(self.pqf[6], self.test_df[[6]])

    def test_columns_setter_invalid_class(self):
        with self.assertRaises(IndexError):
            self.pqf.columns = list("abcdefghijklmn")

    def test_columns_setter_invalid_count(self):
        with self.assertRaises(IndexError):
            self.pqf.columns = pd.Index(list("abcdefghijklm"))

    def test_column_indexing_df(self):
        assert_frame_equal(self.test_df[[2]], self.pqf[[2]])

    def test_column_indexing_multiple(self):
        assert_frame_equal(self.test_df[[2, 5, 8]], self.pqf[[2, 5, 8]])

    def test_column_indexing_mi_list(self):
        cols = [
            (11, "daily", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            (9, "daily", "BLOCK1:ZONE1_WALL_5_0_0", "Wall Gain", "W"),
            (10, "daily", "BLOCK1:ZONE2_WALL_4_8_9", "Wall Gain", "W"),
            (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
        ]
        assert_frame_equal(self.test_df[cols], self.pqf[cols])

    def test_column_indexing_missing(self):
        cols = [
            (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
            (1000, "some", "invalid", "variable", "")
        ]
        with self.assertRaises(KeyError):
            _ = self.pqf[cols]

    def test_column_indexing_mi(self):
        assert_frame_equal(
            self.test_df[[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")]],
            self.pqf[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")],
        )

    def test_column_indexing_missing_string(self):
        with self.assertRaises(KeyError):
            _ = self.pqf["invalid"]

    def test_column_indexing_invalid_tuple(self):
        with self.assertRaises(IndexError):
            print(self.pqf[("invalid",)])

    def test_column_indexing_invalid_mixed_type(self):
        with self.assertRaises(IndexError):
            _ = self.pqf[[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"), 6]],

    def test_loc_slice_rows(self):
        assert_frame_equal(
            self.test_df.loc[datetime(2002, 1, 1): datetime(2002, 1, 2)],
            self.pqf.loc[datetime(2002, 1, 1): datetime(2002, 1, 2)],
        )

    def test_loc(self):
        assert_frame_equal(
            self.test_df.loc[datetime(2002, 1, 1): datetime(2002, 1, 2), [2]],
            self.pqf.loc[datetime(2002, 1, 1): datetime(2002, 1, 2), [2]],
        )

    def test_invalid_loc(self):
        with self.assertRaises(IndexError):
            _ = self.pqf.loc[:, ["a", "b", "c", 1.1234]]

    def test_setter_new_var(self):
        new_col = [1, 2, 3]
        new_var = (20, "daily", "new", "dummy", "type")
        self.test_df[new_var] = new_col
        self.pqf[new_var] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_setter_update_var(self):
        new_col = [1, 2, 3]
        new_var = (20, "daily", "new", "dummy", "type")
        self.test_df[new_var] = new_col
        self.pqf[new_var] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_loc_setter(self):
        new_col = [1, 2, 3]
        var = (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s")
        self.test_df.loc[:, var] = new_col
        self.pqf.loc[:, var] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_loc_setter_id(self):
        new_col = [2, 3, 4]
        var = (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s")
        self.test_df.loc[:, var] = new_col
        self.pqf.loc[:, var[0]] = new_col
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_loc_setter_all(self):
        self.test_df.loc[:] = 1
        self.pqf.loc[:] = 1
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

    def test_loc_invalid_setter(self):
        with self.assertRaises(TypeError):
            self.pqf.loc[:, 1] = pd.DataFrame({"a": [1, 2, 3]})

    def test_update_parquet(self):
        df = pd.DataFrame([[1], [2], [3]], columns=pd.Index(["a"], name="id"))
        self.pqf.save_df_to_parquet("test_parquet.parquet", df)
        self.assertTrue(Path(self.pqf.workdir, "test_parquet.parquet").exists())

    def get_full_df(self):
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_store_df(self):
        self.pqf.clean_up()  # clean setUp storage

        # save each column as an independent parquet
        ParquetFrame.CHUNK_SIZE = 1
        self.pqf = ParquetFrame.from_df(self.test_df, "some_name")
        self.assertEqual(14, len(list(self.pqf.workdir.iterdir())))
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_insert_column_start(self):
        self.pqf.insert(0, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
        self.test_df.insert(0, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
        assert_frame_equal(self.pqf.get_df(), self.test_df)

    def test_insert_column_middle(self):
        self.pqf.insert(5, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
        self.test_df.insert(5, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
        assert_frame_equal(self.pqf.get_df(), self.test_df)

    def test_insert_column_end(self):
        self.pqf.insert(14, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
        self.test_df.insert(14, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
        assert_frame_equal(self.pqf.get_df(), self.test_df)

    def test_insert_column_invalid(self):
        with self.assertRaises(IndexError):
            self.pqf.insert(25, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])

    def test_insert_into_empty_frame(self):
        cols = self.test_df.columns.tolist()
        self.test_df.drop(columns=cols, inplace=True, axis=1)
        self.pqf.drop(columns=cols, inplace=True, axis=1)
        self.test_df["foo"] = [1, 2, 3]
        self.pqf["foo"] = [1, 2, 3]
        assert_frame_equal(self.test_df, self.pqf.get_df(), check_column_type=False)

    def test_drop(self):
        self.test_df.drop(columns=[6, 10], inplace=True, level="id")
        self.pqf.drop(columns=[6, 10], inplace=True, level="id")
        assert_frame_equal(self.test_df, self.pqf.get_df())

    def test_drop_invalid_level(self):
        with self.assertRaises(IndexError):
            self.pqf.drop(columns=[1, 2, 3], level="foo")

    def test_drop_all(self):
        cols = self.test_df.columns.tolist()
        self.test_df.drop(columns=cols, inplace=True, axis=1)
        self.pqf.drop(columns=cols, inplace=True)
        self.assertTrue(self.pqf.get_df().empty)
        assert_frame_equal(self.test_df, self.pqf.get_df(), check_column_type=False)

        # add dummy variable to check frame
        self.test_df["foo"] = [1, 2, 3]
        self.pqf["foo"] = [1, 2, 3]
        assert_frame_equal(self.test_df, self.pqf.get_df(), check_column_type=False)

    def test_save_load_info_parquets(self):
        self.pqf.save_info_parquets()
        self.assertTrue(Path(self.pqf.workdir, ParquetFrame.INDEX_PARQUET).exists())
        self.assertTrue(Path(self.pqf.workdir, ParquetFrame.COLUMNS_PARQUET).exists())
        self.assertTrue(Path(self.pqf.workdir, ParquetFrame.CHUNKS_PARQUET).exists())

        test_chunks = self.pqf._chunks_table.copy()
        self.pqf._index = None
        self.pqf._columns = None
        self.pqf._chunks_table = None

        self.pqf.load_info_parquets()
        assert_index_equal(self.test_df.index, self.pqf.index)
        assert_index_equal(self.test_df.columns, self.pqf.columns)
        assert_frame_equal(test_chunks, self.pqf._chunks_table)

    def test_load_missing_parquets(self):
        self.pqf.save_info_parquets()
        index_path = Path(self.pqf.workdir, self.pqf.INDEX_PARQUET)
        index_path.unlink()

        with self.assertRaises(FileNotFoundError):
            self.pqf.load_info_parquets()

    def test_parquet_frame_context_maneger(self):
        with ParquetFrame(df=self.test_df, name="test") as pqf:
            assert_frame_equal(self.test_df, pqf.get_df())
        self.assertFalse(pqf.workdir.exists())
