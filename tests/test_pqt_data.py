from unittest import TestCase
import pandas as pd
from esofile_reader.data.pqt_data import ParquetFrame
from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

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
            self.pqf.get_full_df().columns
        )

    def test_columns_setter_invalid_class(self):
        with self.assertRaises(IndexError):
            self.pqf.columns = list("abcdefghijklmn")

    def test_columns_setter_invalid_count(self):
        with self.assertRaises(IndexError):
            self.pqf.columns = pd.Index(list("abcdefghijklm"))

    def test_loc(self):
        self.pqf[:]

    # def test_store_parquet(self):
    #     self.fail()
    #
    # def test_update_parquet(self):
    #     self.fail()
    #
    # def get_full_df(self):
    #     self.fail()
    #
    # def test_get_df(self):
    #     self.fail()
    #
    # def test_create_chunk(self):
    #     self.fail()
    #
    # def test_store_df(self):
    #     self.fail()
    #
    # def test_insert(self):
    #     self.fail()
    #
    # def test_get_all_chunk_id_pairs(self):
    #     self.fail()
    #
    # def test_get_chunk_id_pairs(self):
    #     self.fail()
    #
    # def test_drop(self):
    #     self.fail()
    #
    # def test_delete_column_items(self):
    #     self.fail()
    #
    # def test_update_column_item(self):
    #     self.fail()
    #
    # def test_add_column_item(self):
    #     self.fail()
