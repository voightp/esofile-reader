import tempfile
from copy import copy
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from pandas.testing import assert_frame_equal, assert_index_equal

from esofile_reader.pqt.parquet_tables import ParquetFrame, parquet_frame_factory
from tests.session_fixtures import ROOT_PATH


@pytest.fixture
def test_df():
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
    # hack to avoid setting check_freq for each pandas assert
    test_index.freq = None
    test_df = pd.DataFrame(
        [
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
        ],
        columns=test_columns,
        index=test_index,
    )
    return test_df


@pytest.fixture
def parquet_frame(test_df):
    with tempfile.TemporaryDirectory(dir=Path(ROOT_PATH, "storages")) as temp_dir:
        ParquetFrame.CHUNK_SIZE = 3
        parquet_frame = ParquetFrame.from_df(test_df, f"test", pardir=temp_dir)
        try:
            yield parquet_frame
        finally:
            parquet_frame.clean_up()


def test_name(parquet_frame):
    assert parquet_frame.name == "table-test"


def test_index(parquet_frame, test_df):
    assert_index_equal(parquet_frame.index, test_df.index)


def test_index_setter(parquet_frame):
    new_index = pd.DatetimeIndex(
        pd.date_range("2003-1-1", freq="d", periods=3), name="timestamp"
    )
    parquet_frame.index = new_index
    assert_index_equal(new_index, parquet_frame.index)

    for path in parquet_frame.chunk_paths:
        tbl = pq.read_pandas(path).to_pandas()
        assert_index_equal(new_index, tbl.index)


def test_columns(parquet_frame, test_df):
    assert_index_equal(parquet_frame.columns, test_df.columns)


def test_columns_setter(parquet_frame, test_df):
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
    parquet_frame.columns = new_columns
    test_df.columns = new_columns

    assert_index_equal(new_columns, parquet_frame.columns)
    assert_index_equal(new_columns, parquet_frame.get_df().columns)

    assert_frame_equal(
        parquet_frame[(6, "this", "is", "new", "index")],
        test_df[[(6, "this", "is", "new", "index")]],
    )
    assert_frame_equal(parquet_frame[6], test_df[[6]])


def test_columns_setter_invalid_class(parquet_frame):
    with pytest.raises(IndexError):
        parquet_frame.columns = list("abcdefghijklmn")


def test_columns_setter_invalid_count(parquet_frame):
    with pytest.raises(IndexError):
        parquet_frame.columns = pd.Index(list("abcdefghijklm"))


def test_column_indexing_df(parquet_frame, test_df):
    assert_frame_equal(test_df[[2]], parquet_frame[[2]])


def test_column_indexing_multiple(parquet_frame, test_df):
    assert_frame_equal(test_df[[2, 5, 8]], parquet_frame[[2, 5, 8]])


def test_column_indexing_mi_list(parquet_frame, test_df):
    cols = [
        (11, "daily", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
        (9, "daily", "BLOCK1:ZONE1_WALL_5_0_0", "Wall Gain", "W"),
        (10, "daily", "BLOCK1:ZONE2_WALL_4_8_9", "Wall Gain", "W"),
        (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
    ]
    assert_frame_equal(test_df[cols], parquet_frame[cols])


def test_column_indexing_missing(parquet_frame):
    cols = [
        (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
        (1000, "some", "invalid", "variable", ""),
    ]
    with pytest.raises(KeyError):
        _ = parquet_frame[cols]


def test_column_indexing_mi(parquet_frame, test_df):
    assert_frame_equal(
        test_df[[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")]],
        parquet_frame[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C")],
    )


def test_column_indexing_missing_string(parquet_frame):
    with pytest.raises(KeyError):
        _ = parquet_frame["invalid"]


def test_column_indexing_invalid_tuple(parquet_frame):
    with pytest.raises(IndexError):
        _ = parquet_frame[("invalid",)]


def test_column_indexing_invalid_mixed_type(parquet_frame):
    with pytest.raises(IndexError):
        _ = (parquet_frame[[(2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"), 6]],)


def test_loc_slice_rows(parquet_frame, test_df):
    assert_frame_equal(
        test_df.loc[datetime(2002, 1, 1) : datetime(2002, 1, 2)],
        parquet_frame.loc[datetime(2002, 1, 1) : datetime(2002, 1, 2)],
    )


def test_loc(parquet_frame, test_df):
    assert_frame_equal(
        test_df.loc[datetime(2002, 1, 1) : datetime(2002, 1, 2), [2]],
        parquet_frame.loc[datetime(2002, 1, 1) : datetime(2002, 1, 2), [2]],
    )


def test_invalid_loc(parquet_frame):
    with pytest.raises(IndexError):
        _ = parquet_frame.loc[:, ["a", "b", "c", 1.1234]]


def test_setter_new_var(parquet_frame, test_df):
    new_col = [1, 2, 3]
    new_var = (20, "daily", "new", "dummy", "type")
    test_df[new_var] = new_col
    parquet_frame[new_var] = new_col
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_setter_update_var(parquet_frame, test_df):
    new_col = [1, 2, 3]
    new_var = (20, "daily", "new", "dummy", "type")
    test_df[new_var] = new_col
    parquet_frame[new_var] = new_col
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_loc_setter(parquet_frame, test_df):
    new_col = [1, 2, 3]
    var = (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s")
    test_df.loc[:, var] = new_col
    parquet_frame.loc[:, var] = new_col
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_loc_setter_id(parquet_frame, test_df):
    new_col = [2, 3, 4]
    var = (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s")
    test_df.loc[:, var] = new_col
    parquet_frame.loc[:, var[0]] = new_col
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_loc_setter_all(parquet_frame, test_df):
    test_df.loc[:] = 1
    parquet_frame.loc[:] = 1
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_loc_setter_boolean_arr(parquet_frame, test_df):
    new_col = [1, 2, 3]
    arr = [False] * 7 + [True] + [False] * 6
    test_df.loc[:, arr] = new_col
    parquet_frame.loc[:, arr] = new_col
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_loc_sliced_setter(parquet_frame, test_df):
    new_col = [1, 2]
    var = (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s")
    test_df.loc[datetime(2002, 1, 1) : datetime(2002, 1, 2), var] = new_col
    parquet_frame.loc[datetime(2002, 1, 1) : datetime(2002, 1, 2), var] = new_col
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_loc_invalid_setter(parquet_frame):
    with pytest.raises(TypeError):
        parquet_frame.loc[:, 1] = pd.DataFrame({"a": [1, 2, 3]})


def test_loc_both_slices(parquet_frame, test_df):
    assert_frame_equal(test_df, parquet_frame.loc[:, :])


def test_update_parquet(parquet_frame):
    df = pd.DataFrame([[1], [2], [3]], columns=pd.Index(["a"], name="id"))
    parquet_frame.save_df_to_parquet("test_parquet.parquet", df)
    assert Path(parquet_frame.workdir, "test_parquet.parquet").exists()


def get_full_df(parquet_frame, test_df):
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_store_df_minimum_chunk_size(test_df, tmpdir):
    ParquetFrame.CHUNK_SIZE = 1  # save each column as an independent parquet
    parquet_frame = ParquetFrame.from_df(test_df, "some_name", pardir=tmpdir)
    assert len(list(parquet_frame.workdir.iterdir())) == 14
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_insert_column_start(parquet_frame, test_df):
    parquet_frame.insert(0, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
    test_df.insert(0, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
    assert_frame_equal(parquet_frame.get_df(), test_df)


def test_insert_column_middle(parquet_frame, test_df):
    parquet_frame.insert(5, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
    test_df.insert(5, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
    assert_frame_equal(parquet_frame.get_df(), test_df)


def test_insert_column_end(parquet_frame, test_df):
    parquet_frame.insert(14, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
    test_df.insert(14, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])
    assert_frame_equal(parquet_frame.get_df(), test_df)


def test_insert_column_invalid(parquet_frame):
    with pytest.raises(IndexError):
        parquet_frame.insert(25, (100, "this", "is", "dummy", "type"), ["a", "b", "c"])


def test_insert_into_empty_frame(parquet_frame, test_df):
    cols = test_df.columns.tolist()
    test_df.drop(columns=cols, inplace=True, axis=1)
    parquet_frame.drop(columns=cols, inplace=True, axis=1)
    test_df["foo"] = [1, 2, 3]
    parquet_frame["foo"] = [1, 2, 3]
    assert_frame_equal(test_df, parquet_frame.get_df(), check_column_type=False)


def test_drop(parquet_frame, test_df):
    test_df.drop(columns=[6, 10], inplace=True, level="id")
    parquet_frame.drop(columns=[6, 10], inplace=True, level="id")
    assert_frame_equal(test_df, parquet_frame.get_df())


def test_drop_invalid_level(parquet_frame):
    with pytest.raises(IndexError):
        parquet_frame.drop(columns=[1, 2, 3], level="foo")


def test_drop_all(parquet_frame, test_df):
    cols = test_df.columns.tolist()
    test_df.drop(columns=cols, inplace=True, axis=1)
    parquet_frame.drop(columns=cols, inplace=True)
    assert parquet_frame.get_df().empty
    assert_frame_equal(test_df, parquet_frame.get_df(), check_column_type=False)

    # add dummy variable to check frame
    test_df["foo"] = [1, 2, 3]
    parquet_frame["foo"] = [1, 2, 3]
    assert_frame_equal(test_df, parquet_frame.get_df(), check_column_type=False)


def test_save_index_parquets(parquet_frame):
    parquet_frame.save_index_parquets()
    assert Path(parquet_frame.workdir, ParquetFrame.INDEX_PARQUET).exists()
    assert Path(parquet_frame.workdir, ParquetFrame.COLUMNS_PARQUET).exists()
    assert Path(parquet_frame.workdir, ParquetFrame.CHUNKS_PARQUET).exists()


def test_load_index_parquets(parquet_frame, test_df):
    parquet_frame.save_index_parquets()
    test_chunks = parquet_frame._chunks_table.copy()
    parquet_frame._index = None
    parquet_frame._columns = None
    parquet_frame._chunks_table = None

    parquet_frame.load_index_parquets()
    assert_index_equal(test_df.index, parquet_frame.index)
    assert_index_equal(test_df.columns, parquet_frame.columns)
    assert_frame_equal(test_chunks, parquet_frame._chunks_table)


def test_load_missing_parquets(parquet_frame):
    parquet_frame.save_index_parquets()
    index_path = Path(parquet_frame.workdir, parquet_frame.INDEX_PARQUET)
    index_path.unlink()

    with pytest.raises(FileNotFoundError):
        parquet_frame.load_index_parquets()


def test_parquet_frame_context_maneger(parquet_frame, test_df):
    with parquet_frame_factory(df=test_df, name="test") as pqf:
        assert_frame_equal(test_df, pqf.get_df())
    assert not pqf.workdir.exists()


def test_copy_to(parquet_frame, test_df):
    with tempfile.TemporaryDirectory(dir=Path(ROOT_PATH, "storages")) as temp_dir:
        copied_frame = parquet_frame.copy_to(temp_dir)
        assert_frame_equal(test_df, copied_frame.get_df())


def test_copy(parquet_frame, test_df):
    copied_frame = copy(parquet_frame)
    assert_frame_equal(test_df, copied_frame.get_df())
