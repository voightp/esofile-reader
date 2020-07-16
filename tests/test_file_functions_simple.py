import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from esofile_reader import ResultsFile, SimpleVariable
from esofile_reader.exceptions import CannotAggregateVariables
from tests import ROOT


class TestSimpleFileFunctions(unittest.TestCase):
    def setUp(self) -> None:
        self.rf = ResultsFile.from_excel(
            Path(ROOT).joinpath("./eso_files/test_excel_results.xlsx"),
            sheet_names=["simple-template-monthly", "simple-no-template-no-index"]
        )

    def test_table_names(self):
        self.assertListEqual(
            self.rf.table_names,
            ["monthly-simple", "simple-no-template-no-index"],
        )

    def test_can_convert_rate_to_energy(self):
        pairs = [
            ("monthly-simple", True),
            ("simple-no-template-no-index", False),
        ]
        for table, can_convert in pairs:
            self.assertEqual(can_convert, self.rf.can_convert_rate_to_energy(table))

    def test_all_ids(self):
        self.assertEqual(len(self.rf.tables.get_all_variable_ids()), 14)

    def test_created(self):
        self.assertTrue(isinstance(self.rf.file_created, datetime))

    def test_complete(self):
        self.assertTrue(self.rf.complete)

    def test_header_df(self):
        names = ["id", "table", "key", "units"]
        self.assertEqual(
            self.rf.tables.get_all_variables_df().columns.to_list(), names
        )
        self.assertEqual(len(self.rf.tables.get_all_variables_df().index), 14)

        frames = []
        for table in self.rf.table_names:
            frames.append(self.rf.get_header_df(table))
        df = pd.concat(frames, axis=0)
        assert_frame_equal(df, self.rf.tables.get_all_variables_df())

    def test_rename(self):
        original = self.rf.file_name
        self.rf.rename("foo")
        self.assertEqual(self.rf.file_name, "foo")
        self.rf.rename(original)

    def test__add_file_name_row(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.rf._add_file_name(df, "row")
        mi = pd.MultiIndex.from_product(
            [["test_excel_results"], index], names=["file", "timestamp"]
        )
        assert_frame_equal(out, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=mi))

    def test__add_file_name_column(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.rf._add_file_name(df, "column")
        mi = pd.MultiIndex.from_product(
            [["test_excel_results"], ["a", "c"]], names=["file", None]
        )
        assert_frame_equal(out, pd.DataFrame([[1, 4], [2, 5], [3, 6]], index=index, columns=mi))

    def test__add_file_name_invalid(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.rf._add_file_name(df, "foo")
        mi = pd.MultiIndex.from_product(
            [["test_excel_results"], index], names=["file", "timestamp"]
        )
        assert_frame_equal(out, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=mi))

    def test__merge_frame(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df1 = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        df2 = pd.DataFrame({"b": [1, 2, 3]}, index=index)

        mi = pd.MultiIndex.from_product(
            [["test_excel_results"], index], names=["file", "timestamp"]
        )
        df = self.rf._merge_frame([df1, df2], add_file_name="row")
        assert_frame_equal(
            df, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6], "b": [1, 2, 3]}, index=mi)
        )

    def test__merge_frame_update_dt(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df1 = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        df2 = pd.DataFrame({"b": [1, 2, 3]}, index=index)

        mi = pd.MultiIndex.from_product(
            [["test_excel_results"], ["01-01", "02-01", "03-01"]],
            names=["file", "timestamp"],
        )
        df = self.rf._merge_frame(
            [df1, df2], add_file_name="row", timestamp_format="%d-%m"
        )
        assert_frame_equal(
            df, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6], "b": [1, 2, 3]}, index=mi)
        )

    def test_find_ids(self):
        v = SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units="")
        ids = self.rf.find_id(v, part_match=False)
        self.assertEqual(ids, [2])

    def test_find_ids_part_match(self):
        v = SimpleVariable(table="monthly-simple", key="BLOCK1", units="")
        ids = self.rf.find_id(v, part_match=True)
        self.assertEqual(ids, [2, 6, 7])

    def test_find_ids_part_invalid(self):
        v = SimpleVariable(table="time", key="BLOCK1:ZONE1", units="")
        ids = self.rf.find_id(v, part_match=False)
        self.assertEqual(ids, [])

    def test__find_pairs(self):
        v = SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units="")
        out = self.rf._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {"monthly-simple": [2]})

    def test__find_pairs_part_match(self):
        v = SimpleVariable(table="monthly-simple", key="BLOCK1", units="")
        out = self.rf._find_pairs(v, part_match=True)
        self.assertDictEqual(out, {"monthly-simple": [2, 6, 7]})

    def test__find_pairs_invalid(self):
        v = SimpleVariable(table="timestep", key="BLOCK1", units="")
        out = self.rf._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {})

    def test_create_new_header_variable(self):
        v1 = self.rf.create_header_variable("monthly-simple", "dummy", "foo")
        self.assertTupleEqual(
            v1, SimpleVariable(table="monthly-simple", key="dummy", units="foo")
        )

    def test_rename_variable(self):
        v1 = SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units="")
        self.rf.rename_variable(v1, new_key="NEW3")
        v2 = SimpleVariable(table="monthly-simple", key="NEW3", units="")
        ids = self.rf.find_id(v2)
        self.assertListEqual(ids, [2])
        # revert change
        self.rf.rename_variable(v2, new_key=v1.key)
        ids = self.rf.find_id(v1)
        self.assertListEqual(ids, [2])

    def test_rename_variable_invalid(self):
        v = SimpleVariable(table="monthly-simple", key="foo", units="")
        out = self.rf.rename_variable(v, new_key="NEW4")
        self.assertIsNone(out)

    def test_rename_variable_invalid_names(self):
        v = SimpleVariable(table="monthly-simple", key="BLOCK2:ZONE1", units="")
        out = self.rf.rename_variable(v)
        self.assertIsNone(out)
        ids = self.rf.find_id(v)
        self.assertListEqual(ids, [3])

    def test_add_output(self):
        id_, var = self.rf.insert_variable("monthly-simple", "new", "C", list(range(12)))
        self.assertTupleEqual(var, SimpleVariable("monthly-simple", "new", "C"))
        self.rf.remove_variables(var)

    def test_add_two_outputs(self):
        id_, var1 = self.rf.insert_variable("monthly-simple", "new", "C", list(range(12)))
        self.assertTupleEqual(var1, SimpleVariable("monthly-simple", "new", "C"))

        id_, var2 = self.rf.insert_variable("monthly-simple", "new", "C", list(range(12)))
        self.assertTupleEqual(var2, SimpleVariable("monthly-simple", "new (1)", "C"))
        self.rf.remove_variables(var1)
        self.rf.remove_variables(var2)

    def test_add_output_test_tree(self):
        id_, var = self.rf.insert_variable("monthly-simple", "new", "C", list(range(12)))
        self.assertTupleEqual(var, SimpleVariable("monthly-simple", "new", "C"))

        ids = self.rf.search_tree.find_ids(var)
        self.assertEqual(ids, [100])

        self.rf.remove_variables(var)
        ids = self.rf.search_tree.find_ids(var)
        self.assertEqual(ids, [])

    def test_add_output_invalid(self):
        out = self.rf.insert_variable("monthly-simple", "new", "C", [1])
        self.assertIsNone(out)

    def test_add_output_invalid_table(self):
        with self.assertRaises(KeyError):
            _ = self.rf.insert_variable("foo", "new", "C", [1])

    def test_aggregate_variables(self):
        v1 = SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units="")
        v2 = SimpleVariable(table="monthly-simple", key="BLOCK2:ZONE1", units="")
        v3 = SimpleVariable(table="monthly-simple", key="BLOCK3:ZONE1", units="")
        id_, var = self.rf.aggregate_variables([v1, v2, v3], "sum")
        self.assertEqual(
            var,
            SimpleVariable(table="monthly-simple", key="Custom Key - sum", units=""),
        )

    def test_aggregate_variables_too_few_variables(self):
        v1 = SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units="")
        v2 = SimpleVariable(table="monthly-simple", key="INVALID", units="")
        with self.assertRaises(CannotAggregateVariables):
            _ = self.rf.aggregate_variables([v1, v2], "sum")

    def test_as_df(self):
        df = self.rf.get_numeric_table("monthly-simple")
        self.assertTupleEqual(df.shape, (12, 7))
        self.assertListEqual(df.columns.names, ["id", "table", "key", "units"])
        self.assertEqual(df.index.name, "timestamp")

    def test_as_df_invalid_table(self):
        with self.assertRaises(KeyError):
            _ = self.rf.get_numeric_table("foo")

    def test__find_pairs_by_id(self):
        pairs = self.rf._find_pairs([1, 2, 3, 10, 11])
        self.assertDictEqual(
            {"monthly-simple": [1, 2, 3], "simple-no-template-no-index": [10, 11]}, pairs
        )
