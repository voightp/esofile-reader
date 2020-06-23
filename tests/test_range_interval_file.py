import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.testing import assert_index_equal, assert_frame_equal

from esofile_reader.base_file import BaseFile, CannotAggregateVariables
from esofile_reader.constants import N_DAYS_COLUMN, DAY_COLUMN
from esofile_reader.mini_classes import Variable
from esofile_reader.search_tree import Tree
from esofile_reader.storage.pqt_storage import ParquetStorage
from esofile_reader.storage.sql_storage import SQLStorage
from esofile_reader.tables.df_tables import DFTables


class TestRangeIntervalFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        variables = [
            (1, "range", "ZoneA", "Temperature", "C"),
            (2, "range", "ZoneB", "Temperature", "C"),
            (3, "range", "ZoneC", "Temperature", "C"),
            (4, "range", "ZoneC", "Heating Load", "W"),
        ]
        columns = pd.MultiIndex.from_tuples(
            variables, names=["id", "table", "key", "type", "units"]
        )
        index = pd.RangeIndex(start=0, stop=3, step=1, name="range")
        results = pd.DataFrame(
            [
                [25.123, 27.456, 14.546, 1000],
                [25.123, 27.456, 14.546, 2000],
                [25.123, 27.456, 14.546, 3000],
            ],
            columns=columns,
            index=index,
        )

        tables = DFTables()
        tables["range"] = results

        tree = Tree()
        tree.populate_tree(tables.get_all_variables_dct())
        bf = BaseFile("", "no-dates", datetime.utcnow(), tables, tree)
        cls.bf = bf

        storage = SQLStorage()
        id_ = storage.store_file(bf)
        cls.db_bf = storage.files[id_]

    def test_table_names(self):
        self.assertListEqual(self.bf.table_names, ["range"])

    def test_all_ids(self):
        self.assertEqual(len(self.bf.tables.get_all_variable_ids()), 4)

    def test_created(self):
        self.assertTrue(isinstance(self.bf.file_created, datetime))

    def test_complete(self):
        self.assertTrue(self.bf.complete)

    def test_header_df(self):
        self.assertEqual(
            self.bf.tables.get_all_variables_df().columns.to_list(),
            ["id", "table", "key", "type", "units"],
        )
        self.assertEqual(len(self.bf.tables.get_all_variables_df().index), 4)

    def test_find_ids(self):
        v = Variable(table="range", key="ZoneC", type="Temperature", units="C")
        ids = self.bf.find_ids(v, part_match=False)
        self.assertEqual(ids, [3])

    def test_find_ids_part_invalid(self):
        v = Variable(
            table="range", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        ids = self.bf.find_ids(v, part_match=False)
        self.assertEqual(ids, [])

    def test__find_pairs(self):
        v = Variable(table="range", key="ZoneC", type="Temperature", units="C")
        out = self.bf._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {"range": [3]})

    def test__find_pairs_invalid(self):
        v = Variable(table="range", key="BLOCK1", type="Zone People Occupant Count", units="")
        out = self.bf._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {})

    def test_create_new_header_variable(self):
        v1 = self.bf.create_header_variable(
            table="range", key="ZoneC", var="Temperature", units="C"
        )
        self.assertTupleEqual(
            v1, Variable(table="range", key="ZoneC (1)", type="Temperature", units="C")
        )

    def test_rename_variable(self):
        v = Variable(table="range", key="ZoneC", type="Temperature", units="C")
        self.bf.rename_variable(v, new_key="NEW", new_type="VARIABLE")

        v = Variable(table="range", key="NEW", type="VARIABLE", units="C")
        ids = self.bf.find_ids(v)
        self.assertListEqual(ids, [3])

        self.bf.rename_variable(v, new_key="ZoneC", new_type="Temperature")

    def test_add_output(self):
        id_, var = self.bf.add_variable("range", "new", "type", "C", [1, 2, 3])
        self.assertTupleEqual(var, Variable("range", "new", "type", "C"))
        self.bf.remove_variables(var)

    def test_add_output_test_tree(self):
        id_, var = self.bf.add_variable("range", "new", "type", "C", [1, 2, 3])
        self.assertTupleEqual(var, Variable("range", "new", "type", "C"))

        ids = self.bf.search_tree.find_ids(var)
        self.assertIsNot(ids, [])
        self.assertEqual(len(ids), 1)

        self.bf.remove_variables(var)
        ids = self.bf.search_tree.find_ids(var)
        self.assertEqual(ids, [])

    def test_add_output_invalid(self):
        out = self.bf.add_variable("range", "new", "type", "C", [1])
        self.assertIsNone(out)

    def test_aggregate_variables(self):
        v = Variable(table="range", key=None, type="Temperature", units="C")
        id_, var = self.bf.aggregate_variables(v, "sum")
        self.assertEqual(
            var, Variable(table="range", key="Custom Key - sum", type="Temperature", units="C"),
        )
        self.bf.remove_variables(var)

    def test_aggregate_energy_rate(self):
        _, v1 = self.bf.add_variable(
            "range", "CHILLER", "Chiller Electric Power", "W", [1, 1, 1]
        )
        _, v2 = self.bf.add_variable(
            "range", "CHILLER", "Chiller Electric Power", "J", [2, 2, 2]
        )

        with self.assertRaises(CannotAggregateVariables):
            _ = self.bf.aggregate_variables([v1, v2], "sum")

        self.bf.remove_variables([v1, v2])

    def test_as_df(self):
        df = self.bf.get_numeric_table("range")
        self.assertTupleEqual(df.shape, (3, 4))
        self.assertListEqual(df.columns.names, ["id", "table", "key", "type", "units"])
        self.assertEqual(df.index.name, "range")

    def test_sql_results(self):
        variables = [
            Variable("range", "ZoneA", "Temperature", "C"),
            Variable("range", "ZoneB", "Temperature", "C"),
        ]
        df1 = self.bf.get_results(variables)
        df2 = self.db_bf.get_results(variables)

        pd.testing.assert_frame_equal(df1, df2)

    def test_get_results_include_day(self):
        variables = [
            Variable("range", "ZoneA", "Temperature", "C"),
            Variable("range", "ZoneB", "Temperature", "C"),
        ]
        df = self.bf.get_results(variables, include_day=True, add_file_name="")
        assert_index_equal(pd.RangeIndex(start=0, stop=3, step=1, name="range"), df.index)

    def test_sql_no_n_days_column(self):
        with self.assertRaises(KeyError):
            self.db_bf.tables.get_special_column("range", N_DAYS_COLUMN)

    def test_sql_no_day_column(self):
        with self.assertRaises(KeyError):
            self.db_bf.tables.get_special_column("range", DAY_COLUMN)

    def test_parquet_file(self):
        path = Path("range_pqs" + ParquetStorage.EXT)
        pqs = ParquetStorage(path)
        id_ = pqs.store_file(self.bf)
        pqf = pqs.files[id_]

        pqs.save()
        loaded_pqs = ParquetStorage.load_storage(path)
        loaded_pqf = loaded_pqs.files[id_]

        assert_index_equal(pqf.tables["range"].index, loaded_pqf.tables["range"].index)

        assert_index_equal(pqf.tables["range"].columns, loaded_pqf.tables["range"].columns)

        assert_frame_equal(pqf.tables["range"].get_df(), loaded_pqf.tables["range"].get_df())

        path.unlink()
