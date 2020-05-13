import unittest

from esofile_reader.mini_classes import Variable
from esofile_reader.search_tree import Tree, Node


class TestSearchTree(unittest.TestCase):
    def setUp(self) -> None:
        self.header = {
            "daily": {
                1: Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
                2: Variable("daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
                3: Variable("daily", "BLOCK1:ZONE3", "Zone Temperature", "C"),
                4: Variable("daily", "BLOCK1:ZONE1", "Heating Load", "W"),
                5: Variable("daily", "BLOCK1:ZONE1_WALL_3_0_0_0_0_0_WIN", "Window Gain", "W"),
                6: Variable("daily", "BLOCK1:ZONE1_WALL_4_0_0_0_0_0_WIN", "Window Gain", "W"),
                9: Variable("daily", "BLOCK1:ZONE1_WALL_5_0_0", "Wall Gain", "W"),
                10: Variable("daily", "BLOCK1:ZONE2_WALL_4_8_9", "Wall Gain", "W"),
            },
            "monthly": {
                11: Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
                12: Variable("monthly", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
                13: Variable("monthly", "Some Flow 1", "Mass Flow", "kg/s"),
                14: Variable("monthly", "Some Curve", "Performance Curve 1", "kg/s"),
                15: Variable("monthly", "Some Curve", "Performance Curve 1", "kg/s"),
                16: Variable("monthly", "Some Curve", "Performance Curve 1", "kg/s"),
            },
        }
        self.tree = Tree()
        self.tree.populate_tree(self.header)

    def test_node_init(self):
        node = Node(None, "FOO")
        self.assertIsNone(node.parent)
        self.assertEqual("FOO", node.key)
        self.assertListEqual([], node.children)

    def test_str_tree(self):
        tree = Tree()
        tree.populate_tree(
            {
                "daily": {
                    1: Variable("daily", "b", "c", "d"),
                    2: Variable("daily", "b", "e", "f"),
                    3: Variable("daily", "b", "g", "h"),
                }
            }
        )
        strtree = """groot
	daily
		c
			b
				d
					1

		e
			b
				f
					2

		g
			b
				h
					3

"""
        self.assertEqual(strtree, tree.__repr__())

    def test_add_branch(self):
        v = Variable("monthly", "new key", "new type_", "C")
        id_ = self.tree._add_branch(17, v)
        self.assertIsNone(id_)
        self.assertEqual([17], self.tree.find_ids(v))

    def test_add_duplicate_branch(self):
        v = Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J")
        id_ = self.tree._add_branch(18, v)
        self.assertEqual(18, id_)
        self.assertEqual([11], self.tree.find_ids(v))

    def test_populate_tree(self):
        tree = Tree()
        duplicates = tree.populate_tree(self.header)
        self.assertDictEqual(
            {
                15: Variable("monthly", "Some Curve", "Performance Curve 1", "kg/s"),
                16: Variable("monthly", "Some Curve", "Performance Curve 1", "kg/s"),
            },
            duplicates,
        )

    def test_variable_exists(self):
        self.assertTrue(
            self.tree.variable_exists(Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"))
        )

    def test_variable_not_exists(self):
        self.assertFalse(
            self.tree.variable_exists(Variable("monthly", "Meter", "INVALID", "J"))
        )

    def test_get_ids(self):
        ids = self.tree.find_ids(Variable("monthly", "meter", "BLOCK1:ZONE1#LIGHTS", None))
        self.assertListEqual([11], ids)

    def test_get_ids_no_part_match(self):
        ids = self.tree.find_ids(
            Variable("monthly", "meter", "BLOCK1:ZONE", None), part_match=False
        )
        self.assertListEqual([], ids)

    def test_get_ids_part_match(self):
        ids = self.tree.find_ids(
            Variable("monthly", "meter", "BLOCK1:ZONE", None), part_match=True
        )
        self.assertListEqual([11, 12], ids)

    def test_remove_variable(self):
        v = Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C")
        self.tree.remove_variable(v)
        self.assertListEqual([], self.tree.find_ids(v))
        self.assertListEqual(
            [2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14],
            self.tree.find_ids(Variable(None, None, None, None)),
        )

    def test_remove_branch(self):
        v = Variable("monthly", None, None, None)
        self.tree.remove_variable(v)
        self.assertListEqual([], self.tree.find_ids(v))

    def test_remove_variables(self):
        v1 = Variable("monthly", None, None, None)
        v2 = Variable("daily", None, "Zone Temperature", None)
        self.tree.remove_variables([v1, v2])
        self.assertListEqual(
            [4, 5, 6, 9, 10], self.tree.find_ids(Variable(None, None, None, None))
        )

    def test_numeric_variable_name(self):
        v = Variable("hourly", 10, 11, 12)
        self.tree.add_variable(17, v)

    def test_add_variable(self):
        v = Variable("this", "is", "new", "variable")
        res = self.tree.add_variable(17, v)
        self.assertTrue(res)
        self.assertListEqual([17], self.tree.find_ids(v))

    def test_add_variable_invalid(self):
        v = Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C")
        res = self.tree.add_variable(17, v)
        self.assertFalse(res)
        self.assertListEqual([1], self.tree.find_ids(v))
