import unittest
from copy import deepcopy, copy

from esofile_reader.exceptions import DuplicateVariable
from esofile_reader.mini_classes import Variable, SimpleVariable
from esofile_reader.search_tree import Tree, Node


class TestSimpleSearchTree(unittest.TestCase):
    def setUp(self) -> None:
        self.header = {
            "daily": {
                1: SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
                2: SimpleVariable("daily", "BLOCK1:ZONE2 Zone Temperature", "C"),
                3: SimpleVariable("daily", "BLOCK1:ZONE3 Zone Temperature", "C"),
                4: SimpleVariable("daily", "BLOCK1:ZONE1 Heating Load", "W"),
                5: SimpleVariable("daily", "BLOCK1:ZONE1_WALL_3_0_0_WIN Window Gain", "W"),
                6: SimpleVariable("daily", "BLOCK1:ZONE1_WALL_4_0_0_WIN Window Gain", "W"),
                9: SimpleVariable("daily", "BLOCK1:ZONE1_WALL_5_0_0 Wall Gain", "W"),
                10: SimpleVariable("daily", "BLOCK1:ZONE2_WALL_4_8_9 Wall Gain", "W"),
            },
            "monthly": {
                11: SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"),
                12: SimpleVariable("monthly", "Meter BLOCK1:ZONE2#LIGHTS", "J"),
                13: SimpleVariable("monthly", "Some Flow 1 Mass Flow", "kg/s"),
                14: SimpleVariable("monthly", "Some Curve Performance Curve 1", "kg/s"),
            },
        }
        self.tree = Tree.from_header_dict(self.header)

    def test_node_init(self):
        node = Node(None, "FOO")
        self.assertIsNone(node.parent)
        self.assertEqual("FOO", node.key)
        self.assertDictEqual(dict(), node.children)

    def test_tree_structure(self):
        tree = Tree.from_header_dict(
            {
                "daily": {
                    1: SimpleVariable("daily", "c", "b"),
                    2: SimpleVariable("daily", "c", "a"),
                    3: SimpleVariable("daily", "g", "b"),
                    4: SimpleVariable("monthly", "g", "b"),
                }
            }
        )
        test_tree = {
            "SimpleVariable": {
                "daily": {"c": {"b": 1, "a": 2}, "g": {"b": 3}},
                "monthly": {"g": {"b": 4}},
            }
        }

        for ch0 in tree.root.children.values():
            for ch1 in ch0.children.values():
                for ch2 in ch1.children.values():
                    for ch3 in ch2.children.values():
                        leaf = ch3.children
                        self.assertEqual(
                            test_tree[ch0.key][ch1.key][ch2.key][ch3.key], leaf.key
                        )

    def test_add_branch(self):
        v = SimpleVariable("monthly", "new key new type_", "C")
        id_ = self.tree._add_branch(17, v)
        self.assertIsNone(id_)
        self.assertEqual([17], self.tree.find_ids(v))

    def test_add_duplicate_branch(self):
        v = SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J")
        id_ = self.tree._add_branch(18, v)
        self.assertEqual(18, id_)
        self.assertEqual([11], self.tree.find_ids(v))

    def test_tree_from_dict_duplicates(self):
        duplicate_header = deepcopy(self.header)
        v = SimpleVariable("monthly", "Some Curve Performance Curve 1", "kg/s")
        duplicate_header["monthly"][15] = v
        duplicate_header["monthly"][16] = v
        try:
            _ = Tree.from_header_dict(duplicate_header)
        except DuplicateVariable as e:
            self.assertDictEqual({15: v, 16: v}, e.duplicates)
        else:
            self.fail("DuplicateVariable exception not raised!")

    def test_variable_exists(self):
        self.assertTrue(
            self.tree.variable_exists(
                SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J")
            )
        )

    def test_variable_not_exists(self):
        self.assertFalse(
            self.tree.variable_exists(SimpleVariable("monthly", "Meter INVALID", "J"))
        )

    def test_find_ids(self):
        ids = self.tree.find_ids(SimpleVariable("monthly", "meter BLOCK1:ZONE1#LIGHTS", None))
        self.assertListEqual([11], ids)

    def test_find_ids_no_part_match(self):
        ids = self.tree.find_ids(
            SimpleVariable("monthly", "meter BLOCK1:ZONE", None), part_match=False
        )
        self.assertListEqual([], ids)

    def test_find_ids_part_match(self):
        ids = self.tree.find_ids(
            SimpleVariable("monthly", "meter BLOCK1:ZONE", None), part_match=True
        )
        self.assertListEqual([11, 12], ids)

    def test_remove_variable(self):
        v = SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C")
        self.tree.remove_variable(v)
        self.assertListEqual([], self.tree.find_ids(v))
        self.assertListEqual(
            [2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14],
            self.tree.find_ids(SimpleVariable(None, None, None)),
        )

    def test_remove_branch(self):
        v = SimpleVariable("monthly", None, None)
        self.tree.remove_variable(v)
        self.assertListEqual([], self.tree.find_ids(v))

    def test_remove_variables(self):
        v1 = SimpleVariable("monthly", None, None)
        v2 = SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", None)
        self.tree.remove_variables([v1, v2])
        self.assertListEqual(
            [2, 3, 4, 5, 6, 9, 10], self.tree.find_ids(SimpleVariable(None, None, None))
        )

    def test_numeric_variable_name(self):
        v = SimpleVariable("hourly", 11, 12)
        self.tree.add_variable(17, v)

    def test_add_variable(self):
        v = SimpleVariable("this", "is new", "variable")
        res = self.tree.add_variable(17, v)
        self.assertTrue(res)
        self.assertListEqual([17], self.tree.find_ids(v))

    def test_add_variable_invalid(self):
        v = SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C")
        res = self.tree.add_variable(17, v)
        self.assertFalse(res)
        self.assertListEqual([1], self.tree.find_ids(v))


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
            },
        }
        self.tree = Tree.from_header_dict(self.header)

    def test_node_init(self):
        node = Node(None, "FOO")
        self.assertIsNone(node.parent)
        self.assertEqual("FOO", node.key)
        self.assertDictEqual(dict(), node.children)

    def test_tree_structure(self):
        tree = Tree.from_header_dict(
            {
                "daily": {
                    1: Variable("daily", "b", "c", "d"),
                    2: Variable("daily", "b", "c", "f"),
                    3: Variable("daily", "b", "g", "h"),
                    4: Variable("monthly", "b", "g", "h"),
                }
            }
        )
        test_tree = {
            "Variable": {
                "daily": {"b": {"d": {"c": 1}, "f": {"c": 2}, "h": {"g": 3}}},
                "monthly": {"b": {"h": {"g": 4}}},
            }
        }
        for ch0 in tree.root.children.values():
            for ch1 in ch0.children.values():
                for ch2 in ch1.children.values():
                    for ch3 in ch2.children.values():
                        for ch4 in ch3.children.values():
                            leaf = ch4.children
                            self.assertEqual(
                                test_tree[ch0.key][ch1.key][ch2.key][ch3.key][ch4.key],
                                leaf.key,
                            )

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

    def test_variable_exists(self):
        self.assertTrue(
            self.tree.variable_exists(Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"))
        )

    def test_variable_not_exists(self):
        self.assertFalse(
            self.tree.variable_exists(Variable("monthly", "Meter", "INVALID", "J"))
        )

    def test_find_ids(self):
        ids = self.tree.find_ids(Variable("monthly", "meter", "BLOCK1:ZONE1#LIGHTS", None))
        self.assertListEqual([11], ids)

    def test_find_ids_no_part_match(self):
        ids = self.tree.find_ids(
            Variable("monthly", "meter", "BLOCK1:ZONE", None), part_match=False
        )
        self.assertListEqual([], ids)

    def test_find_ids_part_match(self):
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


class TestMixedSearchTree(unittest.TestCase):
    def setUp(self) -> None:
        self.header = {
            "daily": {
                1: SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
                2: Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Radiant", "C"),
                3: Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Operative", "C"),
                4: SimpleVariable("daily", "BLOCK1:ZONE1 Heating Load", "W"),
                5: SimpleVariable("daily", "BLOCK1:ZONE1_WALL_3_0_0_WIN Window Gain", "W"),
            },
            "monthly": {
                6: SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"),
                7: SimpleVariable("monthly", "Meter BLOCK1:ZONE2#LIGHTS", "J"),
                8: Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
                9: Variable("monthly", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
            },
        }
        self.tree = Tree.from_header_dict(self.header)

    def test_tree_structure(self):
        tree = Tree.from_header_dict(
            {
                "daily": {
                    1: SimpleVariable("daily", "c", "b"),
                    2: Variable("daily", "c", "a", "b"),
                    3: SimpleVariable("daily", "g", "b"),
                },
                "monthly": {
                    4: SimpleVariable("monthly", "g", "b"),
                    5: Variable("monthly", "g", "b", "f"),
                },
            }
        )
        self.assertEqual(
            """groot
	SimpleVariable
		daily
			c
				b
					1
			g
				b
					3
		monthly
			g
				b
					4
	Variable
		daily
			c
				b
					a
						2
		monthly
			g
				f
					b
						5
""",
            tree.__repr__(),
        )

    def test_add_branch(self):
        v = SimpleVariable("monthly", "new key new type_", "C")
        id_ = self.tree._add_branch(17, v)
        self.assertIsNone(id_)
        self.assertEqual([17], self.tree.find_ids(v))

        v = Variable("monthly", "new key", "new type_", "C")
        id_ = self.tree._add_branch(18, v)
        self.assertIsNone(id_)
        self.assertEqual([18], self.tree.find_ids(v))

    def test_add_duplicate_branch(self):
        v = SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J")
        id_ = self.tree._add_branch(18, v)
        self.assertEqual(18, id_)
        self.assertEqual([6], self.tree.find_ids(v))

        v = Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J")
        id_ = self.tree._add_branch(19, v)
        self.assertEqual(19, id_)
        self.assertEqual([8], self.tree.find_ids(v))

    def test_variable_exists(self):
        vars = [
            SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
            Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Radiant", "C"),
            Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Operative", "C"),
            SimpleVariable("daily", "BLOCK1:ZONE1 Heating Load", "W"),
            SimpleVariable("daily", "BLOCK1:ZONE1_WALL_3_0_0_WIN Window Gain", "W"),
            SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"),
            SimpleVariable("monthly", "Meter BLOCK1:ZONE2#LIGHTS", "J"),
            Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            Variable("monthly", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
        ]
        for v in vars:
            self.assertTrue(self.tree.variable_exists(v))

    def test_variable_not_exists(self):
        self.assertFalse(
            self.tree.variable_exists(SimpleVariable("monthly", "Meter INVALID", "J"))
        )
        self.assertFalse(
            self.tree.variable_exists(Variable("monthly", "Meter", "INVALID", "J"))
        )

    def test_find_ids(self):
        vars = [
            SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
            Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Radiant", "C"),
            Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Operative", "C"),
            SimpleVariable("daily", "BLOCK1:ZONE1 Heating Load", "W"),
            SimpleVariable("daily", "BLOCK1:ZONE1_WALL_3_0_0_WIN Window Gain", "W"),
            SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"),
            SimpleVariable("monthly", "Meter BLOCK1:ZONE2#LIGHTS", "J"),
            Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            Variable("monthly", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
        ]
        for v, id_ in zip(vars, list(range(1, 10))):
            self.assertEqual([id_], self.tree.find_ids(v))

    def test_find_ids_part_match(self):
        ids = self.tree.find_ids(
            SimpleVariable("monthly", "BLOCK1:ZONE", None), part_match=True
        )
        self.assertListEqual([6, 7], ids)

        ids = self.tree.find_ids(
            Variable("monthly", None, "BLOCK1:ZONE", None), part_match=True
        )
        self.assertListEqual([8, 9], ids)

    def test_remove_variable(self):
        v = SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C")
        self.tree.remove_variable(v)
        self.assertListEqual([], self.tree.find_ids(v))
        self.assertListEqual(
            [4, 5, 6, 7], self.tree.find_ids(SimpleVariable(None, None, None)),
        )
        self.assertListEqual(
            [2, 3, 8, 9], self.tree.find_ids(Variable(None, None, None, None)),
        )

    def test_remove_branch(self):
        v = SimpleVariable("monthly", None, None)
        self.tree.remove_variable(v)
        self.assertListEqual([], self.tree.find_ids(v))

    def test_remove_variables(self):
        v1 = SimpleVariable("monthly", None, None)
        v2 = SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", None)
        v3 = Variable("daily", None, None, None)
        self.tree.remove_variables([v1, v2, v3])
        self.assertListEqual([4, 5], self.tree.find_ids(SimpleVariable(None, None, None)))
        self.assertListEqual([8, 9], self.tree.find_ids(Variable(None, None, None, None)))

    def test_numeric_variable_name(self):
        v = SimpleVariable("hourly", 11, 12)
        self.tree.add_variable(17, v)

    def test_add_variable(self):
        v = SimpleVariable("this", "is new", "variable")
        res = self.tree.add_variable(17, v)
        self.assertTrue(res)
        self.assertListEqual([17], self.tree.find_ids(v))

    def test_add_variable_invalid(self):
        v = SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C")
        res = self.tree.add_variable(17, v)
        self.assertFalse(res)
        self.assertListEqual([1], self.tree.find_ids(v))

    def test_find_ids_warning(self):
        v = SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C")
        res = self.tree.add_variable(17, v)
        self.assertFalse(res)
        self.assertListEqual([1], self.tree.find_ids(v, check_only=True))

    def test_copy(self):
        self.assertEqual(self.tree.__repr__(), copy(self.tree.__repr__()))
