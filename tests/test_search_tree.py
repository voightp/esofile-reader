from copy import copy

import pytest

from esofile_reader.exceptions import DuplicateVariable
from esofile_reader.typehints import Variable, SimpleVariable
from esofile_reader.search_tree import Tree, Node


@pytest.fixture(scope="function")
def simple_tree():
    header = {
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
    return Tree.from_header_dict(header)


@pytest.fixture(scope="function")
def tree():
    header = {
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
    return Tree.from_header_dict(header)


@pytest.fixture(scope="function")
def mixed_tree():
    header = {
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
    return Tree.from_header_dict(header)


def test_node_init():
    node = Node(None, "FOO")
    assert not node.parent
    assert node.key == "FOO"
    assert node.children == dict()


def test_simple_tree_structure():
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
                    assert leaf.key == test_tree[ch0.key][ch1.key][ch2.key][ch3.key]


def test_tree_structure():
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
                        assert (
                            leaf.key == test_tree[ch0.key][ch1.key][ch2.key][ch3.key][ch4.key]
                        )


def test_mixed_tree_structure():
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
    assert (
        tree.__repr__()
        == """groot
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
"""  # noqa W191
    )  # noqa E101


@pytest.mark.parametrize(
    "test_tree, id_, variable",
    [
        (
            pytest.lazy_fixture("simple_tree"),
            17,
            Variable("monthly", "new key", "new type_", "C"),
        ),
        (pytest.lazy_fixture("tree"), 17, SimpleVariable("monthly", "new key new type_", "C")),
    ],
)
def test_add_branch(test_tree, id_, variable):
    id_ = test_tree._add_branch(id_, variable)
    assert id_ is None
    assert test_tree.find_ids(variable) == [17]


@pytest.mark.parametrize(
    "test_tree, id_, variable",
    [
        (pytest.lazy_fixture("tree"), 17, Variable("monthly", "new key", "new type_", "C")),
        (
            pytest.lazy_fixture("simple_tree"),
            17,
            SimpleVariable("monthly", "new key new type_", "C"),
        ),
    ],
)
def test_add_branch_into_other_tree_type(test_tree, id_, variable):
    id_ = test_tree._add_branch(id_, variable)
    assert id_ is None
    assert test_tree.find_ids(variable) == [17]


@pytest.mark.parametrize(
    "test_tree, id_, variable",
    [
        (
            pytest.lazy_fixture("tree"),
            17,
            Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
        ),
        (
            pytest.lazy_fixture("simple_tree"),
            17,
            SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"),
        ),
    ],
)
def test_add_duplicate_branch(test_tree, id_, variable):
    duplicate_id = test_tree._add_branch(id_, variable)
    assert id_ == duplicate_id


DUPLICITE_HEADER = {
    "daily": {
        1: Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
        2: Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
        3: Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
    },
    "monthly": {
        11: Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
        12: Variable("monthly", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
    },
}


def test_tree_from_dict_with_duplicates():
    try:
        _ = Tree.from_header_dict(DUPLICITE_HEADER)
    except DuplicateVariable as e:
        v = Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C")
        assert {2: v, 3: v} == e.duplicates
    else:
        pytest.fail("DuplicateVariable exception not raised!")


def test_cleaned_tree_from_dict_with_duplicates():
    tree, duplicates = Tree.cleaned_from_header_dict(DUPLICITE_HEADER)
    assert duplicates == {
        2: Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
        3: Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
    }


@pytest.mark.parametrize(
    "test_tree, variable, exists",
    [
        (
            pytest.lazy_fixture("tree"),
            Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            True,
        ),
        (
            pytest.lazy_fixture("simple_tree"),
            SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"),
            True,
        ),
        (pytest.lazy_fixture("tree"), Variable("monthly", "Meter", "INVALID", "J"), False),
        (
            pytest.lazy_fixture("simple_tree"),
            SimpleVariable("monthly", "Meter INVALID", "J"),
            False,
        ),
    ],
)
def test_variable_exists(test_tree, variable, exists):
    assert test_tree.variable_exists(variable) is exists


@pytest.mark.parametrize(
    "test_tree, variable, ids",
    [
        (
            pytest.lazy_fixture("tree"),
            Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            [11],
        ),
        (
            pytest.lazy_fixture("simple_tree"),
            SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"),
            [11],
        ),
    ],
)
def test_find_ids(test_tree, variable, ids):
    assert test_tree.find_ids(variable) == ids


@pytest.mark.parametrize(
    "test_tree, variable, part_match, test_ids",
    [
        (
            pytest.lazy_fixture("tree"),
            Variable("monthly", "meter", "BLOCK1:ZONE", None),
            False,
            [],
        ),
        (
            pytest.lazy_fixture("simple_tree"),
            SimpleVariable("monthly", "meter BLOCK1:ZONE", None),
            False,
            [],
        ),
        (
            pytest.lazy_fixture("tree"),
            Variable("monthly", "Meter", "BLOCK1:ZONE", "J"),
            True,
            [11, 12],
        ),
        (
            pytest.lazy_fixture("simple_tree"),
            SimpleVariable("monthly", "meter BLOCK1:ZONE", None),
            True,
            [11, 12],
        ),
    ],
)
def test_find_ids_part_match(test_tree, variable, part_match, test_ids):
    ids = test_tree.find_ids(variable, part_match=part_match)
    assert ids == test_ids


@pytest.mark.parametrize(
    "test_tree, variable, remaining_ids",
    [
        (
            pytest.lazy_fixture("tree"),
            Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
            [2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14],
        ),
        (
            pytest.lazy_fixture("simple_tree"),
            SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
            [2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14],
        ),
        (
            pytest.lazy_fixture("mixed_tree"),
            Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
            [2, 3, 8, 9],
        ),
        (
            pytest.lazy_fixture("mixed_tree"),
            SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
            [4, 5, 6, 7],
        ),
    ],
)
def test_remove_variable(test_tree, variable, remaining_ids):
    test_tree.remove_variables(variable)
    check_variable = (
        SimpleVariable(None, None, None)
        if type(variable) is SimpleVariable
        else Variable(None, None, None, None)
    )
    assert test_tree.find_ids(variable) == []
    assert test_tree.find_ids(check_variable) == remaining_ids


@pytest.mark.parametrize(
    "test_tree, variable",
    [
        (pytest.lazy_fixture("tree"), Variable("monthly", None, None, None)),
        (pytest.lazy_fixture("simple_tree"), SimpleVariable("daily", None, None)),
        (pytest.lazy_fixture("mixed_tree"), SimpleVariable("monthly", None, None)),
    ],
)
def test_remove_branch(test_tree, variable):
    test_tree.remove_variables(variable)
    assert test_tree.find_ids(variable) == []


@pytest.mark.parametrize(
    "test_tree, variable",
    [
        (pytest.lazy_fixture("tree"), Variable("monthly", 10, 11, 12)),
        (pytest.lazy_fixture("simple_tree"), SimpleVariable("daily", 11, 12)),
        (pytest.lazy_fixture("tree"), Variable("this", "is", "new", "variable")),
        (pytest.lazy_fixture("simple_tree"), SimpleVariable("this", "is new", "variable")),
        (pytest.lazy_fixture("mixed_tree"), SimpleVariable("this", "is new", "variable")),
    ],
)
def test_add_variable(test_tree, variable):
    test_tree.add_variable(17, variable)
    assert test_tree.find_ids(variable) == [17]


@pytest.mark.parametrize(
    "test_tree, variable",
    [
        (
            pytest.lazy_fixture("tree"),
            Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
        ),
        (
            pytest.lazy_fixture("simple_tree"),
            SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
        ),
        (
            pytest.lazy_fixture("mixed_tree"),
            SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
        ),
    ],
)
def test_add_variable_invalid(test_tree, variable):
    with pytest.raises(KeyError):
        test_tree.add_variable(17, variable)


@pytest.mark.parametrize(
    "variables, exists",
    [
        (
            [
                SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
                Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Radiant", "C"),
                Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Operative", "C"),
                SimpleVariable("daily", "BLOCK1:ZONE1 Heating Load", "W"),
                SimpleVariable("daily", "BLOCK1:ZONE1_WALL_3_0_0_WIN Window Gain", "W"),
                SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"),
                SimpleVariable("monthly", "Meter BLOCK1:ZONE2#LIGHTS", "J"),
                Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
                Variable("monthly", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
            ],
            True,
        ),
        (
            [
                SimpleVariable("monthly", "Meter INVALID", "J"),
                Variable("monthly", "Meter", "INVALID", "J"),
            ],
            False,
        ),
    ],
)
def test_variable_exists_mixed_tree(mixed_tree, variables, exists):
    assert all(map(lambda x: mixed_tree.variable_exists(x) is exists, variables))


@pytest.mark.parametrize(
    "variable, expected_id",
    [
        (SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"), 1),
        (Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Radiant", "C"), 2),
        (Variable("daily", "BLOCK1:ZONE2 Zone Temperature", "Operative", "C"), 3),
        (SimpleVariable("daily", "BLOCK1:ZONE1 Heating Load", "W"), 4),
        (SimpleVariable("daily", "BLOCK1:ZONE1_WALL_3_0_0_WIN Window Gain", "W"), 5),
        (SimpleVariable("monthly", "Meter BLOCK1:ZONE1#LIGHTS", "J"), 6),
        (SimpleVariable("monthly", "Meter BLOCK1:ZONE2#LIGHTS", "J"), 7),
        (Variable("monthly", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"), 8),
        (Variable("monthly", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"), 9),
    ],
)
def test_find_ids_mixed_tree(mixed_tree, variable, expected_id):
    assert mixed_tree.find_ids(variable) == [expected_id]


@pytest.mark.parametrize(
    "variable, expected_ids",
    [
        (SimpleVariable("monthly", "Meter", None), [6, 7]),
        (Variable("monthly", None, "BLOCK1:ZONE", None), [8, 9]),
    ],
)
def test_find_multiple_ids_part_match(mixed_tree, variable, expected_ids):
    assert mixed_tree.find_ids(variable, part_match=True) == expected_ids


def test_remove_multiple_variables(mixed_tree):
    v1 = SimpleVariable("monthly", None, None)
    v2 = SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", None)
    v3 = Variable("daily", None, None, None)
    mixed_tree.remove_variables([v1, v2, v3])
    assert mixed_tree.find_ids(SimpleVariable(None, None, None)) == [4, 5]
    assert mixed_tree.find_ids(Variable(None, None, None, None)) == [8, 9]


@pytest.mark.parametrize(
    "test_tree",
    [
        pytest.lazy_fixture("tree"),
        pytest.lazy_fixture("simple_tree"),
        pytest.lazy_fixture("mixed_tree"),
    ],
)
def test_copy_tree(test_tree):
    assert test_tree.__repr__() == copy(test_tree.__repr__())


@pytest.mark.parametrize(
    "test_tree, variable, id_",
    [
        (
            pytest.lazy_fixture("tree"),
            Variable("daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
            1,
        ),
        (
            pytest.lazy_fixture("simple_tree"),
            SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
            1,
        ),
        (
            pytest.lazy_fixture("mixed_tree"),
            SimpleVariable("daily", "BLOCK1:ZONE1 Zone Temperature", "C"),
            1,
        ),
    ],
)
def test_copy_tree_identity(test_tree, variable, id_):
    copied_tree = copy(test_tree)
    copied_tree.remove_variables(variable)
    assert test_tree.find_ids(variable) == [id_]
