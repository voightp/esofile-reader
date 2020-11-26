import contextlib
from copy import copy
from typing import Union, Optional, Dict, List, Iterator, Tuple

from esofile_reader.df.level_names import TABLE_LEVEL, KEY_LEVEL, TYPE_LEVEL, UNITS_LEVEL
from esofile_reader.exceptions import DuplicateVariable
from esofile_reader.typehints import Variable, SimpleVariable, VariableType


class Node:
    """ A base tree component.

    Parameters
    ----------
    parent : {Node, None}
        A parent nod of the node.
    key : str
        A node identifier.

    Attributes
    ----------
    parent : Node
        A parent node of the node.
    key : str
        A node identifier.
    children : {Dict of {str : Node}, LeafNode}
        Child nodes or variable id.

    Notes
    -----
    Children are not ordered!

    """

    def __init__(self, parent: Optional["Node"], key: str):
        self.parent = parent
        self.key = key
        self.children = {}

    def copy(self, new_parent=None):
        new_node = Node(parent=new_parent, key=self.key)
        if isinstance(self.children, LeafNode):
            new_node.children = LeafNode(new_node, self.children.key)
        else:
            for child_key, child_node in self.children.items():
                new_node.children[child_key] = child_node.copy(new_node)
        return new_node


class LeafNode:
    """ A bottom node of a tree.

    Parameters
    ----------
    parent : {Node, None}
        A parent nod of the node.
    key : str
        A node identifier.

    Attributes
    ----------
    parent : Node
        A parent node of the node.
    key : int
        A node identifier.

    """

    def __init__(self, parent: Node, key: Union[str, int]):
        self.parent = parent
        self.key = key


class Tree:
    """
    A class which creates a tree like structure of the header dictionary.

    Tree needs to be populated using 'populate_tree' method.

    Tree class is supposed to be used with SimpleVariable
    namedtuple (this variable has 3 levels, table, key and units).

    Attributes
    ----------
    root : Node
        A base root node which holds all the data
        as its children and children of children.

    """

    SIMPLE_BRANCH_ORDER = [TABLE_LEVEL, KEY_LEVEL, UNITS_LEVEL]
    BRANCH_ORDER = [TABLE_LEVEL, KEY_LEVEL, UNITS_LEVEL, TYPE_LEVEL]

    def __init__(self):
        self.root = Node(None, "groot")

    def __repr__(self):
        def create_string_items(node: Union[Node, LeafNode], lst: list, level: int = -1):
            """ Create a string representation of a tree. """
            level += 1
            tabs = level * "\t"
            lst.append(f"{tabs}{node.key}\n")
            if not isinstance(node, LeafNode):
                if isinstance(node.children, LeafNode):
                    create_string_items(node.children, lst, level=level)
                elif isinstance(node, Node):
                    for child in sorted(list(node.children.values()), key=lambda x: str(x.key)):
                        create_string_items(child, lst, level=level)

        string_items = []
        create_string_items(self.root, string_items)
        return str.join("", string_items)

    def __copy__(self):
        new_tree = Tree()
        new_tree.root = self.root.copy(None)
        return new_tree

    @classmethod
    def from_header_dict(cls, header_dct: Dict[str, Dict[int, VariableType]]) -> "Tree":
        """ Create a search tree instance from header dictionary. """
        tree = Tree()
        duplicates = {}
        for table, data in header_dct.items():
            for id_, variable in data.items():
                duplicate_id = tree._add_branch(id_, variable)
                if duplicate_id:
                    duplicates[duplicate_id] = variable
        if duplicates:
            raise DuplicateVariable(
                f"Header contains duplicates: {duplicates}", tree, duplicates
            )
        return tree

    @classmethod
    def cleaned_from_header_dict(
        cls, header_dct: Dict[str, Dict[int, VariableType]]
    ) -> Tuple["Tree", Optional[Dict[int, Variable]]]:
        """ Create a search tree instance from header dictionary with no duplicates. """
        try:
            tree = Tree.from_header_dict(header_dct)
            duplicates = None
        except DuplicateVariable as e:
            tree = e.clean_tree
            duplicates = e.duplicates
        return tree, duplicates

    def create_variable_iterator(self, variable: VariableType) -> Iterator:
        """ Pass reordered variable. """

        def low_string(s):
            # piece can be 'None' which will be kept
            return str(s).lower() if s else s

        lower = map(low_string, variable)
        if isinstance(variable, SimpleVariable):
            order = self.SIMPLE_BRANCH_ORDER
            v = SimpleVariable(*lower)
        else:
            order = self.BRANCH_ORDER
            v = Variable(*lower)
        # each class has its own sub branch
        tree_variable = [v.__getattribute__(level) for level in order]
        tree_variable.insert(0, v.__class__.__name__)
        return iter(tree_variable)

    @staticmethod
    def _add_node(node_key: str, parent: Node) -> Node:
        """ Create a new node if it does not exists. """
        if node_key in parent.children:
            node = parent.children[node_key]
        else:
            node = Node(parent, node_key)
            parent.children[node_key] = node
        return node

    def _add_branch(self, id_: int, variable: VariableType) -> Optional[int]:
        """ Append a branch to the tree. """
        tree_variable = self.create_variable_iterator(variable)
        parent = self.root
        for node_key in tree_variable:
            parent = self._add_node(node_key, parent)
        leaf = LeafNode(parent, id_)
        if not parent.children:
            parent.children = leaf
        else:
            # there's already a leaf, variable is a duplicate
            return id_

    def add_variable(self, id_: int, variable: Union[SimpleVariable, Variable]) -> None:
        """ Add new variable into the tree. """
        duplicate_id = self._add_branch(id_, variable)
        if duplicate_id:
            raise KeyError(f"Cannot add variable: '{variable}', it already exists!")

    def loop(
        self,
        node: Node,
        ids: List[int],
        tree_variable: Iterator[Optional[str]],
        part_match: bool = False,
    ) -> None:
        """ Search through the tree to find ids. """
        try:
            variable_piece = next(tree_variable)
            if variable_piece is not None:
                # First level ('Variable', 'SimpleVariable') needs to completely match
                if part_match:
                    # multiple children can match the condition
                    for node_key, child_node in node.children.items():
                        if variable_piece in node_key:
                            self.loop(
                                child_node, ids, copy(tree_variable), part_match=part_match
                            )
                else:
                    with contextlib.suppress(KeyError):
                        child_node = node.children[variable_piece]
                        self.loop(child_node, ids, tree_variable, part_match=part_match)
            else:
                # Condition not applied, loop through all children
                for child_node in node.children.values():
                    self.loop(child_node, ids, copy(tree_variable), part_match=part_match)
        except StopIteration:
            # reached bottom level, bottom node holds only leaf
            ids.append(node.children.key)

    def _search(
        self, variable: Union[SimpleVariable, Variable], part_match: bool,
    ) -> List[int]:
        tree_variable = self.create_variable_iterator(variable)
        ids = []
        variable_type = next(tree_variable)
        with contextlib.suppress(KeyError):
            node = self.root.children[variable_type]
            self.loop(node, ids, tree_variable, part_match=part_match)
        return sorted(ids)

    def find_ids(
        self, variables: Union[SimpleVariable, Variable], part_match: bool = False,
    ) -> List[int]:
        """ Find variable ids for given arguments. """
        variables = variables if isinstance(variables, list) else [variables]
        all_ids = []
        for variable in variables:
            ids = self._search(variable, part_match)
            if ids:
                all_ids.extend(ids)
        return all_ids

    def variable_exists(self, variable: Union[SimpleVariable, Variable]) -> bool:
        """ Check if variable exists. """
        return bool(self._search(variable, part_match=False))

    def loop_remove(self, node: Node, tree_variable: Iterator[str]) -> None:
        def remove_recursively(n):
            parent = n.parent
            if parent:
                parent.children.pop(n.key)
                if not parent.children:
                    # remove node only if there are no children left
                    remove_recursively(parent)

        try:
            variable_piece = next(tree_variable)
            if variable_piece is not None:
                with contextlib.suppress(KeyError):
                    child_node = node.children[variable_piece]
                    self.loop_remove(child_node, tree_variable)
            else:
                for child_node_key in list(node.children.keys()):
                    self.loop_remove(node.children[child_node_key], copy(tree_variable))
        except StopIteration:
            remove_recursively(node)

    def remove_variables(self, variables: Union[Variable, List[Variable]]):
        """ Remove variable from the tree. """
        variables = variables if isinstance(variables, list) else [variables]
        for variable in variables:
            tree_variable = self.create_variable_iterator(variable)
            self.loop_remove(self.root, tree_variable)
