import contextlib
import logging
from typing import Union, Optional, Dict, List

from esofile_reader.constants import *
from esofile_reader.mini_classes import Variable, SimpleVariable


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
    key : {str, int}
        A node identifier.
    children : list of Node
        Node represents a leaf if children is 'None'.

    Notes
    -----
    Children are not ordered!

    """

    def __init__(self, parent: Optional["Node"], key: Union[str, int]):
        self.parent = parent
        self.key = key
        self.children = set()


class Tree:
    """
    A class which creates a tree like structure of the header dictionary.

    Tree needs to be populated using 'populate_tree' method.

    Tree class is supposed to be used with SimpleVariable
    namedtuple (this variable has 3 levels, interval, key and units).

    Attributes
    ----------
    root : Node
        A base root node which holds all the data
        as its children and children of children.

    """

    def __init__(self):
        self.root = Node(None, "groot")

    def __repr__(self):
        def _loopstr(node: Node, lst: list, level: int = -1) -> None:
            """ Create a string representation of a tree. """
            level += 1
            tabs = level * "\t"
            lst.append(f"{tabs}{node.key}\n")
            if node.children:
                for child in sorted(list(node.children), key=lambda x: str(x.key)):
                    _loopstr(child, lst, level=level)

        lst = []
        _loopstr(self.root, lst)
        return str.join("", lst)

    @staticmethod
    def tree_variable(variable: Union[Variable, SimpleVariable]):
        """ Pass reordered variable. """

        def low_string(s):
            # piece can be 'None' which will be kept
            return str(s).lower() if s else s

        lower = map(low_string, variable)
        if isinstance(variable, SimpleVariable):
            order = [INTERVAL_LEVEL, KEY_LEVEL, UNITS_LEVEL]
            v = SimpleVariable(*lower)
        else:
            order = [INTERVAL_LEVEL, KEY_LEVEL, UNITS_LEVEL, TYPE_LEVEL]
            v = Variable(*lower)
        # each class has its own sub branch
        tree_variable = [v.__getattribute__(level) for level in order]
        tree_variable.insert(0, v.__class__.__name__)
        return tree_variable

    @staticmethod
    def _add_node(node_key: str, parent: Node) -> Node:
        """ Create a new node if it does not exists. """
        children = parent.children
        try:
            nd = next(ch for ch in children if ch.key == node_key)
        except StopIteration:
            nd = Node(parent, node_key)
            parent.children.add(nd)
        return nd

    def _add_branch(self, id_: int, variable: Union[Variable, SimpleVariable]) -> Optional[int]:
        """ Append a branch to the tree. """
        tree_variable = self.tree_variable(variable)
        parent = self.root
        for node_key in tree_variable:
            # all keys needs to be str
            parent = self._add_node(node_key, parent)
        # add 'leaf'
        val = Node(parent, id_)
        val.children = None
        if not parent.children:
            parent.children.add(val)
        else:
            # there's already a leaf, variable is a duplicate
            return id_

    def add_variable(self, id_: int, variable: Union[SimpleVariable, Variable]) -> bool:
        """ Add new variable into the tree. """
        duplicate_id = self._add_branch(id_, variable)
        return not bool(duplicate_id)

    def populate_tree(self, header_dct: Dict[str, Dict[int, Variable]]) -> Dict[int, Variable]:
        """ Create a search tree. """
        duplicates = {}
        for interval, data in header_dct.items():
            for id_, variable in data.items():
                duplicate_id = self._add_branch(id_, variable)
                if duplicate_id:
                    duplicates[duplicate_id] = variable
        return duplicates

    def _loop(
            self,
            node: Node,
            ids: List[int],
            tree_variable: List[Optional[str]],
            part_match: bool = False,
            level: int = 0,
    ) -> None:
        """ Search through the tree to find ids. """
        if level == len(tree_variable):
            # reached bottom level, bottom node has only single leaf id node
            ids.append(next(iter(node.children)).key)
        else:
            condition = tree_variable[level]
            level += 1
            if condition is not None:
                # First level ('Variable', 'SimpleVariable') needs to completely match
                if part_match and level != 1:
                    # multiple children can match the condition
                    for nd in node.children:
                        if condition in nd.key:
                            self._loop(
                                nd, ids, tree_variable, part_match=part_match, level=level
                            )
                else:
                    with contextlib.suppress(StopIteration):
                        nd = next(n for n in node.children if n.key == condition)
                        self._loop(nd, ids, tree_variable, part_match=part_match, level=level)
            else:
                # Condition not applied, loop through all children
                for nd in node.children:
                    self._loop(nd, ids, tree_variable, part_match=part_match, level=level)

    def find_ids(
            self, variable: Union[SimpleVariable, Variable], part_match: bool = False
    ) -> List[int]:
        """ Find variable ids for given arguments. """
        tree_variable = self.tree_variable(variable)
        ids = []
        self._loop(self.root, ids, tree_variable, part_match=part_match)
        if not ids:
            logging.warning(f"'{variable}' not found in tree!")
        return sorted(ids)

    def variable_exists(self, variable: Union[SimpleVariable, Variable]) -> bool:
        """ Check if variable exists. """
        return bool(self.find_ids(variable, part_match=False))

    def _rem_loop(self, node: Node, tree_variable: List[str], level: int = 0) -> None:
        def remove_recursively(n):
            parent = n.parent
            if parent:
                parent.children.remove(n)
                if not parent.children:
                    # remove node only if there are no children left
                    remove_recursively(parent)

        if len(node.children) == 1 and not next(iter(node.children)).children:
            remove_recursively(node)
        else:
            condition = tree_variable[level]
            level += 1
            if condition is not None:
                with contextlib.suppress(StopIteration):
                    nd = next(n for n in node.children if n.key == condition)
                    self._rem_loop(nd, tree_variable, level=level)
            else:
                for nd in list(node.children):
                    self._rem_loop(nd, tree_variable, level=level)

    def remove_variable(self, variable: Union[SimpleVariable, Variable]) -> None:
        tree_variable = self.tree_variable(variable)
        self._rem_loop(self.root, tree_variable)

    def remove_variables(self, variables: Union[Variable, List[Variable]]):
        """ Remove variable from the tree. """
        variables = variables if isinstance(variables, list) else [variables]
        for variable in variables:
            self.remove_variable(variable)
