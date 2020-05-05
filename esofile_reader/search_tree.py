import logging
from typing import Union, Optional, Dict, List

from esofile_reader.constants import *
from esofile_reader.mini_classes import Variable


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
    """

    def __init__(self, parent: Optional["Node"], key: Union[str, int]):
        self.parent = parent
        self.key = key
        self.children = []


class Tree:
    """
    A class which creates a tree like structure of
    the header dictionary.

    Tree needs to be populated using 'populate_tree'
    method.

    Attributes
    ----------
    root : Node
        A base root node which holds all the data
        as its children and children of children.

    """
    ORDER = [
        INTERVAL_LEVEL,
        TYPE_LEVEL,
        KEY_LEVEL,
        UNITS_LEVEL
    ]

    def __init__(self):
        self.root = Node(None, "groot")

    def __repr__(self):
        return self.str_tree()

    def tree_variable(self, variable: Variable):
        """ Pass reordered variable. """

        def low_string(s):
            # piece can be 'None' which will be kept
            return str(s).lower() if s else s

        v = Variable(*map(low_string, variable))
        return [v.__getattribute__(level) for level in self.ORDER]

    def str_tree(self) -> str:
        """ A string representation of a tree. """

        def _loopstr(node: Node, level: int, lst: list) -> None:
            """ Create a string representation of a tree. """
            level += 1
            tabs = level * "\t"
            if node.children:
                lst.append("{}{}\n".format(tabs, node.key))
                for child in node.children:
                    _loopstr(child, level, lst)
            else:
                lst.append("{}{}\n\n".format(tabs, node.key))

        level = -1
        lst = []
        _loopstr(self.root, level, lst)
        return str.join("", lst)

    @staticmethod
    def _add_node(node_key: str, parent: Node) -> Node:
        """ Create a new node if it does not exists. """
        children = parent.children
        try:
            nd = next(ch for ch in children if ch.key == node_key)
        except StopIteration:
            nd = Node(parent, node_key)
            parent.children.append(nd)
        return nd

    def _add_branch(self, id_: int, variable: Variable) -> Optional[int]:
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
            parent.children.append(val)
        else:
            # there's already a leaf, variable is a duplicate
            return id_

    def add_variable(self, id_: int, variable: Variable) -> bool:
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

    @staticmethod
    def _match(node: Node, condition: str, part_match: bool = False) -> bool:
        """ Check if node matches condition. """
        if not part_match:
            return node.key == condition
        else:
            return condition in node.key

    def _loop(
            self,
            node: Node,
            level: int,
            ids: List[int],
            cond: List[Optional[str]],
            part_match: bool = False
    ) -> None:
        """ Search through the tree to find ids. """
        level += 1

        # Reached the top of the tree, store value
        if not node.children:
            ids.append(node.key)
            return

        # filtering condition applied
        if cond[level] is not None:
            if self._match(node, cond[level], part_match=part_match):
                for nd in node.children:
                    self._loop(nd, level, ids, cond, part_match=part_match)
        else:
            # Condition not applied, loop through all children
            for nd in node.children:
                self._loop(nd, level, ids, cond, part_match=part_match)

    def variable_exists(self, variable: Variable) -> bool:
        """ Check if variable exists. """
        tree_variable = self.tree_variable(variable)
        ids = []
        for nd in self.root.children:
            level = -1
            self._loop(nd, level, ids, tree_variable)
        return bool(ids)

    def find_ids(self, variable: Variable, part_match: bool = False) -> List[int]:
        """ Find variable ids for given arguments. """
        tree_variable = self.tree_variable(variable)
        ids = []
        for nd in self.root.children:
            level = -1
            self._loop(nd, level, ids, tree_variable, part_match=part_match)
        if not ids:
            logging.warning(f"Variable: '{variable}' not found in tree!")
        return ids

    def _rem_loop(self, node: Node, level: int, cond: List[str]) -> None:
        """ Recursively remove nodes. """

        def remove_recursively(n):
            parent = n.parent
            if parent:
                parent.children.remove(n)
                if not parent.children:
                    # remove node only if there are no children left
                    remove_recursively(parent)

        level += 1

        # Reached the top of tree, recursively remove nodes
        if not node.children:
            remove_recursively(node)
            return

        # Handle if filtering condition applied
        if cond[level] is not None:
            if self._match(node, cond[level]):
                for nd in node.children[::-1]:
                    self._rem_loop(nd, level, cond)
        else:
            for nd in node.children[::-1]:
                self._rem_loop(nd, level, cond)

    def remove_variable(self, variable: Variable) -> None:
        tree_variable = self.tree_variable(variable)
        for nd in self.root.children[::-1]:
            level = -1
            self._rem_loop(nd, level, tree_variable)

    def remove_variables(self, variables: Union[Variable, List[Variable]]):
        """ Remove variable from the tree. """
        variables = variables if isinstance(variables, list) else [variables]
        for variable in variables:
            self.remove_variable(variable)
