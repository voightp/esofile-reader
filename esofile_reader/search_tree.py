import logging
from typing import Union, Optional, Dict, List

from esofile_reader.mini_classes import Variable


def lower_args(func):
    def wrapper(*args, **kwargs):
        low_args = []
        low_kwargs = {}
        for a in args:
            low_args.append(a.lower() if isinstance(a, str) else a)
        for k, v in kwargs.items():
            low_kwargs[k] = v.lower() if isinstance(v, str) else v
        return func(*low_args, **low_kwargs)

    return wrapper


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

    def __init__(self):
        self.root = Node(None, "groot")

    def __repr__(self):
        return self.str_tree()

    def str_tree(self) -> str:
        """ A string representation of a tree. """
        level = -1
        lst = []
        self._loopstr(self.root, level, lst)
        st = str.join("", lst)
        return st

    def _loopstr(self, node: Node, level: int, lst: list) -> None:
        """ Create a string representation of a tree. """
        level += 1
        tabs = level * "\t"
        if node.children:
            lst.append("{}{}\n".format(tabs, node.key))
            for child in node.children:
                self._loopstr(child, level, lst)
        else:
            lst.append("{}{}\n\n".format(tabs, node.key))

    @staticmethod
    def _add_node(nd_name: str, parent: Node) -> Node:
        """ Create a new node if it does not exists. """
        children = parent.children
        try:
            nd = next(ch for ch in children if ch.key == nd_name)
        except StopIteration:
            nd = None

        if not nd:
            nd = Node(parent, nd_name)
            parent.children.append(nd)

        return nd

    @lower_args
    def add_branch(
            self, interval: str, key: str, var: str, units: str, id_: int
    ) -> Optional[int]:
        """ Append a branch to the tree. """
        pth = [interval.lower(), var.lower(), key.lower(), units.lower()]
        parent = self.root

        for nd_name in pth:
            parent = self._add_node(nd_name, parent)

        # add 'leaf'
        val = Node(parent, id_)
        val.children = None
        if parent.children:
            # there's already a leaf, variable is a duplicate
            return id_

        parent.children.append(val)

    def populate_tree(self, header_dct: Dict[str, Dict[int, Variable]]) -> Dict[int, Variable]:
        """ Create a search tree. """
        duplicates = {}
        for interval, data in header_dct.items():
            for id_, var in data.items():
                dup_id = self.add_branch(interval, var.key, var.variable, var.units, id_)
                if dup_id:
                    duplicates[dup_id] = var
        return duplicates

    @staticmethod
    def _match(node: Node, condition: str, part_match: bool = False) -> bool:
        """ Check if node matches condition. """
        if not part_match:
            return node.key == condition
        else:
            return condition in node.key

    def _loop(
            self, node: Node, level: int, ids: List[int], cond, part_match: bool = False
    ) -> None:
        """ Search through the tree to find ids. """
        level += 1

        # Reached the top of the tree, store value
        if not node.children:
            ids.append(node.key)
            return

        # filtering condition applied
        if cond[level]:
            if self._match(node, cond[level], part_match=part_match):
                for nd in node.children:
                    self._loop(nd, level, ids, cond, part_match=part_match)
        else:
            # Condition not applied, loop through all children
            for nd in node.children:
                self._loop(nd, level, ids, cond, part_match=part_match)

    @lower_args
    def get_ids(
            self,
            interval: str = None,
            key: str = None,
            variable: str = None,
            units: str = None,
            part_match: bool = False
    ) -> List[int]:
        """ Find variable ids for given arguments. """
        cond = [interval, variable, key, units]
        ids = []
        for nd in self.root.children:
            level = -1
            self._loop(nd, level, ids, cond, part_match=part_match)
        if not ids:
            logging.warning(
                f"Variable: '{interval} : {key} " f": {variable} : {units}' not found!"
            )
        return ids

    @lower_args
    def get_pairs(
            self,
            interval: str = None,
            key: str = None,
            variable: str = None,
            units: str = None,
            part_match: bool = False
    ) -> Dict[str, List[int]]:
        """ Find interval : variable ids pairs for given arguments. """
        cond = [variable, key, units]
        pairs = {}

        for node in self.root.children:
            level = -1
            ids = []
            if interval:
                interval = interval.lower()
                if self._match(node, interval):
                    for nd in node.children:
                        self._loop(nd, level, ids, cond, part_match=part_match)
            else:
                for nd in node.children:
                    self._loop(nd, level, ids, cond, part_match=part_match)

            if ids:
                pairs[node.key] = ids

        pairs = {k: v for k, v in pairs.items() if v}

        if not pairs:
            logging.warning(
                f"Variable: '{interval} : {key} " f": {variable} : {units}' not found!"
            )

        return pairs

    def _rem_loop(self, node: Node, level: int, cond: List[str]) -> None:
        """ Recursively remove nodes. """

        def remove_recursively(_node):
            parent = _node.parent
            if parent:
                parent.children.remove(_node)
                if not parent.children:
                    # remove node only if there are no children left
                    remove_recursively(parent)

        level += 1

        # Reached the top of tree, recursively remove nodes
        if not node.children:
            remove_recursively(node)
            return

        # Handle if filtering condition applied
        if cond[level]:
            if self._match(node, cond[level]):
                for nd in node.children:
                    self._rem_loop(nd, level, cond)
        else:
            for nd in node.children:
                self._rem_loop(nd, level, cond)

    @lower_args
    def remove_variable(self, interval: str, key: str, variable: str, units: str) -> None:
        cond = [interval, variable, key, units]
        for nd in self.root.children:
            level = -1
            self._rem_loop(nd, level, cond)

    def remove_variables(self, variables: Union[Variable, List[Variable]]):
        """ Remove variable from the tree. """
        variables = variables if isinstance(variables, list) else [variables]
        for variable in variables:
            self.remove_variable(*variable)

    def add_variable(self, id_: int, variable: Variable) -> bool:
        """ Add new variable into the tree. """
        interval, key, var, units = variable
        duplicate_id = self.add_branch(interval, key, var, units, id_)
        return not bool(duplicate_id)
