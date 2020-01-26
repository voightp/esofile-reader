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
    children : list of Node
        Node represents a leaf if children is 'None'.
    """

    def __init__(self, parent, key):
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

    def str_tree(self):
        """ A string representation of a tree. """
        level = -1
        lst = []
        self._loopstr(self.root, level, lst)
        st = str.join("", lst)
        return st

    def _loopstr(self, node, level, lst):
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
    def _add_node(nd_name, parent):
        """ Create a new node if it does not exists. """

        def _is_in_children():
            """ Return child node if node already exists. """
            for child in children:
                if child.key.lower() == nd_name.lower():
                    return child
            else:
                return None

        children = parent.children
        nd = _is_in_children()

        if not nd:
            nd = Node(parent, nd_name)
            parent.children.append(nd)

        return nd

    def add_branch(self, interval, key, var, units, id_):
        """ Append a branch to the tree. """
        pth = [interval, var, key, units]
        parent = self.root

        for nd_name in pth:
            parent = self._add_node(nd_name, parent)

        # add 'leaf'
        val = Node(parent, id_)
        val.children = None
        if parent.children:
            # there's already a leaf, variable is duplicate
            return id_

        parent.children.append(val)

    def populate_tree(self, header_dct):
        """ Create a search tree. """
        duplicates = []

        for interval, data in header_dct.items():
            for id_, tup in data.items():
                dup_id = self.add_branch(interval, tup.key,
                                         tup.variable, tup.units, id_)
                if dup_id:
                    duplicates.append(dup_id)

        return duplicates

    @staticmethod
    def _match(nd, condition, part_match=False):
        """ Check if node matches condition. """
        if not part_match:
            return nd.key.lower() == condition.lower()
        else:
            return condition.lower() in nd.key.lower()

    def _loop(self, node, level, ids, cond, part_match=False):
        """ Search through the tree to find ids. """
        level += 1

        # Reached the top of the tree, store value
        if not node.children:
            ids.append(node.key)
            return

        # Handle if filtering condition applied
        if cond[level]:
            if self._match(node, cond[level], part_match=part_match):
                for nd in node.children:
                    self._loop(nd, level, ids, cond, part_match=part_match)
            else:
                pass

        # Condition not applied, loop through all children
        else:
            for nd in node.children:
                self._loop(nd, level, ids, cond, part_match=part_match)

    def get_ids(self, interval=None, key=None,
                variable=None, units=None, part_match=False):
        """
        Find variable ids for given arguments.

        """
        cond = [interval, variable, key, units]
        ids = []

        for nd in self.root.children:
            level = -1
            self._loop(nd, level, ids, cond, part_match=part_match)

        if not ids:
            print(f"Variable: '{interval} : {key} : {variable} : {units}' not found!")

        return ids

    def get_pairs(self, interval=None, key=None,
                  variable=None, units=None, part_match=False):
        """
        Find interval : variable ids pairs for given arguments.

        """
        cond = [variable, key, units]
        pairs = {}

        for node in self.root.children:
            level = -1
            ids = []
            if interval:
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
            print(f"Variable: '{interval} : {key} : {variable} : {units}' not found!")

        return pairs

    def _rem_loop(self, node, level, cond):
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

    def remove_variables(self, variables):
        """ Remove variable from the tree. """
        if not isinstance(variables, list):
            variables = [variables]

        for var in variables:
            interval, key, variable, units = var
            cond = [interval, variable, key, units]

            for nd in self.root.children:
                level = -1
                self._rem_loop(nd, level, cond)

    def add_variable(self, id_, variable):
        """ Add new variable into the tree. """
        duplicate_id = self.add_branch(*variable, id_)
        return not bool(duplicate_id)
