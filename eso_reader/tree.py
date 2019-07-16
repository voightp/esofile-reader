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
        A parent nod of the node.
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

    Parameters
    ----------
    header_dct
        An EnergyPlus Eso file header dictionary.

    Attributes
    ----------
    root : Node
        A base root node which holds all the data
        as its children and children of children.
    """

    def __init__(self, header_dct):
        self.root = Node(None, "groot")
        self.create_tree(header_dct)

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

    def create_tree(self, header_dct):
        """ Create a search tree. """
        for interval, data in header_dct.items():
            for id, tup in data.items():
                self.add_branch(interval, tup.key, tup.variable, tup.units, id)

    def add_branch(self, interval, key, var, units, id):
        """ Append a branch to tree. """
        pth = [interval, var, key, units]
        parent = self.root

        for nd_name in pth:
            parent = self._add_node(nd_name, parent)

        val = Node(parent, id)
        val.children = None
        parent.children.append(val)

    def _add_node(self, nd_name, parent):
        """ Create a new node if it does not exists. """
        nd = self._is_in_children(nd_name, parent.children)
        if not nd:
            nd = Node(parent, nd_name)
            parent.children.append(nd)
        return nd

    def _is_in_children(self, nd_name, children):
        """ Return child node if node already exists. """
        for child in children:
            if child.key.lower() == nd_name.lower():
                return child
        else:
            return None

    @staticmethod
    def _match(nd, condition, part_match=False):
        """ Check if node matches condition. """
        if not part_match:
            return nd.key.lower() == condition.lower()
        else:
            return condition.lower() in nd.key.lower()

    def _loop(self, node, level, lst, cond, part_match=False):
        level += 1

        # Reached the top of tree, store value
        if not node.children:
            lst.append(node.key)
            return

        # Handle if filtering condition applied
        if cond[level]:
            if self._match(node, cond[level], part_match=part_match):
                for nd in node.children:
                    self._loop(nd, level, lst, cond, part_match=part_match)
            else:
                pass

        # Condition not applied, loop through all children
        else:
            for nd in node.children:
                self._loop(nd, level, lst, cond, part_match=part_match)

    def search(self, interval=None, key=None, var=None, units=None, part_match=False):
        """ Find variable ids for given arguments. """
        root = self.root
        cond = [None, interval, var, key, units]  # First 'None' to skip root node
        level = -1
        ids = []
        self._loop(root, level, ids, cond, part_match=part_match)

        if not ids:
            print("Variable: '{} : {} : {} : {}' not found!".format(interval, key, var, units))
        return ids
