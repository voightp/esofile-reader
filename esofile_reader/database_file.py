from datetime import datetime

from esofile_reader.base_file import BaseFile
from esofile_reader.utils.search_tree import Tree
from esofile_reader.utils.mini_classes import Data


class DatabaseFile(BaseFile):
    """
    A class to represent database results set.

    Attributes need to be populated from one of file
    wrappers ('EsoFile', 'DiffFile', 'TotalsFile').


    Attributes
    ----------
    id_ : int
        Unique id identifier.
    data : {SQLData, DFData}
        A data object to store results data.
    file_path : str
        A full path of the result file.
    file_created : datetime
        Time and date when of the file generation..
    data : {DFdata, SQLdata}
        A class to store results data
    _search_tree : Tree
        N array tree for efficient id searching.


    Notes
    -----
    Reference file must be complete!

    """

    def __init__(
        self,
        id_: int,
        file_name: str,
        data: Data,
        file_created: datetime,
        totals: bool = False,
        search_tree: Tree = None,
        file_path: str = None,
    ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.data = data
        self.file_created = file_created
        self._search_tree = search_tree
        self.totals = totals

    def rename(self, name: str) -> None:
        self.file_name = name
        if self.data.__class__.__name__ == "SQLData":
            self.data.update_file_name(name)
