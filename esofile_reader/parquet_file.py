from datetime import datetime

from esofile_reader.base_file import BaseFile
from esofile_reader.utils.search_tree import Tree
from esofile_reader.utils.mini_classes import Data, ResultsFile


class ParquetFile(BaseFile):
    def __init__(
        self,
        id_: int,
        file_name: str,
        storage: Data,
        file_created: datetime,
        totals: bool = False,
        search_tree: Tree = None,
        file_path: str = None,
    ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.storage = storage
        self.file_created = file_created
        self._search_tree = search_tree
        self.totals = totals
