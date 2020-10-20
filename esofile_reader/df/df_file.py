from esofile_reader.abc.base_file import BaseFile
from esofile_reader.df.df_tables import DFTables
from esofile_reader.mini_classes import ResultsFileType


class DFFile(BaseFile):
    """
    A class to represent database results set.

    Attributes need to be populated from one of file
    wrappers ('EsoFile', 'DiffFile', 'TotalsFile').


    Attributes
    ----------
    id_ : int
        Unique id identifier.
    file : ResultsFileType
        Processed results file.

    Notes
    -----
    Reference file must be complete!

    """

    def __init__(self, id_: int, file: ResultsFileType):
        self.id_ = id_
        # create a new data so the original won't mutate
        tables = DFTables()
        for table, df in file.tables.items():
            tables[table] = df.copy()
        super().__init__(
            file.file_path,
            file.file_name,
            file.file_created,
            tables,
            file.search_tree,
            file.file_type,
        )
