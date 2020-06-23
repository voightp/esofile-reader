from esofile_reader.base_file import BaseFile
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import ResultsFile
from esofile_reader.storage.base_storage import BaseStorage
from esofile_reader.tables.df_tables import DFTables


class DFFile(BaseFile):
    """
    A class to represent database results set.

    Attributes need to be populated from one of file
    wrappers ('EsoFile', 'DiffFile', 'TotalsFile').


    Attributes
    ----------
    id_ : int
        Unique id identifier.
    file : ResultsFile
        Processed results file.

    Notes
    -----
    Reference file must be complete!

    """

    def __init__(self, id_: int, file: ResultsFile):
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


class DFStorage(BaseStorage):
    def __init__(self):
        super().__init__()
        self.files = {}

    def store_file(self, results_file: ResultsFile) -> int:
        id_gen = incremental_id_gen(checklist=list(self.files.keys()))
        id_ = next(id_gen)
        self.files[id_] = DFFile(id_, results_file)
        return id_

    def delete_file(self, id_: int) -> None:
        del self.files[id_]

    def get_all_file_names(self):
        return [f.file_name for f in self.files.values()]
