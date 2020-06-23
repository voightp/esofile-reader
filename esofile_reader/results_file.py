import os
from datetime import datetime
from pathlib import Path
from typing import Union, List

from esofile_reader.base_file import BaseFile
from esofile_reader.eso_file import ResultsEsoFile
from esofile_reader.processing.diff import process_diff
from esofile_reader.processing.excel import process_excel
from esofile_reader.processing.monitor import DefaultMonitor
from esofile_reader.processing.totals import process_totals
from esofile_reader.search_tree import Tree
from esofile_reader.tables.df_tables import DFTables

try:
    from esofile_reader.processing.esofile import read_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install(pyximport=True, language_level=3)
    from esofile_reader.processing.esofile import read_file


class ResultsFile(BaseFile):
    """
    Generic results file with methods to process data
    from various sources.

    Instances should be created using 'from_*' methods.

    Attributes
    ----------
    file_path : str or Path
        A full path of the result file.
    file_name : str
        File name identifier.
    file_created : datetime.datetime
        Time and date when of the file generation.
    tables : DFTables
        Data storage instance.
    search_tree : Tree
        N array tree for efficient id searching.
    file_type : str, default "na"
        Identifier to store original file type.


    """

    def __init__(
            self,
            file_path: Union[str, Path],
            file_name: str,
            file_created: datetime,
            tables: DFTables,
            search_tree: Tree,
            file_type: str = "na",
    ):
        super().__init__(file_path, file_name, file_created, tables, search_tree, file_type)

    @classmethod
    def from_excel(
            cls,
            file_path: Union[str, Path],
            sheet_names: List[str] = None,
            force_index: bool = False,
            monitor: DefaultMonitor = None,
            header_limit=10,
    ) -> "ResultsFile":
        """ Generate 'ResultsFile' from excel spreadsheet. """
        file_path = Path(file_path)
        file_name = file_path.stem
        file_created = datetime.utcfromtimestamp(os.path.getctime(file_path))
        if not monitor:
            monitor = DefaultMonitor(file_path)
        monitor.processing_started()
        tables, search_tree = process_excel(
            file_path,
            monitor,
            sheet_names=sheet_names,
            force_index=force_index,
            header_limit=header_limit,
        )
        results_file = ResultsFile(
            file_path, file_name, file_created, tables, search_tree, file_type="excel"
        )
        monitor.processing_finished()
        return results_file

    @classmethod
    def from_eso_file(
            cls, file_path: str, monitor: DefaultMonitor = None, year: int = 2002,
    ) -> Union[List["ResultsFile"], "ResultsFile"]:
        """ Generate 'ResultsFile' from EnergyPlus .eso file. """
        # peaks are only allowed on explicit ResultsEsoFIle
        eso_files = ResultsEsoFile.from_multi_env_eso_file(
            file_path, monitor, ignore_peaks=True, year=year
        )
        return eso_files[0] if len(eso_files) == 1 else eso_files

    @classmethod
    def from_totals(cls, results_file: "ResultsFile") -> "ResultsFile":
        """ Generate totals 'ResultsFile' from another file. """
        file_path = results_file.file_path
        file_name = f"{results_file.file_name} - totals"
        file_created = results_file.file_created  # use base file timestamp
        tables, search_tree = process_totals(results_file)
        results_file = ResultsFile(
            file_path, file_name, file_created, tables, search_tree, file_type="totals"
        )
        return results_file

    @classmethod
    def from_diff(cls, file: "ResultsFile", other_file: "ResultsFile") -> "ResultsFile":
        """ Generate 'Results' file as a difference between two files. """
        file_path = ""
        file_name = f"{file.file_name} - {other_file.file_name} - diff"
        file_created = datetime.utcnow()
        tables, search_tree = process_diff(file, other_file)
        results_file = ResultsFile(
            file_path, file_name, file_created, tables, search_tree, file_type="totals"
        )
        return results_file
