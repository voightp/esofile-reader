from datetime import datetime
from pathlib import Path
from typing import Union, List, Optional

from esofile_reader.base_file import BaseFile, get_file_information
from esofile_reader.eso_file import ResultsEsoFile
from esofile_reader.mini_classes import ResultsFileType
from esofile_reader.processing.diff import process_diff
from esofile_reader.processing.excel import process_excel, process_csv
from esofile_reader.processing.progress_logger import EsoFileProgressLogger, GenericProgressLogger
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
        TableType storage instance.
    search_tree : Tree
        N array tree for efficient id searching.
    file_type : str, default "na"
        Identifier to store original file type.


    """

    ESO = "eso"
    TOTALS = "totals"
    DIFF = "diff"
    XLSX = "xlsx"
    CSV = "csv"
    NA = "na"

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
        progress_logger: GenericProgressLogger = None,
        header_limit=10,
    ) -> "ResultsFile":
        """ Generate 'ResultsFile' from excel spreadsheet. """
        file_path, file_name, file_created = get_file_information(file_path)
        if not progress_logger:
            progress_logger = GenericProgressLogger(file_path.name)
        progress_logger.log_task_started("Processing xlsx file.")
        tables, search_tree = process_excel(
            file_path,
            progress_logger,
            sheet_names=sheet_names,
            force_index=force_index,
            header_limit=header_limit,
        )
        results_file = ResultsFile(
            file_path, file_name, file_created, tables, search_tree, file_type=ResultsFile.XLSX
        )
        progress_logger.log_task_finished()
        return results_file

    @classmethod
    def from_csv(
        cls,
        file_path: Union[str, Path],
        force_index: bool = False,
        progress_logger: GenericProgressLogger = None,
        header_limit=10,
    ) -> "ResultsFile":
        """ Generate 'ResultsFile' from csv file. """
        file_path, file_name, file_created = get_file_information(file_path)
        if not progress_logger:
            progress_logger = GenericProgressLogger(file_path.name)
        progress_logger.log_task_started("Process csv file!")
        tables, search_tree = process_csv(
            file_path, progress_logger, force_index=force_index, header_limit=header_limit,
        )
        results_file = ResultsFile(
            file_path, file_name, file_created, tables, search_tree, file_type=ResultsFile.CSV
        )
        progress_logger.log_task_finished()
        return results_file

    @classmethod
    def from_eso_file(
        cls, file_path: str, progress_logger: EsoFileProgressLogger = None, year: int = 2002,
    ) -> Union[List[ResultsFileType], ResultsFileType]:
        """ Generate 'ResultsFileType' from EnergyPlus .eso file. """
        # peaks are only allowed on explicit ResultsEsoFIle
        eso_files = ResultsEsoFile.from_multi_env_eso_file(
            file_path, progress_logger, ignore_peaks=True, year=year
        )
        return eso_files[0] if len(eso_files) == 1 else eso_files

    @classmethod
    def from_totals(cls, results_file: ResultsFileType) -> Optional["ResultsFile"]:
        """ Generate totals 'ResultsFileType' from another file. """
        file_path = results_file.file_path
        file_name = f"{results_file.file_name} - totals"
        file_created = results_file.file_created  # use base file timestamp
        tables, tree = process_totals(results_file)
        results_file = ResultsFile(
            file_path, file_name, file_created, tables, tree, file_type=ResultsFile.TOTALS
        )
        return results_file

    @classmethod
    def from_diff(
        cls, file: ResultsFileType, other_file: ResultsFileType
    ) -> Optional["ResultsFile"]:
        """ Generate 'Results' file as a difference between two files. """
        file_path = ""
        file_name = f"{file.file_name} - {other_file.file_name} - diff"
        file_created = datetime.utcnow()
        tables = process_diff(file, other_file)
        if not tables.empty:
            tree = Tree.from_header_dict(tables.get_all_variables_dct())
            results_file = ResultsFile(
                file_path, file_name, file_created, tables, tree, file_type=ResultsFile.DIFF
            )
            return results_file

    @classmethod
    def from_path(cls, path: str):
        pass
