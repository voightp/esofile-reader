from datetime import datetime
from pathlib import Path
from typing import Union, List, Optional

from esofile_reader.abstractions.base_file import BaseFile, get_file_information
from esofile_reader.df.df_tables import DFTables
from esofile_reader.eso_file import EsoFile
from esofile_reader.exceptions import FormatNotSupported, NoResults
from esofile_reader.mini_classes import ResultsFileType, PathLike
from esofile_reader.processing.diff import process_diff
from esofile_reader.processing.excel import process_excel, process_csv
from esofile_reader.processing.progress_logger import BaseLogger
from esofile_reader.processing.totals import process_totals
from esofile_reader.search_tree import Tree

try:
    from esofile_reader.processing.extensions.esofile import process_eso_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install(pyximport=True, language_level=3)
    from esofile_reader.processing.extensions.esofile import process_eso_file


class GenericFile(BaseFile):
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
    file_type : str, default "na"
        Identifier to store original file type.


    """

    def __init__(
        self,
        file_path: PathLike,
        file_name: str,
        file_created: datetime,
        tables: DFTables,
        search_tree: Tree,
        file_type: str,
    ):
        super().__init__(file_path, file_name, file_created, tables, search_tree, file_type)

    @classmethod
    def from_excel(
        cls,
        file_path: PathLike,
        sheet_names: List[str] = None,
        force_index: bool = False,
        logger: BaseLogger = None,
        header_limit=10,
    ) -> "GenericFile":
        """ Generate 'GenericFile' from excel spreadsheet. """
        file_path, file_name, file_created = get_file_information(file_path)
        if not logger:
            logger = BaseLogger(file_path.name)
        with logger.log_task("Processing xlsx file."):
            tables = process_excel(
                file_path,
                logger,
                sheet_names=sheet_names,
                force_index=force_index,
                header_limit=header_limit,
            )
            if tables.empty:
                raise NoResults(f"There aren't any numeric outputs in file {file_path}.")
            else:
                logger.log_section("generating search tree!")
                tree = Tree.from_header_dict(tables.get_all_variables_dct())
            results_file = GenericFile(
                file_path, file_name, file_created, tables, tree, file_type=BaseFile.XLSX
            )
        return results_file

    @classmethod
    def from_csv(
        cls,
        file_path: PathLike,
        force_index: bool = False,
        logger: BaseLogger = None,
        header_limit=10,
    ) -> "GenericFile":
        """ Generate 'GenericFile' from csv file. """
        file_path, file_name, file_created = get_file_information(file_path)
        if not logger:
            logger = BaseLogger(file_path.name)
        with logger.log_task("Process csv file!"):
            tables = process_csv(
                file_path, logger, force_index=force_index, header_limit=header_limit,
            )
            if tables.empty:
                raise NoResults(f"There aren't any numeric outputs in file {logger.name}.")
            logger.log_section("generating search tree!")
            tree = Tree.from_header_dict(tables.get_all_variables_dct())
            results_file = GenericFile(
                file_path, file_name, file_created, tables, tree, file_type=BaseFile.CSV
            )
        return results_file

    @classmethod
    def from_eplus_file(
        cls, file_path: PathLike, logger: BaseLogger = None, year: Optional[int] = None,
    ) -> "GenericFile":
        """ Generate 'ResultsFile' from EnergyPlus .eso or .sql file. """
        eso_file = EsoFile.from_path(file_path, logger, ignore_peaks=True, year=year)
        return GenericFile(
            eso_file.file_path,
            eso_file.file_name,
            eso_file.file_created,
            eso_file.tables,
            eso_file.search_tree,
            file_type=eso_file.file_type,
        )

    @classmethod
    def from_eplus_multienv_file(
        cls, file_path: PathLike, logger: BaseLogger = None, year: Optional[int] = None,
    ) -> List["GenericFile"]:
        """ Generate 'ResultsFile' from EnergyPlus .eso file. """
        # peaks are only allowed on explicit EsoFile
        eso_files = EsoFile.from_multienv_path(file_path, logger, ignore_peaks=True, year=year)
        return [
            GenericFile(
                ef.file_path,
                ef.file_name,
                ef.file_created,
                ef.tables,
                ef.search_tree,
                file_type=ef.file_type,
            )
            for ef in eso_files
        ]

    @classmethod
    def from_path(cls, path: PathLike, logger: BaseLogger = None, **kwargs) -> "GenericFile":
        """ Generate 'Results file' from generic path. """
        switch = {
            BaseFile.SQL: cls.from_eplus_file,
            BaseFile.ESO: cls.from_eplus_file,
            BaseFile.XLSX: cls.from_excel,
            BaseFile.CSV: cls.from_csv,
        }
        file_type = Path(path).suffix
        try:
            results_file = switch[file_type](Path(path), logger=logger, **kwargs)
        except KeyError:
            raise FormatNotSupported(
                f"Cannot process file '{path}'. '{file_type}' is not supported."
            )
        return results_file

    @classmethod
    def from_totals(cls, results_file: ResultsFileType) -> "GenericFile":
        """ Generate totals 'ResultsFile' from another file. """
        file_path = results_file.file_path
        file_name = f"{results_file.file_name} - totals"
        file_created = results_file.file_created  # use base file timestamp
        tables = process_totals(results_file)
        if tables.empty:
            raise NoResults(f"Cannot generate totals for file '{file_path}'.")
        tree = Tree.from_header_dict(tables.get_all_variables_dct())
        results_file = GenericFile(
            file_path, file_name, file_created, tables, tree, file_type=BaseFile.TOTALS
        )
        return results_file

    @classmethod
    def from_diff(cls, file: ResultsFileType, other_file: ResultsFileType) -> "GenericFile":
        """ Generate 'Resultsfile' as a difference between two files. """
        file_path = ""
        file_name = f"{file.file_name} - {other_file.file_name} - diff"
        file_created = datetime.utcnow()
        tables = process_diff(file, other_file)
        if tables.empty:
            raise NoResults(
                "Cannot generate 'difference' file, there aren't any shared variables!"
            )
        tree = Tree.from_header_dict(tables.get_all_variables_dct())
        results_file = GenericFile(
            file_path, file_name, file_created, tables, tree, file_type=BaseFile.DIFF
        )
        return results_file
