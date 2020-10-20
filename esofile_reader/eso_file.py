import os
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from esofile_reader.abc.base_file import BaseFile, get_file_information
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import PathLike
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.search_tree import Tree
from esofile_reader.df.df_tables import DFTables

try:
    from esofile_reader.processing.extensions.esofile import process_eso_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install(pyximport=True, language_level=3)
    from esofile_reader.processing.extensions.esofile import process_eso_file


class ResultsEsoFile(BaseFile):
    """
    Enhanced results file to allow storing and extracting
    .eso file specific 'peak outputs'.

    File type passed to super() class is always 'eso'.

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


    """

    def __init__(
        self,
        file_path: PathLike,
        file_name: str,
        file_created: datetime,
        tables: DFTables,
        search_tree: Tree,
        peak_tables: Optional[Dict[str, DFTables]] = None,
    ):
        super().__init__(
            file_path, file_name, file_created, tables, search_tree, file_type=BaseFile.ESO
        )
        self.peak_tables = peak_tables

    def __copy__(self):
        # explicitly return this file type for all subclasses
        return ResultsEsoFile(
            file_path=self.file_path,
            file_name=self.file_name,
            file_created=self.file_created,
            tables=copy(self.tables),
            search_tree=copy(self.search_tree),
            peak_tables=copy(self.peak_tables),
        )

    @classmethod
    def from_multi_env_eso_file(
        cls,
        file_path: str,
        progress_logger: EsoFileProgressLogger = None,
        ignore_peaks: bool = True,
        year: int = 2002,
    ) -> List["ResultsEsoFile"]:
        """ Generate independent 'EsoFile' for each environment. """
        file_path, file_name, file_created = get_file_information(file_path)
        if progress_logger is None:
            progress_logger = EsoFileProgressLogger(file_path.name)
        with progress_logger.log_task("Process eso file data!"):
            all_raw_df_outputs = process_eso_file(
                file_path, progress_logger, ignore_peaks=ignore_peaks, year=year
            )
            progress_logger.log_section("creating class instance!")
            eso_files = []
            for i, raw_df_outputs in enumerate(reversed(all_raw_df_outputs)):
                # last processed environment uses a plain name, this is in place to only
                # assign distinct names for 'sizing' results which are reported first
                name = (
                    f"{file_name} - {raw_df_outputs.environment_name}" if i > 0 else file_name
                )
                ef = ResultsEsoFile(
                    file_path=file_path,
                    file_name=name,
                    file_created=file_created,
                    tables=raw_df_outputs.tables,
                    search_tree=raw_df_outputs.tree,
                    peak_tables=raw_df_outputs.peak_tables,
                )
                eso_files.append(ef)
        return eso_files


class EsoFile(ResultsEsoFile):
    """
    A wrapper class to allow .eso file processing by passing
    file path as a parameter.

    Parameters
    ----------
    file_path : str, or Path
        A full path of the result file.
    progress_logger : EsoFileProgressLogger
        A watcher to report processing progress.
    ignore_peaks : bool
        Allow skipping .eso file peak data.
    year : int
        A year for which index data are bound to.

    Raises
    ------
    IncompleteFile
    BlankLineError
    MultiEnvFileRequired


    """

    def __init__(
        self,
        file_path: PathLike,
        progress_logger: EsoFileProgressLogger = None,
        ignore_peaks: bool = True,
        year: int = 2002,
    ):
        if progress_logger is None:
            progress_logger = EsoFileProgressLogger(Path(file_path).name)
        with progress_logger.log_task("Process eso file data!"):
            file_path = Path(file_path)
            file_name = file_path.stem
            file_created = datetime.utcfromtimestamp(os.path.getctime(file_path))
            all_raw_df_outputs = process_eso_file(
                file_path, progress_logger, ignore_peaks=ignore_peaks, year=year
            )
            if len(all_raw_df_outputs) == 1:
                progress_logger.log_section("creating class instance!")
                raw_df_outputs = all_raw_df_outputs[0]
                tables = raw_df_outputs.tables
                peak_tables = raw_df_outputs.peak_tables
                tree = raw_df_outputs.tree
                super().__init__(
                    file_path, file_name, file_created, tables, tree, peak_tables=peak_tables
                )
            else:
                raise MultiEnvFileRequired(
                    f"Cannot populate file {file_path}. "
                    f"as there are multiple environments included.\n"
                    f"Use '{super().__class__.__name__}.process_multi_env_file' "
                    f"to generate multiple files."
                )
