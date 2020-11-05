from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from esofile_reader.abstractions.base_file import BaseFile, get_file_information
from esofile_reader.df.df_tables import DFTables
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import PathLike
from esofile_reader.processing.progress_logger import EsoFileLogger
from esofile_reader.search_tree import Tree

try:
    from esofile_reader.processing.extensions.esofile import process_eso_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install(pyximport=True, language_level=3)
    from esofile_reader.processing.extensions.esofile import process_eso_file


class EsoFile(BaseFile):
    """
    Enhanced results file to allow storing and extracting
    .eso file specific 'peak outputs'.

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
    peak_tables : DFTables
        TableType storage instance.


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
        return EsoFile(
            file_path=self.file_path,
            file_name=self.file_name,
            file_created=self.file_created,
            tables=copy(self.tables),
            search_tree=copy(self.search_tree),
            peak_tables=copy(self.peak_tables),
        )

    @classmethod
    def from_path(
        cls,
        file_path: str,
        progress_logger: EsoFileLogger = None,
        ignore_peaks: bool = True,
        year: Optional[int] = 2002,
    ) -> "EsoFile":
        if progress_logger is None:
            progress_logger = EsoFileLogger(Path(file_path).name)
        with progress_logger.log_task("Process eso file data!"):
            file_path, file_name, file_created = get_file_information(file_path)
            all_raw_df_outputs = process_eso_file(
                file_path, progress_logger, ignore_peaks=ignore_peaks, year=year
            )
            if len(all_raw_df_outputs) == 1:
                progress_logger.log_section("creating class instance!")
                raw_df_outputs = all_raw_df_outputs[0]
                tables = raw_df_outputs.tables
                peak_tables = raw_df_outputs.peak_tables
                tree = raw_df_outputs.tree
                return cls(
                    file_path, file_name, file_created, tables, tree, peak_tables=peak_tables
                )
            else:
                raise MultiEnvFileRequired(
                    f"Cannot populate file {file_path}. "
                    f"as there are multiple environments included.\n"
                    f"Use '{cls.__name__}.from_multi_env_eso_file' "
                    f"to generate multiple files."
                )

    @classmethod
    def from_multi_env_file_path(
        cls,
        file_path: str,
        progress_logger: EsoFileLogger = None,
        ignore_peaks: bool = True,
        year: Optional[int] = 2002,
    ) -> List["EsoFile"]:
        """ Generate independent 'EsoFile' for each environment. """
        file_path, file_name, file_created = get_file_information(file_path)
        if progress_logger is None:
            progress_logger = EsoFileLogger(file_path.name)
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
                ef = EsoFile(
                    file_path=file_path,
                    file_name=name,
                    file_created=file_created,
                    tables=raw_df_outputs.tables,
                    search_tree=raw_df_outputs.tree,
                    peak_tables=raw_df_outputs.peak_tables,
                )
                eso_files.append(ef)
        return eso_files
