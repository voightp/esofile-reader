import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Union, List, Optional

from esofile_reader.search_tree import Tree
from esofile_reader.tables.df_tables import DFTables

try:
    from typing import ForwardRef
except ImportError:
    from typing import _ForwardRef as ForwardRef

from esofile_reader.base_file import BaseFile, get_file_information
from esofile_reader.processing.progress_logger import EsoFileProgressLogger
from esofile_reader.exceptions import *

try:
    from esofile_reader.processing.esofile import read_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install(pyximport=True, language_level=3)
    from esofile_reader.processing.esofile import read_file


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
        file_path: Union[str, Path],
        file_name: str,
        file_created: datetime,
        tables: DFTables,
        search_tree: Tree,
        peak_outputs: Optional[Dict[str, DFTables]] = None,
    ):
        super().__init__(
            file_path, file_name, file_created, tables, search_tree, file_type="eso"
        )
        self.peak_outputs = peak_outputs

    @classmethod
    def from_multi_env_eso_file(
        cls,
        file_path: str,
        progress_logger: EsoFileProgressLogger = None,
        ignore_peaks: bool = True,
        year: int = 2002,
    ) -> List[ForwardRef("EsoFile")]:
        """ Generate independent 'EsoFile' for each environment. """
        file_path, file_name, file_created = get_file_information(file_path)
        if progress_logger is None:
            progress_logger = EsoFileProgressLogger(file_path.name)
        progress_logger.log_task_started("Process eso file data!")

        eso_files = []
        content = read_file(file_path, progress_logger, ignore_peaks=ignore_peaks, year=year)
        content = [c for c in list(zip(*content))[::-1]]  # reverse to get last processed first
        for i, (environment, data, peak_outputs, tree) in enumerate(content):
            # last processed environment uses a plain name
            # this is in place to only assign distinct names for
            # 'sizing' results which are reported first
            name = f"{file_name} - {environment}" if i > 0 else file_name
            ef = ResultsEsoFile(
                file_path, name, file_created, data, tree, peak_outputs=peak_outputs
            )
            eso_files.append(ef)
        progress_logger.log_task_finished()
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
        file_path: Union[str, Path],
        progress_logger: EsoFileProgressLogger = None,
        ignore_peaks: bool = True,
        year: int = 2002,
    ):
        if progress_logger is None:
            progress_logger = EsoFileProgressLogger(Path(file_path).name)
        progress_logger.log_task_started("Process eso file data!")
        file_path = Path(file_path)
        file_name = file_path.stem
        file_created = datetime.utcfromtimestamp(os.path.getctime(file_path))
        content = read_file(file_path, progress_logger, ignore_peaks=ignore_peaks, year=year)
        environment_names = content[0]
        if len(environment_names) == 1:
            tables = content[1][0]
            peak_outputs = content[2][0]
            tree = content[3][0]
            super().__init__(
                file_path, file_name, file_created, tables, tree, peak_outputs=peak_outputs
            )
            progress_logger.log_task_finished()
        else:
            raise MultiEnvFileRequired(
                f"Cannot populate file {file_path}. "
                f"as there are multiple environments included.\n"
                f"Use '{super().__class__.__name__}.process_multi_env_file' "
                f"to generate multiple files."
            )
