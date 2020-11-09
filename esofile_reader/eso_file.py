from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from esofile_reader.abstractions.base_file import BaseFile, get_file_information
from esofile_reader.constants import *
from esofile_reader.df.df_tables import DFTables
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import PathLike
from esofile_reader.processing.progress_logger import EsoFileLogger
from esofile_reader.processing.raw_data import RawData
from esofile_reader.processing.raw_data_parser import choose_parser, Parser
from esofile_reader.search_tree import Tree


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
    peak_tables : Dict of {str, DFTables}
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
    def _process_env_data(
        cls, raw_data: RawData, parser: Parser, logger: EsoFileLogger, year: int
    ) -> Tuple[Tree, DFTables, Optional[Dict[str, DFTables]]]:
        """ Process an environment raw data into final classes. """
        logger.set_maximum_progress(raw_data.get_n_tables() + 1)
        logger.log_section("generating search tree!")
        tree, duplicates = Tree.cleaned_from_header_dict(raw_data.header)
        if duplicates:
            raw_data.remove_variables(duplicates)
        logger.increment_progress()

        logger.log_section("processing dates!")
        dates, n_days = parser.cast_date_data(raw_data, year)
        special_columns = {N_DAYS_COLUMN: n_days, DAY_COLUMN: raw_data.days_of_week}

        logger.log_section("generating tables!")
        tables = parser.cast_outputs(
            raw_data.outputs, raw_data.header, dates, special_columns, logger
        )

        if raw_data.peak_outputs:
            logger.log_section("generating peak tables!")
            peak_tables = parser.cast_peak_outputs(
                raw_data.peak_outputs, raw_data.header, dates, logger
            )
        else:
            peak_tables = None

        return tree, tables, peak_tables

    @classmethod
    def _process_raw_data(
        cls, file_path: PathLike, logger: EsoFileLogger, ignore_peaks: bool, year: int
    ) -> List["EsoFile"]:
        """ Process raw data from all environments into final classes. """
        file_path, file_name, file_created = get_file_information(file_path)
        parser = choose_parser(file_path)
        eso_files = []
        all_raw_data = parser.process_file(file_path, logger, ignore_peaks)
        for i, raw_data in enumerate(reversed(all_raw_data)):
            tree, tables, peak_tables = cls._process_env_data(raw_data, parser, logger, year)
            logger.log_section("Creating class instance!")
            # last processed environment uses a plain name, this is in place to only
            # assign distinct names for 'sizing' results which are reported first
            name = f"{file_name} - {raw_data.environment_name}" if i > 0 else file_name
            ef = cls(
                file_path=file_path,
                file_name=name,
                file_created=file_created,
                tables=tables,
                search_tree=tree,
                peak_tables=peak_tables,
            )
            eso_files.append(ef)
        return eso_files

    @classmethod
    def from_path(
        cls,
        file_path: str,
        logger: EsoFileLogger = None,
        ignore_peaks: bool = True,
        year: Optional[int] = 2002,
    ) -> "EsoFile":
        """ """
        file_path, file_name, file_created = get_file_information(file_path)
        if logger is None:
            logger = EsoFileLogger(file_path.name)
        with logger.log_task(f"Process '{file_path.suffix}' file!"):
            eso_files = cls._process_raw_data(Path(file_path), logger, ignore_peaks, year)
            if len(eso_files) == 1:
                return eso_files[0]
            else:
                raise MultiEnvFileRequired(
                    f"Cannot populate file {file_path}. "
                    f"as there are multiple environments included.\n"
                    f"Use '{cls.__name__}.from_multi_env_path' "
                    f"to generate multiple files."
                )

    @classmethod
    def from_multi_env_path(
        cls,
        file_path: str,
        logger: EsoFileLogger = None,
        ignore_peaks: bool = True,
        year: Optional[int] = 2002,
    ) -> List["EsoFile"]:
        file_path, file_name, file_created = get_file_information(file_path)
        if logger is None:
            logger = EsoFileLogger(Path(file_path).name)
        with logger.log_task(f"Process multi-environment {file_path.suffix} file!"):
            return cls._process_raw_data(Path(file_path), logger, ignore_peaks, year)
