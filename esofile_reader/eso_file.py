from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from esofile_reader.abstractions.base_file import BaseFile, get_file_information
from esofile_reader.constants import *
from esofile_reader.df.df_tables import DFTables
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import PathLike
from esofile_reader.processing.esofile_time import get_n_days_from_cumulative
from esofile_reader.processing.progress_logger import BaseLogger
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
        file_type: str,
        tables: DFTables,
        search_tree: Tree,
        peak_tables: Optional[Dict[str, DFTables]] = None,
    ):
        super().__init__(file_path, file_name, file_created, tables, search_tree, file_type)
        self.peak_tables = peak_tables

    def __copy__(self):
        return type(self)(
            file_path=self.file_path,
            file_name=self.file_name,
            file_created=self.file_created,
            file_type=self.file_type,
            tables=copy(self.tables),
            search_tree=copy(self.search_tree),
            peak_tables=copy(self.peak_tables),
        )

    @classmethod
    def _process_env(
        cls, raw_data: RawData, parser: Parser, logger: BaseLogger, year: int
    ) -> Tuple[Tree, DFTables, Optional[Dict[str, DFTables]]]:
        """ Process an environment raw data into final classes. """
        logger.set_maximum_progress(raw_data.get_n_tables() + 1)
        logger.log_section("sanitizing data")
        parser.sanitize(raw_data)

        logger.log_section("generating search tree")
        tree = Tree.from_header_dict(raw_data.header)
        logger.increment_progress()

        logger.log_section("processing dates")
        dates = parser.cast_to_datetime(raw_data, year)
        n_days = get_n_days_from_cumulative(raw_data.cumulative_days, dates)
        special_columns = {N_DAYS_COLUMN: n_days, DAY_COLUMN: raw_data.days_of_week}

        logger.log_section("generating tables")
        tables = parser.cast_to_df(
            raw_data.outputs, raw_data.header, dates, special_columns, logger
        )

        if raw_data.peak_outputs:
            logger.log_section("generating peak tables")
            peak_tables = parser.cast_peak_to_df(
                raw_data.peak_outputs, raw_data.header, dates, logger
            )
        else:
            peak_tables = None

        return tree, tables, peak_tables

    @classmethod
    def from_path(
        cls,
        file_path: str,
        logger: BaseLogger = None,
        ignore_peaks: bool = True,
        year: Optional[int] = None,
    ) -> "EsoFile":
        eso_files = cls.from_multienv_path(file_path, logger, ignore_peaks, year)
        if len(eso_files) == 1:
            return eso_files[0]
        else:
            raise MultiEnvFileRequired(
                f"Cannot process file '{file_path}'"
                f" as are multiple environments in place.\n"
                f"Use '{cls.__name__}.from_multi_env_path' "
                f"to generate multiple files."
            )

    @classmethod
    def from_multienv_path(
        cls,
        file_path: str,
        logger: BaseLogger = None,
        ignore_peaks: bool = True,
        year: Optional[int] = None,
    ) -> List["EsoFile"]:
        file_path, file_name, file_created = get_file_information(file_path)
        if logger is None:
            logger = BaseLogger(file_path.name)
        parser = choose_parser(file_path)
        with logger.log_task(f"Read '{file_path.suffix}' file"):
            all_raw_data = parser.process_file(file_path, logger, ignore_peaks)
        eso_files = []
        for i, raw_data in enumerate(reversed(all_raw_data)):
            with logger.log_task(f"Process environment: '{raw_data.environment_name}'."):
                tree, tables, peak_tables = cls._process_env(raw_data, parser, logger, year)
                name = f"{file_name} - {raw_data.environment_name}" if i > 0 else file_name
                ef = cls(
                    file_path=file_path,
                    file_name=name,
                    file_created=file_created,
                    tables=tables,
                    search_tree=tree,
                    peak_tables=peak_tables,
                    file_type=file_path.suffix,
                )
                eso_files.append(ef)
            return eso_files
