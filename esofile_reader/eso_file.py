import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Union, List

from esofile_reader.logger import logger
from esofile_reader.search_tree import Tree
from esofile_reader.tables.df_tables import DFTables

try:
    from typing import ForwardRef
except ImportError:
    from typing import _ForwardRef as ForwardRef

import pandas as pd
from esofile_reader.constants import *
from esofile_reader.base_file import BaseFile
from esofile_reader.processing.monitor import DefaultMonitor
from esofile_reader.exceptions import *
from esofile_reader.mini_classes import Variable

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
        peak_outputs: Dict[str, DFTables] = None,
    ):
        super().__init__(
            file_path, file_name, file_created, tables, search_tree, file_type="eso"
        )
        self.peak_outputs = peak_outputs

    @classmethod
    def from_multi_env_eso_file(
        cls,
        file_path: str,
        monitor: DefaultMonitor = None,
        ignore_peaks: bool = True,
        year: int = 2002,
    ) -> List[ForwardRef("EsoFile")]:
        """ Generate independent 'EsoFile' for each environment. """
        eso_files = []
        file_path = Path(file_path)
        file_name = file_path.stem
        file_created = datetime.utcfromtimestamp(os.path.getctime(file_path))
        content = read_file(file_path, monitor=monitor, ignore_peaks=ignore_peaks, year=year)
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
        return eso_files

    def _get_peak_results(
        self,
        variables: List[Variable],
        output_type: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        add_file_name: str = "row",
        include_table_name: bool = False,
        include_id: bool = False,
        part_match: bool = False,
        timestamp_format: str = "default",
    ) -> pd.DataFrame:
        """ Return local peak results. """
        frames = []
        groups = self._find_pairs(variables, part_match=part_match)

        for table, ids in groups.items():
            try:
                df = self.peak_outputs[output_type].get_results(
                    table, ids, start_date, end_date
                )
            except KeyError:
                logger.warning(f"There are no peak outputs stored for table: '{table}'.")
                continue

            if not include_id:
                df.columns = df.columns.droplevel(ID_LEVEL)

            if not include_table_name:
                df.columns = df.columns.droplevel(TABLE_LEVEL)

            frames.append(df)

        return self._merge_frame(frames, timestamp_format, add_file_name)

    def get_results(
        self,
        variables: Union[Variable, List[Variable]],
        output_type: str = "standard",
        **kwargs,
    ) -> pd.DataFrame:
        """
        Return a pandas.DataFrame object with results for given variables.

        This function extracts requested set of outputs from the file
        and converts to specified units if requested.

        Parameters
        ----------
        variables : Variable or list of (Variable)
            Requested variables..
        output_type : {'standard', global_max','global_min', 'local_max', 'local_min'}
                Requested type_ of results.

        **kwargs
            start_date : datetime like object, default None
                A start date for requested results.
            end_date : datetime like object, default None
                An end date for requested results.
            add_file_name : ('row','column',None)
                Specify if file name should be added into results df.
            include_table_name : bool
                Decide if 'table' information should be included on
                the results df.
            include_day : bool
                Add day of week into index, this is applicable only for 'timestep',
                'hourly' and 'daily' outputs.
            include_id : bool
                Decide if variable 'id' should be included on the results df.
            part_match : bool
                Only substring of the part of variable is enough
                to match when searching for variables if this is True.
            units_system : {'SI', 'IP'}
                Selected units type_ for requested outputs.
            rate_to_energy_ : bool
                Defines if 'rate' will be converted to energy.
            rate_units : {'W', 'kW', 'MW', 'Btu/h', 'kBtu/h'}
                Convert default 'Rate' outputs to requested units.
            energy_units : {'J', 'kJ', 'MJ', 'GJ', 'Btu', 'kWh', 'MWh'}
                Convert default 'Energy' outputs to requested units
            timestamp_format : str
                Specified str format of a datetime timestamp.

        Returns
        -------
        pandas.DataFrame
            Results for requested variables.

        """
        if output_type in ["local_max", "local_min"]:
            if self.peak_outputs:
                ignore = [
                    "units_system",
                    "rate_to_energy",
                    "rate_units",
                    "energy_units",
                    "include_day",
                ]
                kwargs = {k: v for k, v in kwargs.items() if k not in ignore}
                df = self._get_peak_results(variables, output_type, **kwargs)

            else:
                raise PeaksNotIncluded(
                    "Peak values are not included, it's "
                    "required to add kwarg 'ignore_peaks=False' "
                    "when processing the file."
                )
        else:
            df = super().get_results(variables, output_type=output_type, **kwargs)

        return df


class EsoFile(ResultsEsoFile):
    """
    A wrapper class to allow .eso file processing by passing
    file path as a parameter.

    Parameters
    ----------
    file_path : str, or Path
        A full path of the result file.
    monitor : DefaultMonitor
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
        monitor: DefaultMonitor = None,
        ignore_peaks: bool = True,
        year: int = 2002,
    ):
        file_path = Path(file_path)
        file_name = file_path.stem
        file_created = datetime.utcfromtimestamp(os.path.getctime(file_path))
        content = read_file(file_path, monitor=monitor, ignore_peaks=ignore_peaks, year=year)
        environment_names = content[0]
        if len(environment_names) == 1:
            tables = content[1][0]
            peak_outputs = content[2][0]
            tree = content[3][0]
            super().__init__(
                file_path, file_name, file_created, tables, tree, peak_outputs=peak_outputs
            )
        else:
            raise MultiEnvFileRequired(
                f"Cannot populate file {file_path}. "
                f"as there are multiple environments included.\n"
                f"Use '{super().__class__.__name__}.process_multi_env_file' "
                f"to generate multiple files."
            )
