import os
from esofile_reader.base_file import BaseFile, IncompleteFile
from esofile_reader.totals_file import TotalsFile
from esofile_reader.diff_file import DiffFile

try:
    from esofile_reader.processing.esofile_processor import read_file
except ModuleNotFoundError:
    import pyximport

    pyximport.install()
    from esofile_reader.processing.esofile_processor import read_file


class PeaksNotIncluded(Exception):
    """ Exception is raised when EsoFile has been processed without peaks. """
    pass


class EsoFile(BaseFile):
    """
    The ESO class holds processed EnergyPlus output ESO file data.

    The results are stored in a dictionary using string interval identifiers
    as keys and pandas.DataFrame like classes as values.

    A structure for data bins is works as for 'BaseFile'.

    Attributes
    ----------
    file_path : str
        A full path of the ESO file.
    file_timestamp : datetime.datetime
        Time and date when the ESO file has been generated (extracted from original Eso file).
    header : dict of {str : dict of {int : list of str}}
        A dictionary to store E+ header line
        {period : {ID : (key name, variable name, units)}}
    outputs : dict of {str : Outputs subclass}
        A dictionary holding categorized outputs using pandas.DataFrame like classes.

    Parameters
    ----------
    file_path : path like object
        A full path of the ESO file
    report_progress : bool, default True
        Processing progress is reported in terminal when set as 'True'.
    ignore_peaks : bool, default: True
        Ignore peak values from 'Daily'+ intervals.
    suppress_errors: bool, default False
        Do not raise IncompleteFile exceptions when processing fails

    Raises
    ------
    IncompleteFile

    """

    def __init__(self, file_path, monitor=None, report_progress=True,
                 ignore_peaks=True, suppress_errors=False, year=2002):
        super().__init__()
        self.file_path = file_path
        self.peak_outputs = None
        self.environments = None
        self.populate_content(monitor=monitor,
                              report_progress=report_progress,
                              ignore_peaks=ignore_peaks,
                              suppress_errors=suppress_errors,
                              year=year)

    def populate_content(self, monitor=None, report_progress=True,
                         ignore_peaks=True, suppress_errors=False, year=2002):
        """ Process the eso file to populate attributes. """
        self.file_name = os.path.splitext(os.path.basename(self.file_path))[0]
        self.file_timestamp = os.path.getctime(self.file_path)

        content = read_file(
            self.file_path,
            monitor=monitor,
            report_progress=report_progress,
            ignore_peaks=ignore_peaks,
            suppress_errors=suppress_errors,
            year=year
        )

        if content:
            self._complete = True
            (
                self.environments,
                self.header,
                self.outputs,
                self.peak_outputs,
                self.header_tree,
            ) = content

        else:
            if not suppress_errors:
                raise IncompleteFile(f"Unexpected end of the file reached!\n"
                                     f"File '{self.file_path}' is not complete.")

    def _get_peak_results(self, variables, output_type, start_date=None, end_date=None,
                          add_file_name="row", include_interval=False,
                          include_id=False, part_match=False, timestamp_format="default"):
        """ Return peak results. """
        frames = []
        groups = self._find_pairs(variables, part_match=part_match)

        for interval, ids in groups.items():
            try:
                data_set = self.peak_outputs[output_type][interval]
            except KeyError:
                print(f"There are no peak outputs stored for interval: '{interval}'.")
                continue

            df = data_set.get_results(ids, start_date, end_date)
            df.columns = self._create_header_mi(interval, df.columns)

            if not include_id:
                df.columns = df.columns.droplevel("id")

            if not include_interval:
                df.columns = df.columns.droplevel("interval")

            frames.append(df)

        return self._merge_frame(frames, timestamp_format, add_file_name)

    def get_results(self, variables, output_type="standard", **kwargs):
        """
        Return a pandas.DataFrame object with results for given variables.

        This function extracts requested set of outputs from the file
        and converts to specified units if requested.

        Parameters
        ----------
        variables : Variable or list of (Variable)
            Requested variables..
        output_type : {'standard', global_max','global_min', 'local_max', 'local_min'}
                Requested type of results.

        **kwargs
            start_date : datetime like object, default None
                A start date for requested results.
            end_date : datetime like object, default None
                An end date for requested results.
            add_file_name : ('row','column',None)
                Specify if file name should be added into results df.
            include_interval : bool
                Decide if 'interval' information should be included on
                the results df.
            include_id : bool
                Decide if variable 'id' should be included on the results df.
            part_match : bool
                Only substring of the part of variable is enough
                to match when searching for variables if this is True.
            units_system : {'SI', 'IP'}
                Selected units type for requested outputs.
            rate_to_energy_dct : dct
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
                ignore = ["units_system", "rate_to_energy_dct",
                          "rate_units", "energy_units"]
                kwargs = {k: v for k, v in kwargs.items() if k not in ignore}
                df = self._get_peak_results(variables, output_type, **kwargs)

            else:
                raise PeaksNotIncluded("Peak values are not included, it's "
                                       "required to add kwarg 'ignore_peaks=False' "
                                       "when processing the file.")
        else:
            df = super().get_results(variables, output_type=output_type, **kwargs)

        return df

    def generate_totals(self):
        """ Generate a new 'Totals' file. """
        if self.complete:
            return TotalsFile(self)
        else:
            raise IncompleteFile(f"Cannot generate totals, "
                                 f"file {self.file_path} is not complete!")

    def generate_diff(self, other_file):
        """ Generate a new 'Building' eso file. """
        if self.complete:
            return DiffFile(self, other_file)
        else:
            raise IncompleteFile(f"Cannot generate totals, "
                                 f"file {self.file_path} is not complete!")
