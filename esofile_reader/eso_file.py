import os

from esofile_reader.base_file import BaseFile
from esofile_reader.processing.esofile_processor import read_file
from esofile_reader.totals_file import TotalsFile


class IncompleteFile(Exception):
    """ Exception raised when the file is not complete. """
    pass


class PeaksNotIncluded(Exception):
    """ Exception is raised when EsoFile has been processed without peaks. """
    # PeaksNotIncluded("Peak values are not included, it's required to "
    #                  "add kwarg 'ignore_peaks=False' when processing the file."
    #                  "\nNote that peak values are only applicable for"
    #                  "raw Eso files.")
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
                 ignore_peaks=True, suppress_errors=False):
        super().__init__()
        self.file_path = file_path
        self.peak_outputs = None
        self.populate_content(monitor=monitor,
                              report_progress=report_progress,
                              ignore_peaks=ignore_peaks,
                              suppress_errors=suppress_errors)

    def populate_content(self, monitor=None, report_progress=True,
                         ignore_peaks=True, suppress_errors=False):
        """ Process the eso file to populate attributes. """
        self.file_name = os.path.splitext(os.path.basename(self.file_path))[0]
        self.file_timestamp = os.path.getctime(self.file_path)

        content = read_file(
            self.file_path,
            monitor=monitor,
            report_progress=report_progress,
            ignore_peaks=ignore_peaks,
            suppress_errors=suppress_errors
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

    def get_totals(self):
        """ Generate a new 'Building' eso file. """
        if self.complete:
            return TotalsFile(self)
        else:
            raise IncompleteFile(f"Cannot generate totals, "
                                 f"file {self.file_path} is not complete!")
