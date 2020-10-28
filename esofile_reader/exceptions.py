class VariableNotFound(Exception):
    """ Exception raised when requested variable id is not available. """

    pass


class InvalidOutputType(Exception):
    """ Exception raised when the output time is invalid. """

    pass


class InvalidUnitsSystem(Exception):
    """ Exception raised when units system is invalid. """

    pass


class IncompleteFile(Exception):
    """ Exception raised when the file is not complete. """

    pass


class CannotAggregateVariables(Exception):
    """ Exception raised when variables cannot be aggregated. """

    pass


class NoResults(Exception):
    """ Exception raised when results are requested from an incomplete file. """

    pass


class PeaksNotIncluded(Exception):
    """ Exception is raised when 'EsoFile' has been processed without peaks. """

    pass


class MultiEnvFileRequired(Exception):
    """ Exception raised when populating single 'EsoFile' with multi env data."""

    pass


class InvalidLineSyntax(Exception):
    """ Exception raised for an unexpected line syntax. """

    pass


class BlankLineError(Exception):
    """ Exception raised when eso file contains blank line.  """

    pass


class InsuficientHeaderInfo(Exception):
    """ Exception raised when excel header does not contain enough identifiers. """

    pass


class DuplicateVariable(Exception):
    """ Exception raised header contains duplicate variable. """

    def __init__(self, text, clean_tree, duplicates):
        super().__init__(text)
        self.clean_tree = clean_tree
        self.duplicates = duplicates


class FormatNotSupported(Exception):
    """ Exception raised when processing from unsupported format is requested. """

    pass


class LeapYearMismatch(Exception):
    """ Exception raised when requested year does not match real calendar. """

    pass


class StartDayMismatch(Exception):
    """ Exception raised when start day for given year does not match real calendar. """

    pass
