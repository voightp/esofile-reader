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


class NoSharedVariables(Exception):
    """ Raised when source diff files have no common variables. """
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


class InvalidLineSyntax(AttributeError):
    """ Exception raised for an unexpected line syntax. """

    pass


class BlankLineError(Exception):
    """ Exception raised when eso file contains blank line.  """
    pass
