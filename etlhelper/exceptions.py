"""
ETL Helper Exception classes
"""


class ETLHelperError(Exception):
    """Base class for exceptions in this module"""


class ETLHelperConnectionError(ETLHelperError):
    """Exception raised for bad database connections"""


class ETLHelperQueryError(ETLHelperError):
    """Exception raised for SQL query errors and similar"""


class ETLHelperDbParamsError(ETLHelperError):
    """Exception raised for bad database parameters"""


class ETLHelperExtractError(ETLHelperError):
    """Exception raised when extracting data."""


class ETLHelperInsertError(ETLHelperError):
    """Exception raised when inserting data."""


class ETLHelperHelperError(ETLHelperError):
    """Exception raised when helper selection fails."""
