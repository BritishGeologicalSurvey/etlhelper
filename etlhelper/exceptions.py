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


def log_and_raise(chunk, query, helper, exc, logger, conn):
    conn.rollback()
    msg = (f"Failed to insert chunk: [{chunk[0]}, ..., {chunk[-1]}]\n"
           f"SQL query raised an error.\n\n{query}\n\n"
           f"Required paramstyle: {helper.paramstyle}\n\n{exc}\n")
    logger.debug(msg)
    raise ETLHelperInsertError(msg)


def log_and_continue(chunk, query, helper, exc, logger, conn):
    msg = (f"Failed to insert chunk: [{chunk[0]}, ..., {chunk[-1]}]\n"
           f"SQL query raised an error.\n\n{query}\n\n"
           f"Required paramstyle: {helper.paramstyle}\n\n{exc}\n")
    logger.error(msg)
