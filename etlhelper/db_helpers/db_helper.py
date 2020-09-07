"""
Database helper classes using Factory Pattern
"""
from abc import ABCMeta, abstractmethod
import logging
import os

from etlhelper.exceptions import ETLHelperConnectionError

logger = logging.getLogger('etlhelper')


class DbHelper(metaclass=ABCMeta):
    """
    Abstract Base Class for DBHelpers
    """
    sql_exceptions = None
    connect_exceptions = None
    paramstyle = None

    @abstractmethod
    def __init__(self):
        self.sql_exceptions = tuple()
        self.connect_exceptions = tuple()
        self.required_params = set()
        self.paramstyle = ''
        self.missing_driver_msg = ''
        # This is overridden with real connect method when DbHelper class is
        # successfully initialised if driver is installed
        self._connect_func = self._raise_missing_driver_error_on_connect

    def connect(self, db_params, password_variable=None, **kwargs):
        """
        Return a connection (as appropriate), configured for
        the database with the password obtained from environment variable.  These
        connection classes provide Python's dbapi interface (see PEP 249).  The
        dbapi interface interacts with data row-by-row and is usroed for low-level
        functions.

        :param db_params: DbParams object or similar with appropriate attributes
        :param password_variable: str, name of environment variable with password
        :param kwargs: connection specific keyword arguments e.g. encoding
        :return: Connection object
        """
        # Prepare connection string
        conn_str = self.get_connection_string(db_params, password_variable)

        # Create connection
        try:
            conn = self._connect_func(conn_str, **kwargs)
        except self.connect_exceptions as exc:
            msg = f"Error connecting to {conn_str} via dbapi: {exc}"
            raise ETLHelperConnectionError(msg)
        return conn

    @staticmethod
    def get_password(password_variable):
        """
        Read password from environment variable.
        :param password_variable: str, name of environment variable with password
        :return: str, password
        :raises ETLHelperDbParamsError: Exception when parameter not defined
        """
        if not password_variable:
            msg = "Name of password environment variable e.g. ORACLE_PASSWORD is required"
            logger.error(msg)
            raise ETLHelperConnectionError(msg)
        try:
            return os.environ[password_variable]
        except KeyError:
            msg = f"Password environment variable ({password_variable}) is not set"
            logger.error(msg)
            raise ETLHelperConnectionError(msg)

    @staticmethod
    @abstractmethod
    def get_connection_string(db_params, password_variable):
        """
        :returns: str
        """
        return

    @staticmethod
    def executemany(cursor, query, chunk):
        """
        Call executemany method appropriate to database.  Overridden for PostgreSQL.

        :param cursor: Open database cursor.
        :param query: str, SQL query
        :param chunk: list, Rows of parameters.
        """
        cursor.executemany(query, chunk)

    @staticmethod
    def cursor(conn):
        """
        Return a cursor on the connection.  Overridded for SQLite.

        :param conn: Open database connection.
        """
        return conn.cursor()

    def _raise_missing_driver_error_on_connect(self, *args, **kwargs):
        """
        Raise an exception with helpful message if user tries to connect without driver installed.
        This function replaces a connect function, so *args and **kwargs are collected to allow
        it to accept whatever would be passed to that function.
        """
        raise ETLHelperConnectionError(self.missing_driver_msg)
