"""
Database helper classes using Factory Pattern
"""
from abc import ABCMeta, abstractmethod
import sys
import os

from etlhelper.exceptions import ETLHelperConnectionError


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
        # Dummy function to allowing calling in connect below
        # (To satisfy Pylint)
        # Throws exception if not overridden
        self._connect_func = lambda conn_str: 1/0

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
            # This method is not defined? (Only an attribute)
            conn = self._connect_func(conn_str, **kwargs)
        except self.connect_exceptions as exc:
            msg = f"Error connecting to {conn_str} via dbapi: {exc}"
            raise ETLHelperConnectionError(msg)
            # sys.exit(1)

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
            print("Name of password environment variable e.g. ORACLE_PASSWORD is required")
            sys.exit(1)
        try:
            return os.environ[password_variable]
        except KeyError:
            print(f"Password environment variable ({password_variable}) is not set")
            sys.exit(1)

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
