"""
Database helper classes using Factory Pattern
"""
import hashlib
import logging
import os
import socket
from abc import ABCMeta, abstractmethod
from collections import namedtuple

from etlhelper import exceptions

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
        self._pools = {}
        # This is overridden with real connect method when DbHelper class is
        # successfully initialised if driver is installed
        self._connect_func = self._raise_missing_driver_error_on_connect

    def validate_params(self, **kwargs):
        """
        Validate database parameters.

        Should validate the appropriateparams have been passed.

        :raises ETLHelperParamsError: Error if params are invalid
        """
        # Get a set of the attributes to compare against required attributes.
        given = set(kwargs.keys())

        required_params = self.required_params

        unset_params = (given ^ required_params) & required_params
        if unset_params:
            msg = f'{unset_params} not set. Required parameters are {required_params}'
            raise exceptions.ETLHelperDbParamsError(msg)

        bad_params = given ^ required_params
        if bad_params:
            msg = f"Invalid parameter(s): {bad_params}"
            raise exceptions.ETLHelperDbParamsError(msg)

    def validate(self, password_variable, **kwargs):
        """
        Validates kwargs for particular database type,
        creates connection hash based on connection string
        used to quickly access connection pool later,
        creates db_params namedtuple.

        :param password_variable: str, name of environment variable with password
        :param kwargs: helper specific keyword arguments to be validated
        :return: db_params named tuple and connection hash
        """

        self.validate_params(**kwargs)
        DbParams = namedtuple('DbParams', self.required_params)
        db_params = DbParams(**kwargs)
        connection_string = self.get_connection_string(db_params, password_variable)
        h = hashlib.sha256(connection_string.encode()).hexdigest()
        return db_params, h

    @abstractmethod
    def get_connection(self, connection_hash, db_params, password_variable):
        """Gets connection based on connection hash recieved from register
        or creates new one, possibly from pool

        :param connection_hash: str, connection hash from register
        :param db_params: namedtuple, db_params holding connection parameters
        :param password_variable: str, name of environment variable holding pw
        :return: connection object, may from pool
        """

        return None

    def close(self, connection_hash, connection):
        """
        Closes connection iether based on it's hash or directly
        :param connection_hash: str, connection hash obtained from register
        :param connection: connection itself
        """

        connection.close()

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
            connection = self._connect_func(conn_str, **kwargs)
        except self.connect_exceptions as exc:
            msg = f"Error connecting to {conn_str} via dbapi: {exc}"
            raise exceptions.ETLHelperConnectionError(msg)
        return connection

    def is_reachable(self, db_params):
        """
        Test whether network allows opening of tcp/ip connection to database. No
        username or password are required.

        :return bool:
        """

        s = socket.socket()
        try:
            # Connection succeeds
            s.connect((db_params.host, int(db_params.port)))
            return True
        except OSError:
            # Failed to connect
            return False
        finally:
            s.close()

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
            raise exceptions.ETLHelperConnectionError(msg)
        try:
            return os.environ[password_variable]
        except KeyError:
            msg = f"Password environment variable ({password_variable}) is not set"
            logger.error(msg)
            raise exceptions.ETLHelperConnectionError(msg)

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
        raise exceptions.ETLHelperConnectionError(self.missing_driver_msg)
