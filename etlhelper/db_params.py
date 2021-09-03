"""
This module defines the DbParams class for storing database connection
parameters.
"""
import os
import socket

from etlhelper.connect import connect, get_connection_string, get_sqlalchemy_connection_string
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.exceptions import ETLHelperDbParamsError, ETLHelperHelperError


class DbParams(dict):
    """Generic data holder class for database connection parameters."""

    """
    As we do not know which parameters will be provided in advance, DbParams
    subclasses dict, to give dynamic attributes, following the pattern described
    here: https://amir.rachum.com/blog/2016/10/05/python-dynamic-attributes/
    """
    def __init__(self, dbtype='dbtype not set', **kwargs):
        kwargs.update(dbtype=dbtype.upper())
        super().__init__(kwargs)
        self.validate_params()

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            # getattr should raise AttributeError, not KeyError
            # https://docs.python.org/3/library/functions.html#getattr
            raise AttributeError(f'No such attribute: {item}')

    def __setattr__(self, item, value):
        self[item] = value

        # Validate the updated parameter set, and remove bad item if failed
        try:
            self.validate_params()
        except ETLHelperDbParamsError:
            self.pop(item)
            raise

    def __dir__(self):
        return super().__dir__() + [str(k) for k in self.keys()]

    def validate_params(self):
        """
        Validate database parameters.

        Should validate that a dbtype is a valid one and that the appropriate
        params have been passed for a particular db_type.

        :raises ETLHelperParamsError: Error if params are invalid
        """
        # Get a set of the attributes to compare against required attributes.
        given = set(self.keys())

        try:
            required_params = DB_HELPER_FACTORY.from_dbtype(self.dbtype).required_params
        except ETLHelperHelperError:
            msg = f'{self.dbtype} not in valid types ({DB_HELPER_FACTORY.helpers.keys()})'
            # from None suppresses lower errors in the stack trace
            # Deeper error is recorded in ETLHelperDbParamsError.__context__
            raise ETLHelperDbParamsError(msg) from None

        unset_params = (given ^ required_params) & required_params
        if unset_params:
            msg = f'{unset_params} not set. Required parameters are {required_params}'
            raise ETLHelperDbParamsError(msg)

        valid_params = required_params.union({'dbtype'})
        bad_params = given ^ valid_params
        if bad_params:
            msg = f"Invalid parameter(s): {bad_params}"
            raise ETLHelperDbParamsError(msg)

    @classmethod
    def from_environment(cls, prefix='ETLHelper_'):
        """
        Create DbParams object from parameters specified by environment
        variables e.g. ETLHelper_dbtype, ETLHelper_host, ETLHelper_port, etc.
        :param prefix: str, prefix to environment variable names
        """
        dbparams_keys = [key for key in os.environ if key.startswith(prefix)]
        dbparams_from_env = {key.replace(prefix, '').lower(): os.environ[key]
                             for key in dbparams_keys}

        # Ensure dbtype has been set
        dbtype_var = f'{prefix}dbtype'
        dbtype = dbparams_from_env.get('dbtype', None)
        if dbtype is None:
            msg = f"{dbtype_var} environment variable is not set"
            raise ETLHelperDbParamsError(msg)

        # Only include the required params
        # This prevents something like ETLHelper_password being added
        required_params = DB_HELPER_FACTORY.from_dbtype(dbtype).required_params | {'dbtype'}
        dbparams_from_env = {key: dbparams_from_env[key] for key in required_params}

        return cls(**dbparams_from_env)

    def is_reachable(self):
        """
        Test whether network allows opening of tcp/ip connection to database. No
        username or password are required.

        :return bool:
        """
        items = dict(self.items())
        if items['dbtype'] == 'SQLITE':
            raise ValueError("SQLITE DbParam does not require connection over network")

        s = socket.socket()
        s.settimeout(5)
        try:
            # Connection succeeds
            s.connect((items['host'], int(items['port'])))
            return True
        except OSError:
            # Failed to connect
            return False
        finally:
            s.close()

    def connect(self, password_variable=None, **kwargs):
        """
        Return database connection.

        :param password_variable: str, name of environment variable with password
        :param kwargs: connection specific keyword arguments e.g. row_factory
        :return: Connection object
        """
        return connect(self, password_variable, **kwargs)

    def get_connection_string(self, password_variable=None):
        """
        Return the connection string.

        :param password_variable: str, name of environment variable with password
        :return: str, Connection string
        """
        return get_connection_string(self, password_variable)

    def get_sqlalchemy_connection_string(self, password_variable=None):
        """
        Get a SQLAlchemy connection string.

        :param password_variable: str, name of environment variable with password
        :return: str, Connection string
        """
        return get_sqlalchemy_connection_string(self, password_variable)

    def copy(self):
        """
        Return a shallow copy of DbParams object.

        :return DbParams: DbParams object with same attributes as original.
        """
        return self.__class__(**self)

    @property
    def paramstyle(self):
        """The DBAPI2 paramstyle attribute for database type"""
        return DB_HELPER_FACTORY.from_dbtype(self.dbtype).paramstyle

    def __repr__(self):
        key_val_str = ", ".join([f"{key}='{self[key]}'" for key in self.keys()])
        return f"DbParams({key_val_str})"

    def __str__(self):
        return self.__repr__()
