"""
This module defines the DbParams class for storing database connection
parameters.
"""
import os
import socket

from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.exceptions import ETLHelperDbParamsError, ETLHelperHelperError


class DbParams(dict):
    """Generic data holder class for database connection parameters.

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
        # Prepare set of valid_params
        # dbtype has to be added as it is used to determine required_params
        valid_params = DB_HELPER_FACTORY.from_dbtype(self.dbtype).required_params
        valid_params = valid_params.union({'dbtype'})
        if item not in valid_params:
            msg = f"'{item}' is not a valid DbParams attribute: {valid_params}"
            raise AttributeError(msg)

        self[item] = value

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
            raise ETLHelperDbParamsError(msg)

        unset_params = (given ^ required_params) & required_params
        if unset_params:
            msg = f'{unset_params} not set. Required parameters are {required_params}'
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
        if 'dbtype' not in dbparams_from_env:
            msg = f"{dbtype_var} environment variable is not set"
            raise ETLHelperDbParamsError(msg)

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
        try:
            # Connection succeeds
            s.connect((items['host'], int(items['port'])))
            return True
        except OSError:
            # Failed to connect
            return False
        finally:
            s.close()

    def copy(self):
        """
        Return a shallow copy of DbParams object.

        :return DbParams: DbParams object with same attributes as original.
        """
        return self.__class__(**self)

    def __repr__(self):
        key_val_str = ", ".join([f"{key}='{self[key]}'" for key in self.keys()])
        return f"DbParams({key_val_str})"

    def __str__(self):
        return self.__repr__()
