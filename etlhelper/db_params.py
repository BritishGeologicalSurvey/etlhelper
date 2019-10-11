"""
This module defines the DbParams class for storing database connection
parameters.
"""
import os

from etlhelper.exceptions import ETLHelperDbParamsError, ETLHelperHelperError
from etlhelper.db_helper_factory import DB_HELPER_FACTORY


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
        return self[item]

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

    def __repr__(self):
        key_val_str = ", ".join([f"{key}='{self[key]}'" for key in self.keys()])
        return f"DbParams({key_val_str})"

    def __str__(self):
        return self.__repr__()
