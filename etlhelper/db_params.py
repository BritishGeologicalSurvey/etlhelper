"""
This module defines the DbParams class for storing database connection
parameters.
"""
import os

from etlhelper.exceptions import ETLHelperDbParamsError, ETLHelperHelperError
from etlhelper.db_helper_factory import DB_HELPER_FACTORY


class DbParams:
    """Generic data holder class for database connection parameters"""

    def __init__(self, dbtype=None, odbc_driver=None, host=None, port=None,
                 dbname=None, username=None):
        self.dbtype = dbtype.upper()
        self.odbc_driver = odbc_driver
        self.host = host
        self.port = str(port)
        self.dbname = dbname
        self.username = username
        self.validate_params()

    def validate_params(self):
        """
        Validate database parameters.

        Should validate that a dbtype is a valid one and that the appropriate
        params have been passed for a particular db_type.

        :raises ETLHelperParamsError: Error if params are invalid
        """
        # Get a set of the attributes to compare against required attributes.
        given = set(dir(self))

        try:
            required_params = DB_HELPER_FACTORY.from_dbtype(self.dbtype).required_params
        except ETLHelperHelperError:
            msg = f'{self.dbtype} not in valid types ({DB_HELPER_FACTORY.helpers.keys()})'
            raise ETLHelperDbParamsError(msg)

        if (given ^ required_params) & required_params:
            msg = f'Parameter not set. Required parameters are {required_params}'
            raise ETLHelperDbParamsError(msg)

    @classmethod
    def from_environment(cls, prefix='ETLHelper_'):
        """
        Create DbParams object from parameters specified by environment
        variables e.g. ETLHelper_DBTYPE, ETLHelper_HOST, ETLHelper_PORT, etc.
        :param prefix: str, prefix to environment variable names
        """
        return cls(
            dbtype=os.getenv(f'{prefix}DBTYPE'),
            odbc_driver=os.getenv(f'{prefix}DBDRIVER'),
            host=os.getenv(f'{prefix}HOST'),
            port=os.getenv(f'{prefix}PORT'),
            dbname=os.getenv(f'{prefix}DBNAME'),
            username=os.getenv(f'{prefix}USER'),
        )

    def __repr__(self):
        return (
            f"DbParams(dbtype='{self.dbtype}', driver='{self.odbc_driver}', host='{self.host}', "
            f"port='{self.port}', dbname='{self.dbname}', username='{self.username}')")

    def __str__(self):
        return self.__repr__()
