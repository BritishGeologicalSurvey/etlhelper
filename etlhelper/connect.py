"""
Connect to database
"""
from etlhelper.db_helper_factory import DB_HELPER_FACTORY


def connect(db_params, password_variable=None, **kwargs):
    """
    Return database connection.

    :param db_params: DbParams object or similar with appropriate attributes
    :param password_variable: str, name of environment variable with password
    :param kwargs: connection specific keyword arguments e.g. row_factory
    :return: Connection object
    """
    helper = DB_HELPER_FACTORY.from_db_params(db_params)
    # Helpers will raise ETLHelperConnectionError if connection fails
    conn = helper.connect(db_params, password_variable, **kwargs)
    return conn


def get_connection_string(db_params, password_variable):
    """
    Get a connection string

    :param db_params: DbParams object or similar with appropriate attributes
    :param password_variable: str, name of environment variable with password
    :return: str, Connection string
    """
    helper = DB_HELPER_FACTORY.from_db_params(db_params)
    return helper.get_connection_string(db_params, password_variable)


def get_sqlalchemy_connection_string(db_params, password_variable):
    """
    Get a SQLAlchemy connection string.

    :param db_params: DbParams object or similar with appropriate attributes
    :param password_variable: str, name of environment variable with password
    :return: str, Connection string
    """
    helper = DB_HELPER_FACTORY.from_db_params(db_params)
    return helper.get_sqlalchemy_connection_string(db_params, password_variable)
