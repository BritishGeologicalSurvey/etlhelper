"""Test for the helper factory"""
from unittest.mock import (
    MagicMock,
    Mock,
)

import oracledb
import psycopg2
import pyodbc
import pytest

from etlhelper import DbParams
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.exceptions import ETLHelperHelperError
from etlhelper.db_helpers import OracleDbHelper, PostgresDbHelper, MSSQLDbHelper


@pytest.mark.parametrize("dbtype_keyword, expected_helper",
                         [('ORACLE', OracleDbHelper),
                          ('PG', PostgresDbHelper),
                          ('MSSQL', MSSQLDbHelper)])
def test_from_dbparams(dbtype_keyword, expected_helper):
    """
    Tests correct helper produced given a db params object
    """
    db_params = MagicMock(DbParams)
    db_params.dbtype = dbtype_keyword
    helper = DB_HELPER_FACTORY.from_db_params(db_params)
    assert isinstance(helper, expected_helper)


@pytest.mark.parametrize("expected_helper, db_class",
                         [(OracleDbHelper, oracledb.Connection),
                          (PostgresDbHelper, psycopg2.extensions.connection),
                          (MSSQLDbHelper, pyodbc.Connection)])
def test_from_conn(expected_helper, db_class):
    """
    Tests correct helper produced given a conn object
    """
    conn = Mock()
    # conn.__class__ = oracledb.Connection
    conn.__class__ = db_class
    helper = DB_HELPER_FACTORY.from_conn(conn)
    assert isinstance(helper, expected_helper)


def test_from_conn_not_registered():
    """
    Tests helpful error message on attempt to choose unregistered conn type.
    """
    conn = Mock()
    conn.__class__ = "Not a real class"

    with pytest.raises(ETLHelperHelperError,
                       match=r'Unsupported connection type.*'):
        DB_HELPER_FACTORY.from_conn(conn)


def test_from_db_params_not_registered():
    """
    Tests helpful error message on attempt to choose unregistered db_params
    type.
    """
    db_params = MagicMock(DbParams)
    db_params.dbtype = 'Not a real type'

    with pytest.raises(ETLHelperHelperError,
                       match=r'Unsupported DbParams.dbtype.*'):
        DB_HELPER_FACTORY.from_db_params(db_params)


def test_from_conn_bad_type():
    with pytest.raises(ETLHelperHelperError,
                       match=r'Expected connection-like object.*'):
        DB_HELPER_FACTORY.from_conn('some string')


def test_from_db_params_bad_type():
    with pytest.raises(ETLHelperHelperError,
                       match=r'Expected DbParams-like object.*'):
        DB_HELPER_FACTORY.from_db_params('some string')
