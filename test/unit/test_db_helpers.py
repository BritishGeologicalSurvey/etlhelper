"""Unit tests for db_helpers module."""
import builtins
import sqlite3
from unittest.mock import Mock

import oracledb
import psycopg2
import pyodbc
import pytest

from etlhelper import DbParams
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.db_helpers import (
    MSSQLDbHelper,
    OracleDbHelper,
    PostgresDbHelper,
    SQLiteDbHelper
)
from etlhelper.exceptions import ETLHelperConnectionError

# pylint: disable=missing-docstring

ORACLEDB = DbParams(dbtype='ORACLE', host='server', port='1521',
                    dbname='testdb', user='testuser')

MSSQLDB = DbParams(dbtype='MSSQL', host='server', port='1521', dbname='testdb',
                   user='testuser', odbc_driver='test driver')

POSTGRESDB = DbParams(dbtype='PG', host='server', port='1521', dbname='testdb',
                      user='testuser')

SQLITEDB = DbParams(dbtype='SQLITE', filename='/myfile.db')


@pytest.mark.parametrize('helper, expected', [
    (OracleDbHelper, (oracledb.DatabaseError, oracledb.InterfaceError)),
    (MSSQLDbHelper, (pyodbc.DatabaseError, pyodbc.InterfaceError)),
    (PostgresDbHelper, (psycopg2.DatabaseError, psycopg2.InterfaceError)),
    (SQLiteDbHelper, (sqlite3.DatabaseError, sqlite3.InterfaceError))
])
def test_sql_exceptions(helper, expected):
    # DBAPI 2.0 specifies that all exceptions must inherit from DatabaseError
    # or InterfaceError. See https://peps.python.org/pep-0249/#exceptions
    assert helper().sql_exceptions == expected


@pytest.mark.parametrize('helper, expected', [
    (OracleDbHelper, (oracledb.DatabaseError, oracledb.InterfaceError)),
    (MSSQLDbHelper, (pyodbc.DatabaseError, pyodbc.InterfaceError)),
    (PostgresDbHelper, (psycopg2.DatabaseError, psycopg2.InterfaceError)),
    (SQLiteDbHelper, (sqlite3.DatabaseError, sqlite3.InterfaceError))
])
def test_connect_exceptions(helper, expected):
    # DBAPI 2.0 specifies that all exceptions must inherit from DatabaseError
    # or InterfaceError. See https://peps.python.org/pep-0249/#exceptions
    assert helper().connect_exceptions == expected


@pytest.mark.parametrize('helper, expected', [
    (OracleDbHelper, 'named'),
    (MSSQLDbHelper, 'qmark'),
    (PostgresDbHelper, 'pyformat'),
    (SQLiteDbHelper, 'qmark')
])
def test_paramstyle(helper, expected):
    assert helper().paramstyle == expected


@pytest.mark.parametrize('db_params, driver, expected', [
    (ORACLEDB, oracledb, 'testuser/mypassword@server:1521/testdb'),
    (MSSQLDB, pyodbc,
     'DRIVER=test driver;SERVER=tcp:server;PORT=1521;DATABASE=testdb;UID=testuser;PWD=mypassword'), # NOQA
    (POSTGRESDB, psycopg2,
     'host=server port=1521 dbname=testdb user=testuser password=mypassword'),
    (SQLITEDB, sqlite3, '/myfile.db')
])
def test_connect(monkeypatch, db_params, driver, expected):
    # Arrange
    monkeypatch.setenv('DB_PASSWORD', 'mypassword')
    mock_connect = Mock()
    monkeypatch.setattr(driver, 'connect', mock_connect)
    DB_HELPER_FACTORY.from_dbtype.cache_clear()
    helper = DB_HELPER_FACTORY.from_db_params(db_params)

    # Act
    helper.connect(db_params, 'DB_PASSWORD')

    # Assert
    mock_connect.assert_called_with(expected)


@pytest.mark.parametrize('db_params, expected', [
    (ORACLEDB, 'oracle://testuser:mypassword@server:1521/testdb'),
    (MSSQLDB,
     'mssql+pyodbc://testuser:mypassword@server:1521/testdb?driver=test+driver'),
    # NOQA
    (POSTGRESDB, 'postgresql://testuser:mypassword@server:1521/testdb'),
    (SQLITEDB, 'sqlite:////myfile.db')
])
def test_sqlalchemy_conn_string(monkeypatch, db_params, expected):

    monkeypatch.setenv('DB_PASSWORD', 'mypassword')
    helper = DB_HELPER_FACTORY.from_db_params(db_params)
    conn_str = helper.get_sqlalchemy_connection_string(db_params, 'DB_PASSWORD')

    assert conn_str == expected


@pytest.mark.parametrize('db_params, driver', [
    (ORACLEDB, 'oracledb'), (MSSQLDB, 'pyodbc'), (POSTGRESDB, 'psycopg2'),
    (SQLITEDB, 'sqlite3')
])
def test_connect_without_driver_raises_exception(db_params, driver, monkeypatch):
    """
    Test that exception and helpful message are raised when connecting
    without driver installed
    """
    # Arrange - simulate missing driver module.
    # See https://stackoverflow.com/a/2481588/3508733
    real_import = builtins.__import__

    def raise_import_error(name, globals, locals, fromlist, level):
        if name == driver:
            raise ImportError()
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, '__import__', raise_import_error)
    monkeypatch.setenv("PASSWORD_VARIABLE", "blahblahblah")
    DB_HELPER_FACTORY.from_dbtype.cache_clear()

    # Act and assert
    with pytest.warns(UserWarning), pytest.raises(ETLHelperConnectionError) as excinfo:
        db_params.connect('PASSWORD_VARIABLE')

    # Confirm error message includes driver details
    error_message = excinfo.value.args[0]
    assert "Could not import" in error_message
    assert driver in error_message


@pytest.mark.parametrize('db_params', [ORACLEDB, MSSQLDB, POSTGRESDB])
def test_connect_without_password_variable_raises_exception(db_params):
    with pytest.raises(ETLHelperConnectionError,
                       match=r"Name of password environment variable .* is required"):
        db_params.connect(None)


@pytest.mark.parametrize('db_params', [ORACLEDB, MSSQLDB, POSTGRESDB])
def test_connect_with_unset_password_variable_raises_exception(db_params):
    with pytest.raises(ETLHelperConnectionError,
                       match=r"Password environment variable .* is not set"):
        db_params.connect("This environment variable is not set")
