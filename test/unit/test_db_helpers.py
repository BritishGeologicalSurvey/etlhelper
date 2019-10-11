"""Unit tests for db_helpers module."""
from unittest.mock import Mock
import pytest
import sqlite3

import cx_Oracle
import pyodbc
import psycopg2

from etlhelper import DbParams
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.db_helpers import (
    OracleDbHelper,
    SqlServerDbHelper,
    PostgresDbHelper,
    SQLiteDbHelper
)

# pylint: disable=missing-docstring

ORACLEDB = DbParams(dbtype='ORACLE', host='server', port='1521',
                    dbname='testdb', username='testuser')

MSSQLDB = DbParams(dbtype='MSSQL', host='server', port='1521', dbname='testdb',
                   username='testuser', odbc_driver='test driver')

POSTGRESDB = DbParams(dbtype='PG', host='server', port='1521', dbname='testdb',
                      username='testuser', odbc_driver='test driver')

SQLITEDB = DbParams(dbtype='SQLITE', filename='/myfile.db')


def test_oracle_sql_exceptions():
    helper = OracleDbHelper()
    assert helper.sql_exceptions == (cx_Oracle.DatabaseError)


def test_oracle_connect_exceptions():
    helper = OracleDbHelper()
    assert helper.connect_exceptions == (cx_Oracle.DatabaseError)


@pytest.mark.parametrize('db_params, driver, expected', [
    (ORACLEDB, cx_Oracle, 'testuser/mypassword@server:1521/testdb'),
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
