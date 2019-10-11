"""Unit tests for db_helpers module."""
from unittest.mock import Mock
import pytest

import cx_Oracle
import pyodbc
import psycopg2

from etlhelper import DbParams
from etlhelper.db_helpers import (OracleDbHelper, SqlServerDbHelper, PostgresDbHelper)

# pylint: disable=missing-docstring


@pytest.fixture()
def params():
    return DbParams(dbtype='ORACLE', odbc_driver='test driver', host='testhost',
                    port=1521, dbname='testdb', user='testuser')


def test_oracle_sql_exceptions():
    helper = OracleDbHelper()
    assert helper.sql_exceptions == (cx_Oracle.DatabaseError)


def test_oracle_connect_exceptions():
    helper = OracleDbHelper()
    assert helper.connect_exceptions == (cx_Oracle.DatabaseError)


# dbapi test connections
def test_oracle_connect(monkeypatch):
    # Arrange
    # TODO: Fix DbParams class to take driver as init input.
    db_params = DbParams(dbtype='ORACLE',
                         host='server', port='1521', dbname='testdb',
                         user='testuser')
    monkeypatch.setenv('DB_PASSWORD', 'mypassword')
    expected_conn_str = 'testuser/mypassword@server:1521/testdb'

    mock_connect = Mock()
    monkeypatch.setattr(cx_Oracle, 'connect', mock_connect)

    # Act
    helper = OracleDbHelper()
    helper.connect(db_params, 'DB_PASSWORD')

    # Assert
    mock_connect.assert_called_with(expected_conn_str)


def test_sqlserver_connect(monkeypatch):
    db_params = DbParams(dbtype='MSSQL',
                         host='server', port='1521', dbname='testdb',
                         user='testuser', odbc_driver='test driver')
    monkeypatch.setenv('DB_PASSWORD', 'mypassword')
    expected_conn_str = ('DRIVER=test driver;SERVER=tcp:server;PORT=1521;'
                         'DATABASE=testdb;UID=testuser;PWD=mypassword')

    mock_connect = Mock()
    monkeypatch.setattr(pyodbc, 'connect', mock_connect)

    # Act
    helper = SqlServerDbHelper()
    helper.connect(db_params, 'DB_PASSWORD')

    # Assert
    mock_connect.assert_called_with(expected_conn_str)


def test_postgres_connect(monkeypatch):
    db_params = DbParams(dbtype='PG',
                         host='server', port='1521', dbname='testdb',
                         user='testuser', odbc_driver='test driver')
    monkeypatch.setenv('DB_PASSWORD', 'mypassword')
    expected_conn_str = 'host=server port=1521 dbname=testdb user=testuser password=mypassword'
    mock_connect = Mock()
    monkeypatch.setattr(psycopg2, 'connect', mock_connect)

    # Act
    helper = PostgresDbHelper()
    helper.connect(db_params, 'DB_PASSWORD')

    # Assert
    mock_connect.assert_called_with(expected_conn_str)


# sqlalchemy test connections
def test_oracle_sqlalchemy_conn_string(monkeypatch):
    db_params = DbParams(dbtype='ORACLE',
                         host='server', port='1521', dbname='testdb',
                         user='testuser')
    monkeypatch.setenv('DB_PASSWORD', 'mypassword')
    helper = OracleDbHelper()
    conn_str = helper.get_sqlalchemy_connection_string(db_params, 'DB_PASSWORD')
    expected_conn_str = ('oracle://testuser:mypassword@server:1521/testdb')

    assert conn_str == expected_conn_str


def test_sqlserver_sqlalchemy_connect(monkeypatch):
    db_params = DbParams(dbtype='MSSQL',
                         host='server', port='1521', dbname='testdb',
                         user='testuser', odbc_driver='test driver')
    monkeypatch.setenv('DB_PASSWORD', 'mypassword')
    helper = SqlServerDbHelper()
    conn_str = helper.get_sqlalchemy_connection_string(db_params, 'DB_PASSWORD')
    expected_conn_str = 'mssql+pyodbc://testuser:mypassword@server:1521/testdb?driver=test+driver'

    assert conn_str == expected_conn_str


def test_postgres_sqlalchemy_connect(monkeypatch):
    db_params = DbParams(dbtype='PG',
                         host='server', port='1521', dbname='testdb',
                         user='testuser')
    monkeypatch.setenv('DB_PASSWORD', 'mypassword')
    helper = PostgresDbHelper()
    conn_str = helper.get_sqlalchemy_connection_string(db_params, 'DB_PASSWORD')
    expected_conn_str = 'postgresql://testuser:mypassword@server:1521/testdb'

    assert conn_str == expected_conn_str
