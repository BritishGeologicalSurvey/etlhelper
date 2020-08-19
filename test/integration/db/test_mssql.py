"""Integration tests for MS SQL Server database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
import os
from textwrap import dedent

import pyodbc
import pytest

from etlhelper import connect, get_rows, copy_rows, execute, DbParams
from etlhelper.exceptions import ETLHelperConnectionError, ETLHelperQueryError

# Skip these tests if database is unreachable
MSSQLDB = DbParams.from_environment(prefix='TEST_MSSQL_')
if not MSSQLDB.is_reachable():
    pytest.skip('MSSQL test database is unreachable', allow_module_level=True)


# -- Tests here --

def test_connect():
    conn = connect(MSSQLDB, 'TEST_MSSQL_PASSWORD')
    assert isinstance(conn, pyodbc.Connection)


def test_connect_wrong_password(monkeypatch):
    monkeypatch.setitem(os.environ, 'TEST_MSSQL_PASSWORD', 'bad_password')
    with pytest.raises(ETLHelperConnectionError):
        connect(MSSQLDB, 'TEST_MSSQL_PASSWORD')


def test_bad_select(testdb_conn):
    select_sql = "SELECT * FROM bad_table"
    with pytest.raises(ETLHelperQueryError):
        execute(select_sql, testdb_conn)


def test_bad_insert(testdb_conn):
    insert_sql = "INSERT INTO bad_table (id) VALUES (1)"
    with pytest.raises(ETLHelperQueryError):
        execute(insert_sql, testdb_conn)


def test_bad_constraint(test_tables, testdb_conn):
    # src already has a row with id=1
    insert_sql = "INSERT INTO src (id) VALUES (1)"
    with pytest.raises(ETLHelperQueryError):
        execute(insert_sql, testdb_conn)

def test_copy_rows_happy_path(test_tables, testdb_conn, testdb_conn2,
                              test_table_data):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM testschema.src"
    insert_sql = INSERT_SQL.format(tablename='testschema.dest')
    copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn2)

    # Assert
    sql = "SELECT * FROM testschema.dest"
    result = iter_rows(sql, testdb_conn)
    assert list(result) == test_table_data


# -- Fixtures here --

INSERT_SQL = dedent("""
    INSERT INTO {tablename} (id, value, simple_text, utf8_text,
      day, date_time)
    VALUES
      (?, ?, ?, ?, ?, ?)
      ;""").strip()


@pytest.fixture(scope='function')
def testdb_conn():
    """Get connection to test MS SQL database."""
    with connect(MSSQLDB, 'TEST_MSSQL_PASSWORD') as conn:
        return conn


@pytest.fixture(scope='function')
def testdb_conn2():
    """Get connection to test MS SQL database."""
    with connect(MSSQLDB, 'TEST_MSSQL_PASSWORD') as conn:
        return conn


@pytest.fixture('function')
def test_tables(test_table_data, testdb_conn):
    """
    Create a table and fill with test data.  Teardown after the yield drops it
    again.
    """
    # Define SQL queries
    drop_src_sql = dedent("""
        IF OBJECT_ID('testschema.src') IS NOT NULL
          DROP TABLE testschema.src
          ;""").strip()
    create_src_sql = dedent("""
        CREATE TABLE testschema.src
          (
            id integer,
            value double precision,
            simple_text text,
            utf8_text text,
            day date,
            date_time datetime2(6)
          )
          ;""").strip()
    drop_dest_sql = drop_src_sql.replace('testschema.src', 'testschema.dest')
    create_dest_sql = create_src_sql.replace('testschema.src', 'testschema.dest')

    # Create table and populate with test data
    with testdb_conn.cursor() as cursor:
        # src table
        cursor.execute(drop_src_sql)
        cursor.execute(create_src_sql)
        cursor.executemany(INSERT_SQL.format(tablename='testschema.src'),
                           test_table_data)
        # dest table
        cursor.execute(drop_dest_sql)
        cursor.execute(create_dest_sql)
    testdb_conn.commit()

    # Return control to calling function until end of test
    yield

    # Tear down the table after test completes
    with testdb_conn.cursor() as cursor:
        cursor.execute(drop_src_sql)
        cursor.execute(drop_dest_sql)
    testdb_conn.commit()
