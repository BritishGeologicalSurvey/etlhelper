"""Integration tests for MS SQL Server database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
import os
from textwrap import dedent

import pyodbc
import pytest

from etlhelper import (
    DbParams,
    connect,
    copy_rows,
    copy_table_rows,
    execute,
    get_rows,
    load,
)
from etlhelper.exceptions import (
    ETLHelperConnectionError,
    ETLHelperInsertError,
    ETLHelperQueryError
)

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


def test_copy_rows_happy_path_fast_true(
        test_tables, testdb_conn, testdb_conn2, test_table_data):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn2)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_conn)
    assert result == test_table_data


def test_copy_rows_happy_path_deprecated_tables_fast_true(
        test_deprecated_tables, testdb_conn, testdb_conn2, test_table_data):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    with pytest.warns(UserWarning) as record:
        copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn2)

    # Assert
    assert len(record) == 1
    assert str(record[0].message).startswith(
        "fast_executemany execution failed")

    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_conn)
    assert result == test_table_data


def test_copy_rows_happy_path_fast_false(
        test_tables, testdb_fast_false_conn, testdb_fast_false_conn2, test_table_data):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_fast_false_conn, insert_sql, testdb_fast_false_conn2)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_fast_false_conn)
    assert result == test_table_data


def test_copy_rows_happy_path_deprecated_tables_fast_false(
        test_deprecated_tables, testdb_fast_false_conn, testdb_fast_false_conn2, test_table_data):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_fast_false_conn, insert_sql, testdb_fast_false_conn2)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_fast_false_conn)
    assert result == test_table_data


def test_copy_table_rows_happy_path_fast_true(
        test_tables, testdb_conn, testdb_conn2, test_table_data):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    copy_table_rows('src', testdb_conn, testdb_conn2, target='dest')

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_conn)
    assert result == test_table_data


def test_get_rows_with_parameters(test_tables, testdb_conn,
                                  test_table_data):
    # parameters=None is tested by default in other tests

    # Bind by index
    sql = "SELECT * FROM src where ID = ?"
    result = get_rows(sql, testdb_conn, parameters=(1,))
    assert len(result) == 1
    assert result[0].id == 1


def test_copy_rows_bad_param_style(test_tables, testdb_conn, test_table_data):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = BAD_PARAM_STYLE_SQL.format(tablename='dest')
    with pytest.raises(ETLHelperInsertError):
        copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn)


def test_load_named_tuples(testdb_conn, test_tables, test_table_data):
    # Act
    load('dest', testdb_conn, test_table_data)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_conn)
    assert result == test_table_data


def test_load_dicts(testdb_conn, test_tables, test_table_data):
    # Arrange
    data_as_dicts = [row._asdict() for row in test_table_data]
    expected_message = ("Database connection (<class 'pyodbc.Connection'>) doesn't support named parameters.  "
                        "Pass data as namedtuples instead.")

    # Act and assert
    # pyodbc doesn't support named parameters.
    with pytest.raises(ETLHelperInsertError) as exc_info:
        load('dest', testdb_conn, data_as_dicts)

    assert str(exc_info.value) == expected_message


# -- Fixtures here --

INSERT_SQL = dedent("""
    INSERT INTO {tablename} (id, value, simple_text, utf8_text,
      day, date_time)
    VALUES
      (?, ?, ?, ?, ?, ?)
      ;""").strip()

BAD_PARAM_STYLE_SQL = dedent("""
    INSERT INTO {tablename} (id, value, simple_text, utf8_text,
      day, date_time)
    VALUES
      (%s, %s, %s, %s, %s, %s)
      """).strip()


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


@pytest.fixture(scope='function')
def testdb_fast_false_conn():
    """Get connection to test MS SQL database."""
    with connect(MSSQLDB, 'TEST_MSSQL_PASSWORD', fast_executemany=False) as conn:
        return conn


@pytest.fixture(scope='function')
def testdb_fast_false_conn2():
    """Get connection to test MS SQL database."""
    with connect(MSSQLDB, 'TEST_MSSQL_PASSWORD', fast_executemany=False) as conn:
        return conn


@pytest.fixture(scope='function')
def test_tables(test_table_data, testdb_conn):
    """
    Create a table and fill with test data.  Teardown after the yield drops it
    again.
    """
    # Define SQL queries
    drop_src_sql = "DROP TABLE src"
    create_src_sql = dedent("""
        CREATE TABLE src
          (
            id integer unique,
            value double precision,
            simple_text nvarchar(max),
            utf8_text nvarchar(max),
            day date,
            date_time datetime2(6)
          )
          ;""").strip()
    drop_dest_sql = drop_src_sql.replace('src', 'dest')
    create_dest_sql = create_src_sql.replace('src', 'dest')

    # Create table and populate with test data
    with testdb_conn.cursor() as cursor:
        # src table
        try:
            cursor.execute(drop_src_sql)
        except pyodbc.DatabaseError:
            pass
        cursor.execute(create_src_sql)
        cursor.executemany(INSERT_SQL.format(tablename='src'),
                           test_table_data)
        # dest table
        try:
            cursor.execute(drop_dest_sql)
        except pyodbc.DatabaseError:
            # Error if table doesn't exist
            pass
        cursor.execute(create_dest_sql)
    testdb_conn.commit()

    # Return control to calling function until end of test
    yield

    # Tear down the table after test completes
    with testdb_conn.cursor() as cursor:
        cursor.execute(drop_src_sql)
        cursor.execute(drop_dest_sql)
    testdb_conn.commit()


@pytest.fixture(scope='function')
def test_deprecated_tables(test_table_data, testdb_conn):
    """
    Create a table and fill with test data.  Teardown after the yield drops it
    again.
    """
    # Define SQL queries
    drop_src_sql = "DROP TABLE src"
    create_src_sql = dedent("""
        CREATE TABLE src
          (
            id integer unique,
            value double precision,
            simple_text text,
            utf8_text text,
            day date,
            date_time datetime2(6)
          )
          ;""").strip()
    drop_dest_sql = drop_src_sql.replace('src', 'dest')
    create_dest_sql = create_src_sql.replace('src', 'dest')

    # Create table and populate with test data
    with testdb_conn.cursor() as cursor:
        # src table
        try:
            cursor.execute(drop_src_sql)
        except pyodbc.DatabaseError:
            pass
        cursor.execute(create_src_sql)
        cursor.executemany(INSERT_SQL.format(tablename='src'),
                           test_table_data)
        # dest table
        try:
            cursor.execute(drop_dest_sql)
        except pyodbc.DatabaseError:
            # Error if table doesn't exist
            pass
        cursor.execute(create_dest_sql)
    testdb_conn.commit()

    # Return control to calling function until end of test
    yield

    # Tear down the table after test completes
    with testdb_conn.cursor() as cursor:
        cursor.execute(drop_src_sql)
        cursor.execute(drop_dest_sql)
    testdb_conn.commit()
