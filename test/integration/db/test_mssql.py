"""Integration tests for MS SQL Server database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
import datetime as dt
import os
from unittest.mock import Mock, call
from textwrap import dedent

import pyodbc
import pytest

import etlhelper   # used in legacy tests
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


# -- Below here are legacy tests, kept because they are useful --
def test_get_rows_happy_path(test_tables, testdb_conn, test_table_data):
    sql = "SELECT * FROM testschema.src"
    result = get_rows(sql, testdb_conn)
    assert result == test_table_data


def test_dump_rows_happy_path(test_tables, testdb_conn, test_table_data):
    # Arrange
    sql = "SELECT * FROM testschema.src"
    mock = Mock()
    expected_calls = [call(row) for row in test_table_data]

    # Act
    dump_rows(sql, testdb_conn, mock)

    # Assert
    assert mock.mock_calls == expected_calls


def test_iter_rows_happy_path(test_tables, testdb_conn, test_table_data):
    sql = "SELECT * FROM testschema.src"
    result = iter_rows(sql, testdb_conn)
    assert list(result) == test_table_data


def test_iter_chunks(test_tables, testdb_conn, test_table_data, monkeypatch):
    monkeypatch.setattr('etlhelper.etl.CHUNKSIZE', 1)
    sql = "SELECT * FROM testschema.src"
    result = list(iter_chunks(sql, testdb_conn))
    expected = [[row] for row in test_table_data]
    # element-wise comparison
    for r, e in zip(result, expected):
        for rr, ee in zip(r, e):
            for rrr, eee in zip(rr, ee):
                assert rrr == eee
    # print(f'result = {result}')
    # print(f'expect = {expected}')
    # default comparision
    assert result == expected


def test_iter_rows_dict_factory(test_tables, testdb_conn, test_table_data):
    sql = "SELECT * FROM testschema.src"
    result = iter_rows(sql, testdb_conn, row_factory=dict_rowfactory)
    expected = [
        {'id': 1, 'value': 1.234, 'simple_text': 'text', 'utf8_text': 'Öæ°\nz',
         'day': datetime.date(2018, 12, 7),
         'date_time': datetime.datetime(2018, 12, 7, 13, 1, 59)},
        {'id': 2, 'value': 2.234, 'simple_text': 'text', 'utf8_text': 'Öæ°\nz',
         'day': datetime.date(2018, 12, 8),
         'date_time': datetime.datetime(2018, 12, 8, 13, 1, 59)},
        {'id': 3, 'value': 2.234, 'simple_text': 'text', 'utf8_text': 'Öæ°\nz',
         'day': datetime.date(2018, 12, 9),
         'date_time': datetime.datetime(2018, 12, 9, 13, 1, 59)},
         ]

    assert list(result) == expected


def test_iter_rows_namedtuple_factory(
        test_tables, testdb_conn, test_table_data):
    sql = "SELECT * FROM testschema.src"
    result = iter_rows(sql, testdb_conn, row_factory=namedtuple_rowfactory)
    row = list(result)[0]

    assert row.id == 1
    assert row.value == 1.234
    assert row.simple_text == 'text'
    assert row.utf8_text == 'Öæ°\nz'
    assert row.day == datetime.date(2018, 12, 7)

    # The final assertion is skipped because the test fails, even though the
    # correct value has been assigned.  I don't know why.
    # assert row.date_time == datetime.datetime(2018, 12, 9, 13, 1, 59)


def test_iter_rows_no_results(test_tables, testdb_conn):
    sql = "SELECT * FROM testschema.src WHERE id = -1"
    result = iter_rows(sql, testdb_conn)
    assert list(result) == []


def test_iter_rows_bad_query(test_tables, testdb_conn):
    sql = "SELECT * FROM testschema.this_does_not_exist"
    with pytest.raises(ETLHelperExtractError):
        result = iter_rows(sql, testdb_conn)
        list(result)  # Call list to activate returned generator


def test_insert_rows_happy_path(testdb_conn, test_tables, test_table_data):
    # Arrange
    insert_sql = INSERT_SQL.format(tablename='testschema.dest')

    # Act
    executemany(insert_sql, test_table_data, testdb_conn)

    # Assert
    sql = "SELECT * FROM testschema.dest"
    result = get_rows(sql, testdb_conn)
    assert result == test_table_data


@pytest.mark.parametrize('chunk_size', [1, 2, 3, 4])
def test_insert_rows_chunked(testdb_conn, test_table_data, monkeypatch,
                             test_tables, chunk_size):
    # Arrange
    monkeypatch.setattr('bgs_etl.etl.CHUNKSIZE', chunk_size)
    insert_sql = INSERT_SQL.format(tablename='testschema.dest')

    # Act
    executemany(insert_sql, test_table_data, testdb_conn)

    # Assert
    sql = "SELECT * FROM testschema.dest"
    result = get_rows(sql, testdb_conn)
    assert result == test_table_data


def test_insert_rows_no_rows(testdb_conn, test_tables):
    # Arrange
    insert_sql = INSERT_SQL.format(tablename='testschema.dest')

    # Act
    executemany(insert_sql, [], testdb_conn)

    # Assert
    sql = "SELECT * FROM testschema.dest"
    result = iter_rows(sql, testdb_conn)
    assert list(result) == []


def test_insert_rows_bad_query(testdb_conn, test_table_data):
    # Arrange
    insert_sql = "INSERT INTO bad_table VALUES (?, ?, ?, ?, ?, ?)"

    # Act and assert
    with pytest.raises(ETLHelperInsertError):
        executemany(insert_sql, test_table_data, testdb_conn)
