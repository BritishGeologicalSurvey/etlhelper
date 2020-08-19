"""Integration tests for SQLite database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
import datetime as dt
import os
import sqlite3
import sys
from textwrap import dedent

import pytest

from etlhelper import connect, get_rows, copy_rows, execute, DbParams
from etlhelper.exceptions import (
    ETLHelperConnectionError,
    ETLHelperQueryError,
    ETLHelperInsertError
)


# -- Tests here --

def test_connect(sqlitedb):
    conn = connect(sqlitedb)
    assert isinstance(conn, sqlite3.Connection)
    assert os.path.isfile(sqlitedb.filename)


@pytest.mark.skipif(sys.platform != 'linux', reason='Requires Linux OS')
def test_bad_connect(tmpdir):
    # Attemping to create file in non-existent directory should fail
    try:
        db_params = DbParams(dbtype='SQLITE', filename='/does/not/exist')
        with pytest.raises(ETLHelperConnectionError):
            connect(db_params)
    finally:
        # Restore permissions prior to cleanup
        os.chmod(tmpdir, 0o666)


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


def test_copy_rows_happy_path(test_tables, testdb_conn, test_table_data):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_conn)

    # Fix result date and datetime strings to native classes
    fixed = []
    for row in result:
        fixed.append((
            *row[:4],
            dt.datetime.strptime(row.day, '%Y-%m-%d').date(),
            dt.datetime.strptime(row.date_time, '%Y-%m-%d %H:%M:%S')
        ))

    assert fixed == test_table_data


def test_copy_rows_bad_param_style(test_tables, testdb_conn, test_table_data):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = BAD_PARAM_STYLE_SQL.format(tablename='dest')
    with pytest.raises(ETLHelperInsertError):
        copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn)


def test_get_rows_with_parameters(pgtestdb_test_tables, pgtestdb_conn,
                                  test_table_data):
    # parameters=None is tested by default in other tests

    # Bind by index
    sql = "SELECT * FROM src where ID = %s"
    result = get_rows(sql, pgtestdb_conn, parameters=(1,))
    assert result == [test_table_data[0]]


# -- Fixtures here --

INSERT_SQL = dedent("""
    INSERT INTO {tablename} (id, value, simple_text, utf8_text,
      day, date_time)
    VALUES
      (?, ?, ?, ?, ?, ?)
      """).strip()


BAD_PARAM_STYLE_SQL = dedent("""
    INSERT INTO {tablename} (id, value, simple_text, utf8_text,
      day, date_time)
    VALUES
      (%1, %2, %3, %4, %5, %6)
      """).strip()


@pytest.fixture(scope='function')
def sqlitedb(tmp_path):
    """Get DbParams for temporary SQLite database."""
    filename = f'{tmp_path.absolute()}.db'
    yield DbParams(dbtype='SQLITE', filename=filename)


@pytest.fixture(scope='function')
def testdb_conn(sqlitedb):
    """Get connection to test SQLite database."""
    with connect(sqlitedb) as conn:
        yield conn


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
            id INTEGER PRIMARY KEY,
            value float,
            simple_text text,
            utf8_text text,
            day date,
            date_time datetime
          )
          """).strip()
    drop_dest_sql = drop_src_sql.replace('src', 'dest')
    create_dest_sql = create_src_sql.replace('src', 'dest')

    # Create table and populate with test data
    cursor = testdb_conn.cursor()
    # src table
    try:
        cursor.execute(drop_src_sql)
    except sqlite3.OperationalError:
        pass
    cursor.execute(create_src_sql)
    cursor.executemany(INSERT_SQL.format(tablename='src'),
                       test_table_data)
    # dest table
    try:
        cursor.execute(drop_dest_sql)
    except sqlite3.OperationalError:
        # Error if table doesn't exist
        pass
    cursor.execute(create_dest_sql)

    testdb_conn.commit()

    # Return control to calling function until end of test
    yield

    # Tear down the table after test completes
    cursor.execute(drop_src_sql)
    cursor.execute(drop_dest_sql)
    testdb_conn.commit()
