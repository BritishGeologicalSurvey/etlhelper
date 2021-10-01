"""Integration tests for SQLite database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
import os
import sqlite3
import sys
from textwrap import dedent

import pytest

from etlhelper import connect, get_rows, copy_rows, execute, DbParams, load
from etlhelper.exceptions import (
    ETLHelperConnectionError,
    ETLHelperInsertError,
    ETLHelperQueryError
)

# -- Tests here --


def test_connect(sqlitedb):
    conn = connect(sqlitedb)
    assert isinstance(conn, sqlite3.Connection)
    assert os.path.isfile(sqlitedb.filename)


@pytest.mark.skipif(sys.platform != 'linux', reason='Requires Linux OS')
def test_bad_connect(tmpdir):
    # Attempting to create file in non-existent directory should fail
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

    # Act
    load('dest', testdb_conn, data_as_dicts)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_conn)

    assert result == test_table_data


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
      (%s, %s, %s, %s, %s, %s)
      """).strip()


@pytest.fixture(scope='function')
def sqlitedb(tmp_path):
    """Get DbParams for temporary SQLite database."""
    filename = f'{tmp_path.absolute()}.db'
    yield DbParams(dbtype='SQLITE', filename=filename)


@pytest.fixture(scope='function')
def testdb_conn(sqlitedb):
    """Get connection to test SQLite database."""
    with connect(sqlitedb, detect_types=sqlite3.PARSE_DECLTYPES) as conn:
        # PARSE_DECLTYPES makes SQLite return TIMESTAMP columns as Python datetimes
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
            date_time timestamp
          )
          """).strip()
    drop_dest_sql = drop_src_sql.replace('src', 'dest')
    create_dest_sql = create_src_sql.replace('src', 'dest')

    # Create table and populate with test data
    # Unlike other databases, SQLite cursors cannot be used as context managers
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
