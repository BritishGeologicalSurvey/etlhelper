"""Integration tests for SQLite database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
import os
import sqlite3
import sys
from collections import namedtuple
from textwrap import dedent

import pytest

from etlhelper import (
    DbParams,
    connect,
    copy_rows,
    copy_table_rows,
    execute,
    fetchall,
    generate_insert_sql,
    load,
)
from etlhelper.utils import (
    table_info,
    Column,
)
from etlhelper.row_factories import namedtuple_row_factory
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


def test_copy_rows_happy_path(test_tables, testdb_conn, test_table_data_dict):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn, row_factory=namedtuple_row_factory)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)

    assert result == test_table_data_dict


def test_copy_table_rows_happy_path(test_tables, testdb_conn, test_table_data_dict):
    # Arrange and act
    copy_table_rows('src', testdb_conn, testdb_conn, target='dest')

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)

    assert result == test_table_data_dict


def test_copy_table_rows_on_error(test_tables, testdb_conn, test_table_data_dict):
    # Arrange
    duplicate_id_row_sql = """
       INSERT INTO dest (id, value)
       VALUES (
         1,
         1.234
         )""".strip()
    execute(duplicate_id_row_sql, testdb_conn)

    # Act
    errors = []
    copy_table_rows('src', testdb_conn, testdb_conn, target='dest',
                    on_error=errors.extend)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)

    # Check that first row was caught as error
    row, exception = errors[0]
    assert row["id"] == 1
    assert "unique" in str(exception).lower()

    # Check that other rows were inserted correctly
    assert result[1:] == test_table_data_dict[1:]


def test_fetchall_with_parameters(test_tables, testdb_conn):
    # parameters=None is tested by default in other tests

    # Bind by index
    sql = "SELECT * FROM src where ID = ?"
    result = fetchall(sql, testdb_conn, parameters=(1,))
    assert len(result) == 1
    assert result[0]["id"] == 1


def test_copy_rows_bad_param_style(test_tables, testdb_conn, test_table_data_namedtuple):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = BAD_PARAM_STYLE_SQL.format(tablename='dest')
    with pytest.raises(ETLHelperInsertError):
        copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn)


def test_load_namedtuples(testdb_conn, test_tables, test_table_data_namedtuple):
    # Act
    load('dest', testdb_conn, test_table_data_namedtuple)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn, row_factory=namedtuple_row_factory)

    assert result == test_table_data_namedtuple


def test_load_dicts(testdb_conn, test_tables, test_table_data_dict):
    # Act
    load('dest', testdb_conn, test_table_data_dict)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)

    assert result == test_table_data_dict


def test_generate_insert_sql_tuple(testdb_conn):
    # Act
    data = (1, 'one')
    with pytest.raises(ETLHelperInsertError,
                       match="Row is not a dictionary or namedtuple"):
        generate_insert_sql('my_table', data, testdb_conn)


def test_generate_insert_sql_namedtuple(testdb_conn):
    # Arrange
    TwoColumnRow = namedtuple('TwoColumnRow', ('id', 'data'))
    data = TwoColumnRow(1, 'one')
    expected = 'INSERT INTO my_table (id, data) VALUES (?, ?)'

    # Act
    sql = generate_insert_sql('my_table', data, testdb_conn)

    # Assert
    assert sql == expected


def test_generate_insert_sql_dictionary(testdb_conn):
    # Act
    data = {'id': 1, 'data': 'one'}

    expected = 'INSERT INTO my_table (id, data) VALUES (:id, :data)'

    # Act
    sql = generate_insert_sql('my_table', data, testdb_conn)

    # Assert
    assert sql == expected


def test_table_info_no_schema_no_duplicates(testdb_conn, test_tables):
    # Arrange
    expected = [
        Column(name='id', type='integer', not_null=0, has_default=0),
        Column(name='value', type='float', not_null=1, has_default=0),
        Column(name='simple_text', type='text', not_null=0, has_default=1),
        Column(name='utf8_text', type='text', not_null=0, has_default=0),
        Column(name='day', type='date', not_null=0, has_default=0),
        Column(name='date_time', type='timestamp', not_null=0, has_default=0)
    ]

    # Act
    columns = table_info('src', testdb_conn)

    # Assert
    assert columns == expected


def test_table_info_with_schema_no_duplicates(testdb_conn, test_tables):
    # Arrange
    expected = [
        Column(name='id', type='integer', not_null=0, has_default=0),
        Column(name='value', type='float', not_null=1, has_default=0),
        Column(name='simple_text', type='text', not_null=0, has_default=1),
        Column(name='utf8_text', type='text', not_null=0, has_default=0),
        Column(name='day', type='date', not_null=0, has_default=0),
        Column(name='date_time', type='timestamp', not_null=0, has_default=0)
    ]

    # Act
    columns = table_info('src', testdb_conn, schema='etlhelper')

    # Assert
    assert columns == expected


def test_table_info_bad_table_name_no_schema(testdb_conn, test_tables):
    # Arrange, act and assert
    with pytest.raises(ETLHelperQueryError, match=r"Table name 'bad_table' not found."):
        table_info('bad_table', testdb_conn)


def test_table_info_bad_table_name_with_schema(testdb_conn, test_tables):
    # Arrange, act and assert
    with pytest.raises(ETLHelperQueryError, match=r"Table name 'etlhelper.bad_table' not found."):
        table_info('bad_table', testdb_conn, schema='etlhelper')


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
def test_tables(test_table_data_namedtuple, testdb_conn):
    """
    Create a table and fill with test data.  Teardown after the yield drops it
    again.
    """
    # Define SQL queries
    drop_src_sql = "DROP TABLE src"
    create_src_sql = dedent("""
        CREATE TABLE src
          (
            id integer primary key,
            value float not null,
            simple_text text default 'default',
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
                       test_table_data_namedtuple)
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
