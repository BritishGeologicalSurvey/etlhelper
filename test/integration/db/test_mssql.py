"""Integration tests for MS SQL Server database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
import os
from collections import namedtuple
from textwrap import dedent
from unittest.mock import Mock

import pyodbc
import pytest

from etlhelper import (
    DbParams,
    connect,
    copy_rows,
    copy_table_rows,
    execute,
    executemany,
    fetchall,
    generate_insert_sql,
    load,
)
from etlhelper.utils import (
    table_info,
    Column,
)
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.exceptions import (
    ETLHelperConnectionError,
    ETLHelperInsertError,
    ETLHelperQueryError
)
from etlhelper.row_factories import namedtuple_row_factory

# Skip these tests if database is unreachable
if not os.getenv('TEST_MSSQL_HOST'):
    pytest.skip('MSSQL test database is not defined', allow_module_level=True)

MSSQLDB = DbParams.from_environment(prefix='TEST_MSSQL_')
if not MSSQLDB.is_reachable():
    pytest.skip('MSSQL test database is unreachable', allow_module_level=True)


# -- Tests here --

def test_connect():
    conn = connect(MSSQLDB, 'TEST_MSSQL_PASSWORD',
                   trust_server_certificate=True)
    assert isinstance(conn, pyodbc.Connection)


@pytest.mark.parametrize('trust_server_certificate', [
    True,
    False
])
def test_connect_trust_server_certificate(monkeypatch, trust_server_certificate):
    # Arrange
    helper = DB_HELPER_FACTORY.from_db_params(MSSQLDB)
    mock_connect = Mock()
    monkeypatch.setattr(helper, '_connect_func', mock_connect)

    # Act
    helper.connect(MSSQLDB, 'TEST_MSSQL_PASSWORD',
                   trust_server_certificate=trust_server_certificate)

    # Assert
    conn_str = mock_connect.call_args.args[0]
    trusted = ';TrustServerCertificate=yes' in conn_str
    assert trust_server_certificate is trusted


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
        test_tables, testdb_conn, testdb_conn2, test_table_data_dict):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn2,
              row_factory=namedtuple_row_factory)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)
    assert result == test_table_data_dict


def test_copy_rows_happy_path_deprecated_tables_fast_true(
        test_deprecated_tables, testdb_conn, testdb_conn2, test_table_data_dict):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    with pytest.warns(UserWarning) as record:
        copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn2,
                  row_factory=namedtuple_row_factory)

    # Assert
    assert len(record) == 1
    assert str(record[0].message).startswith(
        "fast_executemany execution failed")

    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)
    assert result == test_table_data_dict


def test_copy_rows_happy_path_fast_false(
        test_tables, testdb_fast_false_conn, testdb_fast_false_conn2, test_table_data_dict):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_fast_false_conn, insert_sql, testdb_fast_false_conn2,
              row_factory=namedtuple_row_factory)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_fast_false_conn)
    assert result == test_table_data_dict


def test_copy_rows_happy_path_deprecated_tables_fast_false(
        test_deprecated_tables, testdb_fast_false_conn,
        testdb_fast_false_conn2, test_table_data_dict):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_fast_false_conn, insert_sql, testdb_fast_false_conn2,
              row_factory=namedtuple_row_factory)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_fast_false_conn)
    assert result == test_table_data_dict


def test_copy_table_rows_happy_path_fast_true(
        test_tables, testdb_conn, testdb_conn2, test_table_data_dict):
    # Note: ODBC driver requires separate connections for source and destination,
    # even if they are the same database.
    # Arrange and act
    copy_table_rows('src', testdb_conn, testdb_conn2, target='dest',
                    row_factory=namedtuple_row_factory)

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
                    on_error=errors.extend, row_factory=namedtuple_row_factory)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)

    # Check that first row was caught as error
    row, exception = errors[0]
    assert row.id == 1
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


def test_copy_rows_bad_param_style(test_tables, testdb_conn):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = BAD_PARAM_STYLE_SQL.format(tablename='dest')
    with pytest.raises(ETLHelperInsertError):
        copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn,
                  row_factory=namedtuple_row_factory)


def test_executemany_dicts_raises_error(testdb_conn, test_tables, test_table_data_dict):
    # Arrange
    # Placeholder doesn't really matter as pydodbc doesn't support
    # named placeholders
    insert_sql = dedent("""
        INSERT INTO dest (id, value, simple_text, utf8_text, day, date_time)
        VALUES (:id, :value, :simple_text, :utf8_text, :day, :date_time)
        ;""").strip()
    expected_message = ("pyodbc driver for MS SQL only supports positional placeholders.  "
                        "Use namedtuple, tuple or list (via row_factory setting for copy_rows).")

    # Act and assert
    # pyodbc doesn't support named parameters.
    with pytest.raises(ETLHelperInsertError) as exc_info:
        executemany(insert_sql, testdb_conn, test_table_data_dict)

    assert str(exc_info.value) == expected_message


def test_load_namedtuples(testdb_conn, test_tables, test_table_data_namedtuple):
    # Act
    load('dest', testdb_conn, test_table_data_namedtuple)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn, row_factory=namedtuple_row_factory)
    assert result == test_table_data_namedtuple


def test_load_dicts(testdb_conn, test_tables, test_table_data_dict):
    # Arrange
    expected_message = ("Database connection (<class 'pyodbc.Connection'>) doesn't support named parameters.  "
                        "Pass data as namedtuples instead.")

    # Act and assert
    # pyodbc doesn't support named parameters.
    with pytest.raises(ETLHelperInsertError) as exc_info:
        load('dest', testdb_conn, test_table_data_dict)

    assert str(exc_info.value) == expected_message


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
    with pytest.raises(ETLHelperInsertError,
                       match="doesn't support named parameters"):
        generate_insert_sql('my_table', data, testdb_conn)


def test_table_info_no_schema_no_duplicates(testdb_conn, test_tables):
    # Arrange
    expected = [
        Column(name='id', type='int', not_null=0, has_default=0),
        Column(name='value', type='float', not_null=1, has_default=0),
        Column(name='simple_text', type='nvarchar', not_null=0, has_default=1),
        Column(name='utf8_text', type='nvarchar', not_null=0, has_default=0),
        Column(name='day', type='date', not_null=0, has_default=0),
        Column(name='date_time', type='datetime2', not_null=0, has_default=0)
    ]

    # Act
    columns = table_info('src', testdb_conn)

    # Assert
    assert columns == expected


def test_table_info_with_schema_no_duplicates(testdb_conn, test_tables):
    # Arrange
    expected = [
        Column(name='id', type='int', not_null=0, has_default=0),
        Column(name='value', type='float', not_null=1, has_default=0),
        Column(name='simple_text', type='nvarchar', not_null=0, has_default=1),
        Column(name='utf8_text', type='nvarchar', not_null=0, has_default=0),
        Column(name='day', type='date', not_null=0, has_default=0),
        Column(name='date_time', type='datetime2', not_null=0, has_default=0)
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
    with connect(MSSQLDB, 'TEST_MSSQL_PASSWORD',
                 trust_server_certificate=True) as conn:
        return conn


@pytest.fixture(scope='function')
def testdb_conn2():
    """Get connection to test MS SQL database."""
    with connect(MSSQLDB, 'TEST_MSSQL_PASSWORD',
                 trust_server_certificate=True) as conn:
        return conn


@pytest.fixture(scope='function')
def testdb_fast_false_conn():
    """Get connection to test MS SQL database."""
    with connect(MSSQLDB, 'TEST_MSSQL_PASSWORD', fast_executemany=False,
                 trust_server_certificate=True) as conn:
        return conn


@pytest.fixture(scope='function')
def testdb_fast_false_conn2():
    """Get connection to test MS SQL database."""
    with connect(MSSQLDB, 'TEST_MSSQL_PASSWORD', fast_executemany=False,
                 trust_server_certificate=True) as conn:
        return conn


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
            id integer unique,
            value double precision not null,
            simple_text nvarchar(max) default 'default',
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
                           test_table_data_namedtuple)
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
def test_deprecated_tables(test_table_data_namedtuple, testdb_conn):
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
                           test_table_data_namedtuple)
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
