"""Integration tests for Oracle database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
import os
from collections import namedtuple
from textwrap import dedent

import oracledb
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
from etlhelper.exceptions import (
    ETLHelperConnectionError,
    ETLHelperInsertError,
    ETLHelperQueryError
)

# Skip these tests if database is unreachable
ORADB = DbParams.from_environment(prefix='TEST_ORACLE_')
if not ORADB.is_reachable():
    pytest.skip('Oracle test database is unreachable', allow_module_level=True)


# -- Tests here --

def test_connect():
    conn = connect(ORADB, 'TEST_ORACLE_PASSWORD')
    assert isinstance(conn, oracledb.Connection)


def test_connect_wrong_password(monkeypatch):
    monkeypatch.setitem(os.environ, 'TEST_ORACLE_PASSWORD', 'bad_password')
    with pytest.raises(ETLHelperConnectionError):
        connect(ORADB, 'TEST_ORACLE_PASSWORD')


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
    result = fetchall(sql, testdb_conn)

    # Fix result date and datetime strings to native classes
    fixed_dates = []
    for row in result:
        fixed_dates.append((
            *row[:4],
            row.DAY.date(),
            row.DATE_TIME
        ))

    assert fixed_dates == test_table_data


def test_copy_table_rows_happy_path(test_tables, testdb_conn, test_table_data):
    # Arrange and act
    copy_table_rows('src', testdb_conn, testdb_conn, target='dest')

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)

    # Fix result date and datetime strings to native classes
    fixed_dates = []
    for row in result:
        fixed_dates.append((
            *row[:4],
            row.DAY.date(),
            row.DATE_TIME
        ))

    assert fixed_dates == test_table_data


def test_copy_table_rows_on_error(test_tables, testdb_conn, test_table_data):
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

    # Check that first row was caught as error, noting that Oracle
    # changes the case of column names
    row, exception = errors[0]
    assert row.ID == 1
    assert "unique" in str(exception).lower()

    # Check that other rows were inserted correctly
    # Fix result date and datetime strings to native classes
    fixed_dates = []
    for row in result[1:]:
        fixed_dates.append((
            *row[:4],
            row.DAY.date(),
            row.DATE_TIME
        ))

    assert fixed_dates == test_table_data[1:]


def test_fetchall_with_parameters(test_tables, testdb_conn,
                                  test_table_data):
    # parameters=None is tested by default in other tests

    # Bind by index
    sql = "SELECT * FROM src where ID = :1"
    result = fetchall(sql, testdb_conn, parameters=(1,))
    assert len(result) == 1
    assert result[0].ID == 1


def test_copy_rows_bad_param_style(test_tables, testdb_conn, test_table_data):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = BAD_PARAM_STYLE_SQL.format(tablename='dest')
    with pytest.raises(ETLHelperInsertError):
        copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn)


def test_load_named_tuples(testdb_conn, test_tables, test_table_data):
    # Arrange
    # Convert to plain tuples as ORACLE makes column names upper case
    expected = [tuple(row) for row in test_table_data]

    # Act
    load('dest', testdb_conn, test_table_data)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)

    # Fix result date and datetime strings to native classes
    fixed_dates = []
    for row in result:
        fixed_dates.append((
            *row[:4],
            row.DAY.date(),
            row.DATE_TIME
        ))

    assert fixed_dates == expected


def test_load_dicts(testdb_conn, test_tables, test_table_data):
    # Arrange
    # Convert to plain tuples as ORACLE makes column names upper case
    expected = [tuple(row) for row in test_table_data]

    # Arrange
    data_as_dicts = [row._asdict() for row in test_table_data]

    # Act
    load('dest', testdb_conn, data_as_dicts)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, testdb_conn)

    # Fix result date and datetime strings to native classes
    fixed_dates = []
    for row in result:
        fixed_dates.append((
            *row[:4],
            row.DAY.date(),
            row.DATE_TIME
        ))

    assert fixed_dates == expected


def test_generate_insert_sql_tuple(testdb_conn):
    # Act
    data = (1, 'one')
    with pytest.raises(ETLHelperInsertError,
                       match="Row is not a dictionary or namedtuple"):
        generate_insert_sql('my_table', data, testdb_conn)


def test_generate_insert_sql_named_tuple(testdb_conn):
    # Arrange
    TwoColumnRow = namedtuple('TwoColumnRow', ('id', 'data'))
    data = TwoColumnRow(1, 'one')
    expected = 'INSERT INTO my_table (id, data) VALUES (:1, :2)'

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
        Column(name='ID', type='NUMBER', not_null=0, has_default=0),
        Column(name='VALUE', type='NUMBER', not_null=1, has_default=0),
        Column(name='SIMPLE_TEXT', type='VARCHAR2', not_null=0, has_default=1),
        Column(name='UTF8_TEXT', type='VARCHAR2', not_null=0, has_default=0),
        Column(name='DAY', type='DATE', not_null=0, has_default=0),
        Column(name='DATE_TIME', type='DATE', not_null=0, has_default=0)
    ]

    # Act
    columns = table_info('src', testdb_conn)

    # Assert
    assert columns == expected


def test_table_info_with_schema_no_duplicates(testdb_conn, test_tables):
    # Arrange
    expected = [
        Column(name='ID', type='NUMBER', not_null=0, has_default=0),
        Column(name='VALUE', type='NUMBER', not_null=1, has_default=0),
        Column(name='SIMPLE_TEXT', type='VARCHAR2', not_null=0, has_default=1),
        Column(name='UTF8_TEXT', type='VARCHAR2', not_null=0, has_default=0),
        Column(name='DAY', type='DATE', not_null=0, has_default=0),
        Column(name='DATE_TIME', type='DATE', not_null=0, has_default=0)
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
      (:1, :2, :3, :4, :5, :6)
      """).strip()

BAD_PARAM_STYLE_SQL = dedent("""
    INSERT INTO {tablename} (id, value, simple_text, utf8_text,
      day, date_time)
    VALUES
      (?, ?, ?, ?, ?, ?)
      """).strip()


@pytest.fixture(scope='function')
def testdb_conn():
    """Get connection to test Oracle database."""
    with connect(ORADB, 'TEST_ORACLE_PASSWORD', encoding="UTF-8",
                 nencoding="UTF-8") as conn:
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
            id NUMBER UNIQUE,
            value NUMBER not null,
            simple_text VARCHAR2(100) default 'default',
            utf8_text VARCHAR2(100),
            day DATE,
            date_time DATE
          )
          """).strip()
    drop_dest_sql = drop_src_sql.replace('src', 'dest')
    create_dest_sql = create_src_sql.replace('src', 'dest')

    # Create table and populate with test data
    with testdb_conn.cursor() as cursor:
        # src table
        try:
            cursor.execute(drop_src_sql)
        except oracledb.DatabaseError:
            pass
        cursor.execute(create_src_sql)
        cursor.executemany(INSERT_SQL.format(tablename='src'),
                           test_table_data)
        # dest table
        try:
            cursor.execute(drop_dest_sql)
        except oracledb.DatabaseError:
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
