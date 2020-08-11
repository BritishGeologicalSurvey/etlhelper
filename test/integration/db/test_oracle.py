"""Integration tests for Oracle database.
These currently run against internal BGS instance.
"""
# pylint: disable=unused-argument, missing-docstring
from datetime import datetime, date
import os
from textwrap import dedent

import cx_Oracle
import pytest

from etlhelper import connect, get_rows, copy_rows, DbParams
from etlhelper.exceptions import ETLHelperConnectionError
from test.conftest import db_is_unreachable

# Skip these tests if database is unreachable
ORADB = DbParams.from_environment(prefix='TEST_ORACLE_')
if db_is_unreachable(ORADB.host, ORADB.port):
    pytest.skip('Oracle test database is unreachable', allow_module_level=True)


# -- Tests here --

def test_connect():
    conn = connect(ORADB, 'TEST_ORACLE_PASSWORD')
    assert isinstance(conn, cx_Oracle.Connection)


def test_connect_wrong_password(monkeypatch):
    monkeypatch.setitem(os.environ, 'TEST_ORACLE_PASSWORD', 'bad_password')
    with pytest.raises(ETLHelperConnectionError):
        connect(ORADB, 'TEST_ORACLE_PASSWORD')


def test_copy_rows_happy_path(test_tables, testdb_conn, test_table_data):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = INSERT_SQL.format(tablename='dest')
    copy_rows(select_sql, testdb_conn, insert_sql, testdb_conn)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, testdb_conn)

    # Oracle returns date object as datetime, convert test data to compare
    for i, row in enumerate(test_table_data):
        row_with_datetimes = [datetime.combine(x, datetime.min.time())
                              if isinstance(x, date) and not isinstance(x, datetime)
                              else x
                              for x in row]
        assert result[i] == tuple(row_with_datetimes)


# -- Fixtures here --

INSERT_SQL = dedent("""
    INSERT INTO {tablename} (id, value, simple_text, utf8_text,
      day, date_time)
    VALUES
      (:1, :2, :3, :4, :5, :6)
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
            id NUMBER,
            value NUMBER,
            simple_text VARCHAR2(100),
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
        except cx_Oracle.DatabaseError:
            pass
        cursor.execute(create_src_sql)
        cursor.executemany(INSERT_SQL.format(tablename='src'),
                           test_table_data)
        # dest table
        try:
            cursor.execute(drop_dest_sql)
        except cx_Oracle.DatabaseError:
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
