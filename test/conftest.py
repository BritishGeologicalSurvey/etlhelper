"""
Fixtures for pytest.  Functions defined here can be passed as arguments to
pytest tests.  scope parameter describes how often they are recreated e.g.
once per module.
"""
import datetime as dt
import os
import socket
from textwrap import dedent

import pytest
from psycopg2.extras import execute_batch

from etlhelper import connect, DbParams

PGTESTDB = DbParams(
    dbtype='PG',
    host='localhost',
    port=5432,
    dbname='etlhelper',
    user='etlhelper_user')


@pytest.fixture(scope='module')
def pgtestdb_insert_sql():
    """Return SQL command used to populate test database."""
    insert_sql = dedent("""
          INSERT INTO src (id, value, simple_text, utf8_text,
            day, date_time)
          VALUES
            (%s, %s, %s, %s, %s, %s)
            ;""").strip()
    return insert_sql


@pytest.fixture(scope='function')
def pgtestdb_test_tables(test_table_data, pgtestdb_conn, pgtestdb_insert_sql):
    """
    Create a table and fill with test data.  Teardown after the yield drops it
    again.
    """
    drop_src_sql = "DROP TABLE IF EXISTS src;"
    create_src_sql = dedent("""
          CREATE TABLE src
            (
              id integer,
              value double precision,
              simple_text text,
              utf8_text text,
              day date,
              date_time timestamp without time zone
            )
            ;""").strip()
    drop_dest_sql = drop_src_sql.replace('src', 'dest')
    create_dest_sql = create_src_sql.replace('src', 'dest')

    # Create table and populate with test data
    with pgtestdb_conn.cursor() as cursor:
        cursor.execute(drop_src_sql)
        cursor.execute(drop_dest_sql)
        cursor.execute(create_src_sql)
        cursor.execute(create_dest_sql)
        execute_batch(cursor, pgtestdb_insert_sql, test_table_data)
    pgtestdb_conn.commit()

    # Return control to calling function until end of test
    yield

    # Tear down the table after test completes
    with pgtestdb_conn.cursor() as cursor:
        cursor.execute(drop_src_sql)
        cursor.execute(drop_dest_sql)
    pgtestdb_conn.commit()


@pytest.fixture(scope='module')
def test_table_data():
    """
    Return list of tuples of test data
    """
    data = [
        (1, 1.234, 'text', 'Öæ°\nz', dt.date(2018, 12, 7),
         dt.datetime(2018, 12, 7, 13, 1, 59)),
        (2, 2.234, 'text', 'Öæ°\nz', dt.date(2018, 12, 8),
         dt.datetime(2018, 12, 8, 13, 1, 59)),
        (3, 2.234, 'text', 'Öæ°\nz', dt.date(2018, 12, 9),
         dt.datetime(2018, 12, 9, 13, 1, 59)),
    ]
    return data


@pytest.fixture(scope='function')
def pgtestdb_conn(pgtestdb_params):
    """Get connection to test PostGIS database."""
    with connect(pgtestdb_params, 'TEST_PG_PASSWORD') as conn:
        yield conn


@pytest.fixture(scope='module')
def pgtestdb_params():
    """
    Create DbParams object for test PostGIS database. Override hostname from
    environment variable to allow CI pipeline to use its own database.
    """
    pg_test_db = PGTESTDB.copy()

    # Override host and port if defined in environment variable
    host = os.getenv('TEST_PG_HOST', None)
    if host:
        pg_test_db.update(host=host)

    port = os.getenv('TEST_PG_PORT', None)
    if port:
        pg_test_db.update(port=port)

    return pg_test_db


def db_is_unreachable(host, port):
    """
    Attempt to connect to generic host, port combination to check network.
    :param host:
    :param port:
    :return: boolean
    """
    s = socket.socket()
    try:
        # Connection succeeds
        s.connect((host, int(port)))
        return False
    except OSError:
        # Failed to connect
        return True
    finally:
        s.close()
