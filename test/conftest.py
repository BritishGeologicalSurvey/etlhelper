"""
Fixtures for pytest.  Functions defined here can be passed as arguments to
pytest tests.  scope parameter describes how often they are recreated e.g.
once per module.
"""
import logging
import os
import socket
import datetime as dt
from collections import namedtuple
from pathlib import Path
from textwrap import dedent
from typing import Any
from zipfile import ZipFile

import pytest
from psycopg2.extras import execute_batch

from etlhelper import (
    connect,
    log_to_console,
    DbParams,
)

PGTESTDB = DbParams(
    dbtype='PG',
    host='localhost',
    port=5432,
    dbname='etlhelper',
    user='etlhelper_user')


@pytest.fixture(scope="function")
def logger() -> logging.Logger:
    """
    Return an enabled etlhelper logger for tests.
    The logger handler is set to NullHandler afterwards.
    """
    log_to_console()
    logger = logging.getLogger("etlhelper")
    yield logger
    logger.handlers.clear()


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
def pgtestdb_test_tables(test_table_data_namedtuple, pgtestdb_conn, pgtestdb_insert_sql):
    """
    Create a table and fill with test data.  Teardown after the yield drops it
    again.
    """
    drop_src_sql = "DROP TABLE IF EXISTS src;"
    create_src_sql = dedent("""
          CREATE TABLE src
            (
              id integer primary key,
              value double precision not null,
              simple_text text default 'default',
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
        execute_batch(cursor, pgtestdb_insert_sql, test_table_data_namedtuple)
    pgtestdb_conn.commit()

    # Return control to calling function until end of test
    yield

    # Tear down the table after test completes
    with pgtestdb_conn.cursor() as cursor:
        cursor.execute(drop_src_sql)
        cursor.execute(drop_dest_sql)
    pgtestdb_conn.commit()


@pytest.fixture(scope='module')
def test_table_data_dict() -> list[dict[str, Any]]:
    """
    Return list of dictionaries of test data
    """
    data = [
        {"id": 1, "value": 1.234, "simple_text": "text", "utf8_text": 'Öæ°\nz',
         "day": dt.date(2018, 12, 7), "date_time": dt.datetime(2018, 12, 7, 13, 1, 59)},
        {"id": 2, "value": 2.234, "simple_text": "text", "utf8_text": 'Öæ°\nz',
         "day": dt.date(2018, 12, 8), "date_time": dt.datetime(2018, 12, 8, 13, 1, 59)},
        {"id": 3, "value": 2.234, "simple_text": "text", "utf8_text": 'Öæ°\nz',
         "day": dt.date(2018, 12, 9), "date_time": dt.datetime(2018, 12, 9, 13, 1, 59)},
    ]
    return data


@pytest.fixture(scope='module')
def test_table_data_namedtuple():
    """
    Return list of tuples of test data
    """
    Row = namedtuple('Row',
                     'id, value, simple_text, utf8_text, day, date_time')
    data = [
        Row(1, 1.234, 'text', 'Öæ°\nz', dt.date(2018, 12, 7),
            dt.datetime(2018, 12, 7, 13, 1, 59)),
        Row(2, 2.234, 'text', 'Öæ°\nz', dt.date(2018, 12, 8),
            dt.datetime(2018, 12, 8, 13, 1, 59)),
        Row(3, 2.234, 'text', 'Öæ°\nz', dt.date(2018, 12, 9),
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


@pytest.fixture()
def dummy_zipfile(tmp_path):
    """
    Return a mocked up zip file for testing setup_oracle_client.py
    """
    names = ['instantclient_19_6/adrci', 'instantclient_19_6/BASIC_LICENSE',
             'instantclient_19_6/BASIC_README', 'instantclient_19_6/genezi',
             'instantclient_19_6/libclntshcore.so.19.1', 'instantclient_19_6/libclntsh.so',
             'instantclient_19_6/libclntsh.so.10.1', 'instantclient_19_6/libclntsh.so.11.1',
             'instantclient_19_6/libclntsh.so.12.1', 'instantclient_19_6/libclntsh.so.18.1',
             'instantclient_19_6/libclntsh.so.19.1', 'instantclient_19_6/libipc1.so',
             'instantclient_19_6/libmql1.so', 'instantclient_19_6/libnnz19.so',
             'instantclient_19_6/libocci.so', 'instantclient_19_6/libocci.so.10.1',
             'instantclient_19_6/libocci.so.11.1', 'instantclient_19_6/libocci.so.12.1',
             'instantclient_19_6/libocci.so.18.1', 'instantclient_19_6/libocci.so.19.1',
             'instantclient_19_6/libociei.so', 'instantclient_19_6/libocijdbc19.so',
             'instantclient_19_6/liboramysql19.so', 'instantclient_19_6/network/',
             'instantclient_19_6/ojdbc8.jar', 'instantclient_19_6/ucp.jar',
             'instantclient_19_6/uidrvci', 'instantclient_19_6/xstreams.jar',
             'instantclient_19_6/network/admin/', 'instantclient_19_6/network/admin/README']

    dummy_zipfile = ZipFile(tmp_path / 'instantclient.zip', 'w')

    # Create files in the archive that just contain filename
    for name in names:
        dummy_zipfile.writestr(name, name)
    dummy_zipfile.close()

    return Path(dummy_zipfile.filename)
