"""Tests for etl logging.  These are run against PostgreSQL."""
# pylint: disable=unused-argument, missing-docstring
import logging
import pytest
import re

from etlhelper import (
    copy_rows,
    execute,
)
from etlhelper.row_factories import namedtuple_row_factory

NO_OUTPUT = []
INFO = [
    'Executing many (chunk_size=1)',
    'Fetching rows (chunk_size=1)',
    '1 rows processed (0 failed)',
    '2 rows processed (0 failed)',
    '3 rows processed (0 failed)',
    '3 rows returned',
    '3 rows processed in total']
INFO_AND_DEBUG = [
    'Executing many (chunk_size=1)',
    'Executing:\n'
    '\n'
    'INSERT INTO dest (id, value, simple_text, utf8_text,\n'
    '  day, date_time)\n'
    'VALUES\n'
    '  (%s, %s, %s, %s, %s, %s)\n'
    '  ;\n'
    '\n'
    'against:\n'
    '\n'
    "<connection object at ???; dsn: 'user=etlhelper_user password=xxx "
    "dbname=etlhelper host=??? port=???', closed: 0>",
    'Fetching rows (chunk_size=1)',
    'Fetching:\n'
    '\n'
    'SELECT * FROM src\n'
    '\n'
    'with parameters:\n'
    '\n'
    '()\n'
    '\n'
    'against:\n'
    '\n'
    "<connection object at ???; dsn: 'user=etlhelper_user password=xxx "
    "dbname=etlhelper host=??? port=???', closed: 0>",
    "First row: Row(id=1, value=1.234, simple_text='text', utf8_text='Öæ°\\nz', "
    'day=datetime.date(2018, 12, 7), date_time=datetime.datetime(2018, 12, 7, 13, '
    '1, 59))',
    '1 rows processed (0 failed)',
    '2 rows processed (0 failed)',
    '3 rows processed (0 failed)',
    '3 rows returned',
    '3 rows processed in total']


@pytest.mark.parametrize('level, expected', [
    (logging.DEBUG, INFO_AND_DEBUG),
    (logging.INFO, INFO),
    (logging.WARNING, NO_OUTPUT),
])
def test_logging_copy_rows(caplog, level, expected,
                           pgtestdb_conn, pgtestdb_test_tables,
                           pgtestdb_insert_sql, logger):
    # Arrange
    caplog.set_level(level, logger=logger.name)
    select_sql = "SELECT * FROM src"
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')

    # Act
    copy_rows(select_sql, pgtestdb_conn, insert_sql, pgtestdb_conn,
              chunk_size=1, row_factory=namedtuple_row_factory)

    # ID for connection object and hostname vary between tests
    # and test environments
    messages = [re.sub(r'object at .*;', 'object at ???;', m)
                for m in caplog.messages]
    messages = [re.sub(r'host=.*? ', 'host=??? ', m)
                for m in messages]
    messages = [re.sub(r'port=[0-9]{1,5}\'', 'port=???\'', m)
                for m in messages]

    # Assert
    for i, message in enumerate(messages):
        assert message == expected[i]


INFO_EXECUTE = ['Executing query']
INFO_AND_DEBUG_EXECUTE = [
    'Executing query',
    'Executing:\n'
    '\n'
    'SELECT 1 AS result;\n'
    '\n'
    'with parameters:\n'
    '\n'
    '()\n'
    '\n'
    'against:\n'
    '\n'
    "<connection object at ???; dsn: 'user=etlhelper_user password=xxx "
    "dbname=etlhelper host=??? port=???', closed: 0>"]


@pytest.mark.parametrize('level, expected', [
    (logging.DEBUG, INFO_AND_DEBUG_EXECUTE),
    (logging.INFO, INFO_EXECUTE),
    (logging.WARNING, NO_OUTPUT),
])
def test_logging_execute(caplog, level, expected, pgtestdb_conn, logger):
    # Arrange
    caplog.set_level(level, logger=logger.name)
    select_sql = "SELECT 1 AS result;"

    # Act
    execute(select_sql, pgtestdb_conn)
    # ID for connection object and hostname vary between tests
    # and test environments
    messages = [re.sub(r'object at .*;', 'object at ???;', m)
                for m in caplog.messages]
    messages = [re.sub(r'host=.*? ', 'host=??? ', m)
                for m in messages]
    messages = [re.sub(r'port=[0-9]{1,5}\'', 'port=???\'', m)
                for m in messages]

    # Assert
    for i, message in enumerate(messages):
        assert message == expected[i]
