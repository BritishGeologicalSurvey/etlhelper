"""Tests for utils functions."""
import pytest

from etlhelper.exceptions import ETLHelperQueryError
from etlhelper.utils import (
    Column,
    table_info,
)

# pylint: disable=unused-argument, missing-docstring


def test_table_info_no_schema_no_duplicates(pgtestdb_conn, pgtestdb_test_tables):
    # Arrange
    expected = [
        Column('id', 'integer', not_null=1, has_default=0),
        Column('value', 'double precision', not_null=1, has_default=0),
        Column('simple_text', 'text', not_null=0, has_default=1),
        Column('utf8_text', 'text', not_null=0, has_default=0),
        Column('day', 'date', not_null=0, has_default=0),
        Column('date_time', 'timestamp without time zone', not_null=0, has_default=0)
    ]

    # Act
    columns = table_info('src', pgtestdb_conn)

    # Assert
    assert columns == expected


def test_table_info_with_schema_no_duplicates(pgtestdb_conn, pgtestdb_test_tables):
    # Arrange
    expected = [
        Column('id', 'integer', not_null=1, has_default=0),
        Column('value', 'double precision', not_null=1, has_default=0),
        Column('simple_text', 'text', not_null=0, has_default=1),
        Column('utf8_text', 'text', not_null=0, has_default=0),
        Column('day', 'date', not_null=0, has_default=0),
        Column('date_time', 'timestamp without time zone', not_null=0, has_default=0)
    ]

    # Act
    columns = table_info('src', pgtestdb_conn, schema='public')

    # Assert
    assert columns == expected


def test_table_info_no_schema_with_duplicates(pgtestdb_conn, duplicate_schema):
    # Arrange, act and assert
    with pytest.raises(ETLHelperQueryError, match=r'Table name src is not unique'):
        table_info('src', pgtestdb_conn)


def test_table_info_with_schema_with_duplicates(pgtestdb_conn, duplicate_schema):
    # Arrange
    expected = [
        Column('id', 'integer', not_null=1, has_default=0),
        Column('value', 'double precision', not_null=1, has_default=0),
        Column('simple_text', 'text', not_null=0, has_default=1),
        Column('utf8_text', 'text', not_null=0, has_default=0),
        Column('day', 'date', not_null=0, has_default=0),
        Column('date_time', 'timestamp without time zone', not_null=0, has_default=0)
    ]

    # Act
    columns = table_info('src', pgtestdb_conn, schema='public')

    # Assert
    assert columns == expected


def test_table_info_bad_table_name_no_schema(pgtestdb_conn, pgtestdb_test_tables):
    # Arrange, act and assert
    with pytest.raises(ETLHelperQueryError, match=r"Table name 'bad_table' not found."):
        table_info('bad_table', pgtestdb_conn)


def test_table_info_bad_table_name_with_schema(pgtestdb_conn, pgtestdb_test_tables):
    # Arrange, act and assert
    with pytest.raises(ETLHelperQueryError, match=r"Table name 'public.bad_table' not found."):
        table_info('bad_table', pgtestdb_conn, schema='public')


# Fixtures here

@pytest.fixture(scope='function')
def duplicate_schema(pgtestdb_conn, pgtestdb_test_tables):
    # Set up
    with pgtestdb_conn.cursor() as cursor:
        # Create a duplicate of the test tables in a new schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS duplicate", pgtestdb_conn)
        cursor.execute("SELECT * INTO duplicate.src FROM src", pgtestdb_conn)
    pgtestdb_conn.commit()

    # Return control to run test
    yield

    # Tear down
    with pgtestdb_conn.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS duplicate CASCADE", pgtestdb_conn)
    pgtestdb_conn.commit()
