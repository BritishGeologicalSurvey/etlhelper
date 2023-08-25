"""Test for etl load functions.  Data loading is carried out using
the executemany call.  These are run against PostgreSQL."""
# pylint: disable=unused-argument, missing-docstring
import re
from collections import namedtuple
from unittest.mock import (
    Mock,
    ANY,
    sentinel,
)

import pytest

import etlhelper.etl as etlhelper_etl
from etlhelper import (
    executemany,
    generate_insert_sql,
    fetchall,
    iter_rows,
    load,
)
from etlhelper.row_factories import namedtuple_row_factory
from etlhelper.etl import ETLHelperInsertError
from etlhelper.exceptions import ETLHelperBadIdentifierError


@pytest.mark.parametrize('commit_chunks', [True, False])
def test_insert_rows_happy_path(pgtestdb_conn, pgtestdb_test_tables,
                                pgtestdb_insert_sql, test_table_data_namedtuple,
                                commit_chunks):
    # Parameterized to ensure success with and without commit_chunks
    # Arrange
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')

    # Act
    processed, failed = executemany(insert_sql, pgtestdb_conn, test_table_data_namedtuple,
                                    commit_chunks=commit_chunks)

    # Assert
    assert processed == len(test_table_data_namedtuple)
    assert failed == 0

    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert result == test_table_data_namedtuple


@pytest.mark.parametrize('commit_chunks', [True, False])
def test_insert_rows_on_error(pgtestdb_conn, pgtestdb_test_tables,
                              pgtestdb_insert_sql, test_table_data_namedtuple,
                              commit_chunks):
    # Parameterized to ensure success with and without commit_chunks
    # Arrange
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')
    # Create duplicated rows to data that will fail primary key check
    duplicated_rows = test_table_data_namedtuple * 2

    # Act
    errors = []
    processed, failed = executemany(insert_sql, pgtestdb_conn, duplicated_rows,
                                    on_error=errors.extend, commit_chunks=commit_chunks)

    # Assert
    assert processed == len(duplicated_rows)
    assert failed == len(errors)

    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert result == test_table_data_namedtuple

    # Assert full set of failed rows failing unique constraint
    failed_rows, exceptions = zip(*errors)
    assert set(failed_rows) == set(test_table_data_namedtuple)
    assert all(['unique' in str(e).lower() for e in exceptions])


@pytest.mark.parametrize('chunk_size', [1, 2, 3, 4])
def test_insert_rows_chunked(pgtestdb_conn, pgtestdb_test_tables,
                             pgtestdb_insert_sql, test_table_data_namedtuple, monkeypatch,
                             chunk_size):
    # Arrange
    monkeypatch.setattr('etlhelper.etl.CHUNKSIZE', chunk_size)
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')

    # Act
    executemany(insert_sql, pgtestdb_conn, test_table_data_namedtuple)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert result == test_table_data_namedtuple


def test_insert_rows_no_rows(pgtestdb_conn, pgtestdb_test_tables,
                             pgtestdb_insert_sql):
    # Arrange
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')

    # Act
    processed, failed = executemany(insert_sql, pgtestdb_conn, [])

    # Assert
    assert processed == 0
    assert failed == 0

    sql = "SELECT * FROM dest"
    result = iter_rows(sql, pgtestdb_conn)
    assert list(result) == []


def test_insert_rows_bad_query(pgtestdb_conn, test_table_data_namedtuple):
    # Arrange
    insert_sql = "INSERT INTO bad_table VALUES (%s, %s, %s, %s, %s, %s)"

    # Act and assert
    with pytest.raises(ETLHelperInsertError):
        executemany(insert_sql, pgtestdb_conn, test_table_data_namedtuple)


def test_load_namedtuples(pgtestdb_conn, pgtestdb_test_tables, test_table_data_namedtuple):
    # Act
    processed, failed = load('dest', pgtestdb_conn, test_table_data_namedtuple)

    # Assert
    assert processed == len(test_table_data_namedtuple)
    assert failed == 0

    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert result == test_table_data_namedtuple


def test_load_dicts(pgtestdb_conn, pgtestdb_test_tables, test_table_data_dict):
    # Act
    processed, failed = load('dest', pgtestdb_conn, test_table_data_dict)

    # Assert
    assert processed == len(test_table_data_dict)
    assert failed == 0

    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn)
    assert result == test_table_data_dict


def test_load_namedtuples_with_transform_generator(pgtestdb_conn, pgtestdb_test_tables, test_table_data_namedtuple):
    # Act
    def transform(chunk):
        for row in chunk:
            yield row

    processed, failed = load('dest', pgtestdb_conn, test_table_data_namedtuple, transform=transform)

    # Assert
    assert processed == len(test_table_data_namedtuple)
    assert failed == 0

    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert result == test_table_data_namedtuple


@pytest.mark.parametrize('null_data', [
    None,
    [],
    (),
    (item for item in ())  # empty generator
    ])
def test_load_no_data(pgtestdb_conn, pgtestdb_test_tables, null_data):
    # Act
    # Function should not crash when data are missing
    processed, failed = load('dest', pgtestdb_conn, null_data)

    assert processed == 0
    assert failed == 0


@pytest.mark.parametrize('chunk_size', [1, 2, 3, 4])
def test_load_namedtuples_chunk_size(pgtestdb_conn, pgtestdb_test_tables,
                                     test_table_data_namedtuple, chunk_size):
    # Act
    load('dest', pgtestdb_conn, test_table_data_namedtuple, chunk_size=chunk_size)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert result == test_table_data_namedtuple


def test_load_parameters_pass_to_executemany(monkeypatch, pgtestdb_conn,
                                             test_table_data_namedtuple):
    # Arrange
    # Patch 'iter_rows' function within etlhelper.etl module
    mock_executemany = Mock()
    mock_executemany.return_value = (0, 0)
    monkeypatch.setattr(etlhelper_etl, 'executemany', mock_executemany)
    # Sentinel items are unique so confirm object that was passed through
    table = 'my_table'
    commit_chunks = sentinel.commit_chunks
    chunk_size = sentinel.chunk_size

    # Act
    load(table, pgtestdb_conn, test_table_data_namedtuple, commit_chunks=commit_chunks,
         chunk_size=chunk_size)

    # Assert
    # load() function writes SQL query
    sql = """
      INSERT INTO my_table (id, value, simple_text, utf8_text, day,
          date_time)
      VALUES (%s, %s, %s, %s, %s, %s)""".strip()
    sql = re.sub(r"\s\s+", " ", sql)  # replace newlines and whitespace

    mock_executemany.assert_called_once_with(
        sql, pgtestdb_conn, ANY,
        transform=None, on_error=None,
        commit_chunks=sentinel.commit_chunks,
        chunk_size=sentinel.chunk_size)


def test_generate_insert_sql_tuple(pgtestdb_conn):
    # Arrange
    data = (1, 'one')

    # Act
    with pytest.raises(ETLHelperInsertError,
                       match="Row is not a dictionary or namedtuple"):
        generate_insert_sql('my_table', data, pgtestdb_conn)


def test_generate_insert_sql_namedtuple(pgtestdb_conn):
    # Arrange
    TwoColumnRow = namedtuple('TwoColumnRow', ('id', 'data'))
    data = TwoColumnRow(1, 'one')
    expected = 'INSERT INTO my_table (id, data) VALUES (%s, %s)'

    # Act
    sql = generate_insert_sql('my_table', data, pgtestdb_conn)

    # Assert
    assert sql == expected


def test_generate_insert_sql_dictionary(pgtestdb_conn):
    # Act
    data = {'id': 1, 'data': 'one'}
    expected = 'INSERT INTO my_table (id, data) VALUES (%(id)s, %(data)s)'

    # Act
    sql = generate_insert_sql('my_table', data, pgtestdb_conn)

    # Assert
    assert sql == expected


def test_generate_insert_sql_None(pgtestdb_conn):
    # Arrange
    data = None

    # Act
    with pytest.raises(ETLHelperInsertError,
                       match="Row is not a dictionary or namedtuple"):
        generate_insert_sql('my_table', data, pgtestdb_conn)


@pytest.mark.parametrize('table, data', [
    ('Bobby; DROP TABLE students', {'id': 1}),  # bad table
    ('my_table', {'Bobby; DROP TABLE students': 1})  # bad column
])
def test_generate_insert_sql_bad_identifers(table, data, pgtestdb_conn):
    # Act
    with pytest.raises(ETLHelperBadIdentifierError):
        generate_insert_sql(table, data, pgtestdb_conn)
