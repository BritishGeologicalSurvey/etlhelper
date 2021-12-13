"""Test for etl load functions.  Data loading is carried out using
the executemany call.  These are run against PostgreSQL."""
# pylint: disable=unused-argument, missing-docstring
from psycopg2.errors import UniqueViolation
import pytest

from etlhelper import iter_rows, get_rows, executemany, load
from etlhelper.etl import ETLHelperInsertError


@pytest.mark.parametrize('commit_chunks', [True, False])
def test_insert_rows_happy_path(pgtestdb_conn, pgtestdb_test_tables,
                                pgtestdb_insert_sql, test_table_data,
                                commit_chunks):
    # Parameterized to ensure success with and without commit_chunks
    # Arrange
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')

    # Act
    executemany(insert_sql, pgtestdb_conn, test_table_data,
                commit_chunks=commit_chunks)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, pgtestdb_conn)
    assert result == test_table_data


@pytest.mark.parametrize('commit_chunks', [True, False])
def test_insert_rows_on_error(pgtestdb_conn, pgtestdb_test_tables,
                              pgtestdb_insert_sql, test_table_data,
                              commit_chunks):
    # Parameterized to ensure success with and without commit_chunks
    # Arrange
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')
    # Create duplicated rows to data that will fail primary key check
    duplicated_rows = test_table_data * 2

    # Act
    errors = []
    executemany(insert_sql, pgtestdb_conn, duplicated_rows,
                on_error=errors.extend, commit_chunks=commit_chunks)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, pgtestdb_conn)
    assert result == test_table_data

    # Assert error handling
    failed_rows, exceptions = zip(*errors)
    assert set(failed_rows) == set(test_table_data)
    assert set([type(e) for e in exceptions]) == {UniqueViolation}


@pytest.mark.parametrize('chunk_size', [1, 2, 3, 4])
def test_insert_rows_chunked(pgtestdb_conn, pgtestdb_test_tables,
                             pgtestdb_insert_sql, test_table_data, monkeypatch,
                             chunk_size):
    # Arrange
    monkeypatch.setattr('etlhelper.etl.CHUNKSIZE', chunk_size)
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')

    # Act
    executemany(insert_sql, pgtestdb_conn, test_table_data)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, pgtestdb_conn)
    assert result == test_table_data


def test_insert_rows_no_rows(pgtestdb_conn, pgtestdb_test_tables,
                             pgtestdb_insert_sql):
    # Arrange
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')

    # Act
    executemany(insert_sql, pgtestdb_conn, [])

    # Assert
    sql = "SELECT * FROM dest"
    result = iter_rows(sql, pgtestdb_conn)
    assert list(result) == []


def test_insert_rows_bad_query(pgtestdb_conn, test_table_data):
    # Arrange
    insert_sql = "INSERT INTO bad_table VALUES (%s, %s, %s, %s, %s, %s)"

    # Act and assert
    with pytest.raises(ETLHelperInsertError):
        executemany(insert_sql, pgtestdb_conn, test_table_data)


def test_load_named_tuples(pgtestdb_conn, pgtestdb_test_tables, test_table_data):
    # Act
    load('dest', pgtestdb_conn, test_table_data)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, pgtestdb_conn)
    assert result == test_table_data


def test_load_dicts(pgtestdb_conn, pgtestdb_test_tables, test_table_data):
    # Arrange
    data_as_dicts = [row._asdict() for row in test_table_data]

    # Act
    load('dest', pgtestdb_conn, data_as_dicts)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, pgtestdb_conn)
    assert result == test_table_data
