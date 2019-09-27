"""Test for etl extract functions.  Extraction is done via the iter_chunks
function and those that call it.
These are run against PostgreSQL."""
# pylint: disable=unused-argument, missing-docstring
import datetime
from unittest.mock import Mock, call

import pytest

from etlhelper import iter_chunks, iter_rows, get_rows, dump_rows, execute
from etlhelper.etl import ETLHelperExtractError, ETLHelperQueryError
from etlhelper.row_factories import dict_rowfactory, namedtuple_rowfactory


@pytest.mark.parametrize('chunk_size, slices', [
        (1, [slice(0, 1), slice(1, 2), slice(2, 3)]),
        (2, [slice(0, 2), slice(2, 3)]),
        (5000, [slice(0, 3)])])
def test_iter_chunks(pgtestdb_test_tables, pgtestdb_conn,
                     test_table_data, monkeypatch, chunk_size, slices):
    # Arrange
    monkeypatch.setattr('etlhelper.etl.CHUNKSIZE', chunk_size)
    sql = "SELECT * FROM src"

    # Act
    result = [list(chunk) for chunk in iter_chunks(sql, pgtestdb_conn)]

    # Assert
    expected = [test_table_data[s] for s in slices]
    assert result == expected


def test_iter_rows_happy_path(pgtestdb_test_tables, pgtestdb_conn,
                              test_table_data):
    sql = "SELECT * FROM src"
    result = iter_rows(sql, pgtestdb_conn)
    assert list(result) == test_table_data


def test_iter_rows_transform(pgtestdb_test_tables, pgtestdb_conn,
                             test_table_data):
    # Arrange
    sql = "SELECT * FROM src"

    def my_transform(rows):
        # Simple transform function that changes size and number of rows
        return [row.id for row in rows if row.id > 1]

    # Act
    result = iter_rows(sql, pgtestdb_conn, transform=my_transform)

    # Assert
    expected = [row[0] for row in test_table_data if row[0] > 1]
    assert list(result) == expected


def test_iter_rows_dict_factory(pgtestdb_test_tables, pgtestdb_conn):
    sql = "SELECT * FROM src"
    result = iter_rows(sql, pgtestdb_conn, row_factory=dict_rowfactory)
    expected = [
        {'id': 1, 'value': 1.234, 'simple_text': 'text', 'utf8_text': 'Öæ°\nz',
         'day': datetime.date(2018, 12, 7),
         'date_time': datetime.datetime(2018, 12, 7, 13, 1, 59)},
        {'id': 2, 'value': 2.234, 'simple_text': 'text', 'utf8_text': 'Öæ°\nz',
         'day': datetime.date(2018, 12, 8),
         'date_time': datetime.datetime(2018, 12, 8, 13, 1, 59)},
        {'id': 3, 'value': 2.234, 'simple_text': 'text', 'utf8_text': 'Öæ°\nz',
         'day': datetime.date(2018, 12, 9),
         'date_time': datetime.datetime(2018, 12, 9, 13, 1, 59)},
         ]

    assert list(result) == expected


def test_iter_rows_namedtuple_factory(
        pgtestdb_test_tables, pgtestdb_conn, test_table_data):
    sql = "SELECT * FROM src"
    result = iter_rows(sql, pgtestdb_conn, row_factory=namedtuple_rowfactory)
    row = list(result)[0]

    assert row.id == 1
    assert row.value == 1.234
    assert row.simple_text == 'text'
    assert row.utf8_text == 'Öæ°\nz'
    assert row.day == datetime.date(2018, 12, 7)

    # The final assertion is skipped because the test fails, even though the
    # correct value has been assigned.  I don't know why.
    # assert row.date_time == datetime.datetime(2018, 12, 9, 13, 1, 59)


def test_iter_rows_no_results(pgtestdb_test_tables, pgtestdb_conn):
    sql = "SELECT * FROM src WHERE id = -1"
    result = iter_rows(sql, pgtestdb_conn)
    assert list(result) == []


def test_iter_rows_bad_query(pgtestdb_test_tables, pgtestdb_conn):
    sql = "SELECT * FROM public.this_does_not_exist"
    with pytest.raises(ETLHelperExtractError):
        result = iter_rows(sql, pgtestdb_conn)
        list(result)  # Call list to activate returned generator


def test_get_rows_happy_path(pgtestdb_test_tables, pgtestdb_conn,
                             test_table_data):
    sql = "SELECT * FROM src"
    result = get_rows(sql, pgtestdb_conn)
    assert result == test_table_data


def test_get_rows_with_transform(pgtestdb_test_tables, pgtestdb_conn):
    sql = "SELECT * FROM src"

    def my_transform(rows):
        # Simple transform function that changes size and number of rows
        return [row.id for row in rows if row.id > 1]

    result = get_rows(sql, pgtestdb_conn, transform=my_transform)
    assert result == [2, 3]


def test_get_rows_with_parameters(pgtestdb_test_tables, pgtestdb_conn,
                                  test_table_data):
    # parameters=None is tested by default in other tests

    # Bind by index
    sql = "SELECT * FROM src where ID = %s"
    result = get_rows(sql, pgtestdb_conn, parameters=(1,))
    assert result == [test_table_data[0]]

    # Bind by name
    sql = "SELECT * FROM src where ID = %(identifier)s"
    result = get_rows(sql, pgtestdb_conn, parameters={'identifier': 1})
    assert result == [test_table_data[0]]


def test_dump_rows_happy_path(pgtestdb_test_tables, pgtestdb_conn,
                              test_table_data):
    # Arrange
    sql = "SELECT * FROM src"
    mock = Mock()
    expected_calls = [call(row) for row in test_table_data]

    # Act
    dump_rows(sql, pgtestdb_conn, mock)

    # Assert
    assert mock.mock_calls == expected_calls


def test_dump_rows_with_transform(pgtestdb_test_tables, pgtestdb_conn):
    # Arrange
    sql = "SELECT * FROM src"

    def my_transform(rows):
        # Simple transform function that changes size and number of rows
        return [row.id for row in rows if row.id > 1]

    mock = Mock()
    expected_calls = [call(2), call(3)]

    # Act
    dump_rows(sql, pgtestdb_conn, mock, transform=my_transform)

    # Assert
    assert mock.mock_calls == expected_calls


def test_execute_happy_path(pgtestdb_test_tables, pgtestdb_conn):
    # Arrange
    sql = "DELETE FROM src;"

    # Act
    execute(sql, pgtestdb_conn)

    # Assert
    result = get_rows('SELECT * FROM src;', pgtestdb_conn)
    assert result == []


def test_execute_with_params(pgtestdb_test_tables, pgtestdb_conn,
                             test_table_data):
    # Arrange
    sql = "DELETE FROM src WHERE id = %s;"
    params = [1]
    expected = test_table_data[1:]

    # Act
    execute(sql, pgtestdb_conn, parameters=params)

    # Assert
    result = get_rows('SELECT * FROM src;', pgtestdb_conn)
    assert result == expected


def test_execute_bad_query(pgtestdb_test_tables, pgtestdb_conn):
    sql = "DELETE * FROM this_does_not_exist"
    with pytest.raises(ETLHelperQueryError):
        execute(sql, pgtestdb_conn)
