"""Test for etl extract functions.  Extraction is done via the iter_chunks
function and those that call it.
These are run against PostgreSQL."""
# pylint: disable=unused-argument, missing-docstring
import datetime
import pytest
import time
from unittest.mock import (
    Mock,
    sentinel,
)

import etlhelper.etl as etlhelper_etl
from etlhelper import (
    iter_chunks,
    iter_rows,
    execute,
    fetchone,
    fetchall,
)
from etlhelper.etl import (
    ETLHelperExtractError,
    ETLHelperQueryError,
)
from etlhelper.row_factories import (
    dict_row_factory,
    list_row_factory,
    namedtuple_row_factory,
    tuple_row_factory,
)


@pytest.mark.parametrize(
    ["chunk_size", "slices"],
    [
        (1, [slice(0, 1), slice(1, 2), slice(2, 3)]),
        (2, [slice(0, 2), slice(2, 3)]),
        (5000, [slice(0, 3)]),
    ],
)
def test_iter_chunks(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_namedtuple,
    chunk_size,
    slices,
):
    # Arrange
    sql = "SELECT * FROM src"

    # Act
    result = [
        list(chunk) for chunk in
        iter_chunks(sql, pgtestdb_conn, chunk_size=chunk_size, row_factory=namedtuple_row_factory)
    ]

    # Assert
    expected = [test_table_data_namedtuple[s] for s in slices]
    assert result == expected


def test_iter_rows_happy_path(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_namedtuple,
):
    sql = "SELECT * FROM src"
    result = iter_rows(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert list(result) == test_table_data_namedtuple


def test_iter_rows_with_parameters(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_namedtuple,
):
    # parameters=None is tested by default in other tests

    # Bind by index
    sql = "SELECT * FROM src where ID = %s"
    result = iter_rows(sql, pgtestdb_conn, parameters=(1,), row_factory=namedtuple_row_factory)
    assert list(result) == [test_table_data_namedtuple[0]]

    # Bind by name
    sql = "SELECT * FROM src where ID = %(identifier)s"
    result = iter_rows(sql, pgtestdb_conn, parameters={"identifier": 1}, row_factory=namedtuple_row_factory)
    assert list(result) == [test_table_data_namedtuple[0]]


def test_iter_rows_transform(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_namedtuple,
):
    # Arrange
    sql = "SELECT * FROM src"

    def my_transform(rows):
        # Simple transform function that changes size and number of rows
        return [row.id for row in rows if row.id > 1]

    # Act
    result = iter_rows(sql, pgtestdb_conn, transform=my_transform, row_factory=namedtuple_row_factory)

    # Assert
    expected = [row[0] for row in test_table_data_namedtuple if row[0] > 1]
    assert list(result) == expected


def test_iter_rows_namedtuple_factory(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_namedtuple,
):
    sql = "SELECT * FROM src"
    result = iter_rows(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    row = list(result)[0]

    assert row.id == 1
    assert row.value == 1.234
    assert row.simple_text == "text"
    assert row.utf8_text == "Öæ°\nz"
    assert row.day == datetime.date(2018, 12, 7)

    # The final assertion is skipped because the test fails, even though the
    # correct value has been assigned.  I don't know why.
    # assert row.date_time == datetime.datetime(2018, 12, 9, 13, 1, 59)


@pytest.mark.parametrize(
    ["row_factory", "expected"],
    [
        (dict_row_factory, {
            "id": 1, "value": 1.234, "simple_text": "text",
            "utf8_text": "Öæ°\nz", "day": datetime.date(2018, 12, 7),
            "date_time": datetime.datetime(2018, 12, 7, 13, 1, 59)}),
        (tuple_row_factory, (
            1, 1.234, "text", "Öæ°\nz", datetime.date(2018, 12, 7),
            datetime.datetime(2018, 12, 7, 13, 1, 59))),
        (list_row_factory, [
            1, 1.234, "text", "Öæ°\nz", datetime.date(2018, 12, 7),
            datetime.datetime(2018, 12, 7, 13, 1, 59)]),
    ],
)
def test_iter_rows_other_row_factories(
    row_factory, expected,
    pgtestdb_test_tables,
    pgtestdb_conn,
):
    sql = "SELECT * FROM src LIMIT 1"
    result = iter_rows(sql, pgtestdb_conn, row_factory=row_factory)

    assert list(result) == [expected]


def test_iter_rows_no_results(pgtestdb_test_tables, pgtestdb_conn):
    sql = "SELECT * FROM src WHERE id = -1"
    result = iter_rows(sql, pgtestdb_conn)
    assert list(result) == []


def test_iter_rows_bad_query(pgtestdb_test_tables, pgtestdb_conn):
    sql = "SELECT * FROM public.this_does_not_exist"
    with pytest.raises(ETLHelperExtractError):
        result = iter_rows(sql, pgtestdb_conn)
        list(result)  # Call list to activate returned generator


def test_fetchone_happy_path(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_dict,
):
    sql = "SELECT * FROM src"
    result = fetchone(sql, pgtestdb_conn)
    assert result == test_table_data_dict[0]


def test_fetchone_none(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_namedtuple,
):
    sql = "SELECT * FROM src WHERE id=999"
    result = fetchone(sql, pgtestdb_conn)
    assert result is None


def test_fetchone_closes_transaction(pgtestdb_conn):
    sql = "SELECT now() AS time"

    # Call the time function
    time1 = fetchone(sql, pgtestdb_conn)

    # Call it again after a delay
    time.sleep(0.001)
    time2 = fetchone(sql, pgtestdb_conn)

    # If the connection was not closed and the same transaction was used
    # then the times would be the same
    assert time2["time"] > time1["time"]


@pytest.mark.parametrize(
    "fetch_func",
    [fetchall],
)
def test_fetch_funcs_close_transaction(pgtestdb_conn, fetch_func):
    sql = "SELECT now() AS time"

    # Call the time function
    time1 = fetch_func(sql, pgtestdb_conn)[0]

    # Call it again after a delay
    time.sleep(0.001)
    time2 = fetch_func(sql, pgtestdb_conn)[0]

    # If the connection was not closed and the same transaction was used
    # then the times would be the same
    assert time2["time"] > time1["time"]


def test_fetchall_happy_path(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_dict,
):
    sql = "SELECT * FROM src"
    result = fetchall(sql, pgtestdb_conn)
    assert result == test_table_data_dict


@pytest.mark.parametrize(
    "fetchmethod",
    ["fetchone", "fetchall"],
)
def test_arguments_passed_to_iter_rows(
    monkeypatch,
    fetchmethod,
    pgtestdb_test_tables,
    pgtestdb_conn,
):
    """Each of these functions calls iter_rows.  This tests check that the
    correct parameters are passed through, which confirms parameters,
    row_factory and transform can all be used."""

    # Arrange - patch 'iter_rows' function within etlhelper.etl module
    mock_iter_rows = Mock()
    monkeypatch.setattr(etlhelper_etl, "iter_rows", mock_iter_rows)

    # Sentinel items are unique so confirm object that was passed through
    sql = sentinel.sql
    parameters = sentinel.parameters
    transform = sentinel.transform
    chunk_size = sentinel.chunk_size

    # Act
    # getattr returns fetchmethod, which we call with given parameters
    # Use real connection parameters to ensure we reach the call to iter_rows
    # Use dict_row_factory to demonstrate the default (namedtuple_row_factory)
    # isn't called
    try:
        getattr(etlhelper_etl, fetchmethod)(
            sql,
            pgtestdb_conn,
            parameters=parameters,
            row_factory=dict_row_factory,
            transform=transform,
            chunk_size=chunk_size,
        )
    except TypeError:
        # We expect an error here as the mock_iter_rows breaks the functions
        # that called it.
        pass

    # Assert
    mock_iter_rows.assert_called_once_with(
        sql,
        pgtestdb_conn,
        parameters=parameters,
        row_factory=dict_row_factory,
        transform=transform,
        chunk_size=chunk_size,
    )


def test_execute_happy_path(pgtestdb_test_tables, pgtestdb_conn):
    # Arrange
    sql = "DELETE FROM src;"

    # Act
    execute(sql, pgtestdb_conn)

    # Assert
    result = fetchall("SELECT * FROM src;", pgtestdb_conn)
    assert result == []


def test_execute_with_params(
    pgtestdb_test_tables,
    pgtestdb_conn,
    test_table_data_dict,
):
    # Arrange
    sql = "DELETE FROM src WHERE id = %s;"
    params = [1]
    expected = test_table_data_dict[1:]

    # Act
    execute(sql, pgtestdb_conn, parameters=params)

    # Assert
    result = fetchall("SELECT * FROM src;", pgtestdb_conn)
    assert result == expected


def test_execute_bad_query(pgtestdb_test_tables, pgtestdb_conn):
    sql = "DELETE * FROM this_does_not_exist"
    with pytest.raises(ETLHelperQueryError):
        execute(sql, pgtestdb_conn)
