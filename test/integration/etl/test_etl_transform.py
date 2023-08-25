"""Tests for etl copy functions.  This includes application of transforms.
These are run against PostgreSQL."""
# pylint: disable=unused-argument, missing-docstring
from datetime import (
    datetime,
    date,
)
from typing import (
    Iterable,
    Any,
)

import pytest

from etlhelper import (
    copy_rows,
    copy_table_rows,
    execute,
    fetchall,
    iter_rows,
    load,
)
from etlhelper.row_factories import (
    dict_row_factory,
    namedtuple_row_factory,
)
from etlhelper.exceptions import ETLHelperBadIdentifierError


def test_copy_rows_happy_path(pgtestdb_conn, pgtestdb_test_tables,
                              pgtestdb_insert_sql, test_table_data_namedtuple):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')
    processed, failed = copy_rows(select_sql, pgtestdb_conn, insert_sql, pgtestdb_conn,
                                  row_factory=namedtuple_row_factory)

    # Assert
    assert processed == len(test_table_data_namedtuple)
    assert failed == 0

    sql = "SELECT * FROM dest"
    result = iter_rows(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert list(result) == test_table_data_namedtuple


def test_copy_table_rows_happy_path(pgtestdb_conn, pgtestdb_test_tables, test_table_data_dict):
    # Arrange and act
    processed, failed = copy_table_rows('src', pgtestdb_conn, pgtestdb_conn, target='dest')

    # Assert
    assert processed == len(test_table_data_dict)
    assert failed == 0

    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn)
    assert result == test_table_data_dict


def test_copy_table_rows_on_error(pgtestdb_test_tables, pgtestdb_conn,
                                  test_table_data_dict):
    # Arrange
    duplicate_id_row_sql = """
       INSERT INTO dest (id, value)
       VALUES (1, 1.234)""".strip()
    execute(duplicate_id_row_sql, pgtestdb_conn)

    # Act
    errors = []
    processed, failed = copy_table_rows('src', pgtestdb_conn, pgtestdb_conn, target='dest',
                                        on_error=errors.extend)

    # Assert
    assert processed == len(test_table_data_dict)
    assert failed == len(errors)

    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn)

    # Check that first row was caught as error
    row, exception = errors[0]
    assert row["id"] == 1
    assert "unique" in str(exception).lower()

    # Check that other rows were inserted correctly
    assert result[1:] == test_table_data_dict[1:]


def test_copy_table_rows_bad_table(pgtestdb_conn, pgtestdb_test_tables):
    # Arrange and act
    with pytest.raises(ETLHelperBadIdentifierError):
        copy_table_rows('bad; sql', pgtestdb_conn, pgtestdb_conn, target='dest')


def transform_return_list(rows):
    """
    Simple transform function that changes size and number of rows.
    """
    return [(row.id, row.value) for row in rows if row.id > 1]


def transform_yield_new_rows(rows):
    """
    Simple transform function that changes size and number of rows.
    """
    for row in rows:
        new_row = (row.id, row.value)
        if row.id > 1:
            yield new_row


def transform_return_generator(rows):
    """
    Simple transform function that changes size and number of rows.
    """
    return ((row.id, row.value) for row in rows if row.id > 1)


def transform_return_new_list(rows):
    """
    Simple transform function that changes size and number of rows
    using traditional for loop syntax.
    """
    transformed = []
    for row in rows:
        if row.id > 1:
            new_row = (row.id, row.value)
            transformed.append(new_row)

    return transformed


@pytest.mark.parametrize('my_transform',
                         [transform_return_list, transform_return_generator,
                          transform_return_new_list, transform_yield_new_rows])
def test_copy_rows_transform(pgtestdb_conn, pgtestdb_test_tables, my_transform):
    # Arrange
    select_sql = "SELECT * FROM src"
    insert_sql = "INSERT INTO dest (id, value) VALUES (%s, %s)"

    expected = [(2, 2.234, 'default', None, None, None),
                (3, 2.234, 'default', None, None, None)]

    # Act
    copy_rows(select_sql, pgtestdb_conn, insert_sql, pgtestdb_conn,
              transform=my_transform, row_factory=namedtuple_row_factory)

    # Assert
    sql = "SELECT * FROM dest"
    result = iter_rows(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert list(result) == expected


def transform_yield_modified_dict(chunk):
    """Add 1 thousand to id, replace text with UPPER CASE and replace newlines
    from utf8_text."""
    for row in chunk:
        row['id'] += 1000
        row['simple_text'] = row['simple_text'].upper()
        row['utf8_text'] = row['utf8_text'].replace('\n', ' ')
        yield row


def test_fetchall_with_modify_dict(pgtestdb_conn, pgtestdb_test_tables):
    # Arrange
    select_sql = "SELECT * FROM src LIMIT 1"
    expected = [{
        'date_time': datetime(2018, 12, 7, 13, 1, 59),
        'day': date(2018, 12, 7),
        'id': 1001,
        'simple_text': 'TEXT',
        'utf8_text': 'Öæ° z',
        'value': 1.234
    }]

    # Act
    result = fetchall(select_sql, pgtestdb_conn, row_factory=dict_row_factory,
                      transform=transform_yield_modified_dict)

    # Assert
    assert result == expected


def test_copy_rows_with_dict_row_factory(pgtestdb_conn, pgtestdb_test_tables):
    # Arrange
    select_sql = "SELECT * FROM src"
    insert_sql = """
        INSERT INTO dest (id, value, simple_text, utf8_text, day, date_time)
        VALUES (
            %(id)s, %(value)s, %(simple_text)s,
            %(utf8_text)s, %(day)s, %(date_time)s
        );
    """

    expected = [(1001, 1.234, 'TEXT', 'Öæ° z', date(2018, 12, 7), datetime(2018, 12, 7, 13, 1, 59)),
                (1002, 2.234, 'TEXT', 'Öæ° z', date(2018, 12, 8), datetime(2018, 12, 8, 13, 1, 59)),
                (1003, 2.234, 'TEXT', 'Öæ° z', date(2018, 12, 9), datetime(2018, 12, 9, 13, 1, 59))]

    # Act
    copy_rows(select_sql, pgtestdb_conn, insert_sql, pgtestdb_conn,
              transform=transform_yield_modified_dict)

    # Assert
    sql = "SELECT * FROM dest"
    result = fetchall(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)
    assert result == expected


def convert_namedtuples_dicts(rows):
    """
    Convert a list of namedtuples to dictionaries.
    This function is used to prepare data.
    """
    return [row._asdict() for row in rows]


def transform_namedtuple(rows: list[tuple[Any]]) -> Iterable[tuple[Any]]:
    """
    Simple transform function for a list of namedtuples which:
        - Adds 1000 to each 'id' value
        - Converts the text in the column 'simple_text' to UPPER
    """
    for row in rows:
        row = row._replace(id=row.id + 1000)
        row = row._replace(simple_text=row.simple_text.upper())
        yield row


def transform_dict(rows: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """
    Simple transform function for a list of dictionaries which:
        - Adds 1000 to each 'id' value
        - Converts the text in the column 'upper_text' to UPPER
    """
    for row in rows:
        row["id"] += 1000
        row["simple_text"] = row["simple_text"].upper()

        yield row


@pytest.mark.parametrize(
    ["test_data_conversion", "my_transform", "expected"],
    [
        (
            None,
            transform_namedtuple,
            [
                # pre-loaded data
                (1, 1.234, 'text', 'Öæ°\nz', date(2018, 12, 7), datetime(2018, 12, 7, 13, 1, 59)),
                (2, 2.234, 'text', 'Öæ°\nz', date(2018, 12, 8), datetime(2018, 12, 8, 13, 1, 59)),
                (3, 2.234, 'text', 'Öæ°\nz', date(2018, 12, 9), datetime(2018, 12, 9, 13, 1, 59)),
                # newly loaded transformed data
                (1001, 1.234, 'TEXT', 'Öæ°\nz', date(2018, 12, 7), datetime(2018, 12, 7, 13, 1, 59)),
                (1002, 2.234, 'TEXT', 'Öæ°\nz', date(2018, 12, 8), datetime(2018, 12, 8, 13, 1, 59)),
                (1003, 2.234, 'TEXT', 'Öæ°\nz', date(2018, 12, 9), datetime(2018, 12, 9, 13, 1, 59)),
            ],
        ),
        (
            convert_namedtuples_dicts,
            transform_dict,
            [
                # pre-loaded data
                (1, 1.234, 'text', 'Öæ°\nz', date(2018, 12, 7), datetime(2018, 12, 7, 13, 1, 59)),
                (2, 2.234, 'text', 'Öæ°\nz', date(2018, 12, 8), datetime(2018, 12, 8, 13, 1, 59)),
                (3, 2.234, 'text', 'Öæ°\nz', date(2018, 12, 9), datetime(2018, 12, 9, 13, 1, 59)),
                # newly loaded transformed data
                (1001, 1.234, 'TEXT', 'Öæ°\nz', date(2018, 12, 7), datetime(2018, 12, 7, 13, 1, 59)),
                (1002, 2.234, 'TEXT', 'Öæ°\nz', date(2018, 12, 8), datetime(2018, 12, 8, 13, 1, 59)),
                (1003, 2.234, 'TEXT', 'Öæ°\nz', date(2018, 12, 9), datetime(2018, 12, 9, 13, 1, 59)),
            ],
        ),
    ],
)
def test_load_transform(
    pgtestdb_conn,
    pgtestdb_test_tables,
    test_table_data_namedtuple,
    test_data_conversion,
    my_transform,
    expected,
):
    # Arrange
    if test_data_conversion:
        test_table_data_namedtuple = test_data_conversion(test_table_data_namedtuple)

    # Act
    processed, failed = load(
        table="src",
        conn=pgtestdb_conn,
        rows=test_table_data_namedtuple,
        transform=my_transform,
    )

    sql = "SELECT * FROM src"
    result = fetchall(sql, pgtestdb_conn, row_factory=namedtuple_row_factory)

    # Assert
    for i, row in enumerate(result):
        assert row == expected[i]
