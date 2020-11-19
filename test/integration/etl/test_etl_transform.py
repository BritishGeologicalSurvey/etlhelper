"""Tests for etl copy functions.  This includes application of transforms.
These are run against PostgreSQL."""
# pylint: disable=unused-argument, missing-docstring
from datetime import datetime, date

import pytest

from etlhelper import iter_rows, copy_rows, get_rows
from etlhelper.row_factories import dict_row_factory


def test_copy_rows_happy_path(pgtestdb_conn, pgtestdb_test_tables,
                              pgtestdb_insert_sql, test_table_data):
    # Arrange and act
    select_sql = "SELECT * FROM src"
    insert_sql = pgtestdb_insert_sql.replace('src', 'dest')
    copy_rows(select_sql, pgtestdb_conn, insert_sql, pgtestdb_conn)

    # Assert
    sql = "SELECT * FROM dest"
    result = iter_rows(sql, pgtestdb_conn)
    assert list(result) == test_table_data


def transform_return_list(rows):
    # Simple transform function that changes size and number of rows
    return [(row.id,) for row in rows if row.id > 1]


def transform_return_generator(rows):
    # Simple transform function that changes size and number of rows
    return ((row.id,) for row in rows if row.id > 1)


@pytest.mark.parametrize('my_transform',
                         [transform_return_list, transform_return_generator])
def test_copy_rows_transform(pgtestdb_conn, pgtestdb_test_tables, my_transform):
    # Arrange
    select_sql = "SELECT * FROM src"
    insert_sql = "INSERT INTO dest (id) VALUES (%s)"

    expected = [(2, None, None, None, None, None),
                (3, None, None, None, None, None)]

    # Act
    copy_rows(select_sql, pgtestdb_conn, insert_sql, pgtestdb_conn,
              transform=my_transform)

    # Assert
    sql = "SELECT * FROM dest"
    result = iter_rows(sql, pgtestdb_conn)
    assert list(result) == expected


def transform_modify_dict(chunk):
    """Add 1 thousand to id, replace text with UPPER CASE and replace newlines
    from utf8_text."""
    new_chunk = []

    for row in chunk:
        row['id'] += 1000
        row['simple_text'] = row['simple_text'].upper()
        row['utf8_text'] = row['utf8_text'].replace('\n', ' ')
        new_chunk.append(row)

    return new_chunk


def test_get_rows_with_modify_dict(pgtestdb_conn, pgtestdb_test_tables):
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
    result = get_rows(select_sql, pgtestdb_conn, row_factory=dict_row_factory,
                      transform=transform_modify_dict)

    # Assert
    assert result == expected


def test_copy_rows_with_dict_row_factory(pgtestdb_conn, pgtestdb_test_tables, pgtestdb_insert_sql, test_table_data):
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
              transform=transform_modify_dict, row_factory=dict_row_factory)

    # Assert
    sql = "SELECT * FROM dest"
    result = get_rows(sql, pgtestdb_conn)
    assert result == expected
