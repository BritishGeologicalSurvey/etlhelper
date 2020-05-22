from unittest import mock
import pytest
from etlhelper.row_factories import namedtuple_rowfactory
import sqlite3


# Test uses sqlite3 cursor, but all dbapi compliant cursors will be the same.
mock_cursor = mock.MagicMock(sqlite3.Cursor)
mock_cursor.fetchmany.return_value = [
    (1, 2, 3),
    (4, 5, 6),
]


def test_valid_field_names():
    mock_cursor.description = (('id', None, None, None, None, None, None),
                               ('name', None, None, None, None, None, None),
                               ('desc', None, None, None, None, None, None))

    create_row = namedtuple_rowfactory(mock_cursor)

    rows = (create_row(row) for row in mock_cursor.fetchmany(50))

    for i, row in enumerate(rows):
        assert row._fields[0] == "id"
        assert row._fields[1] == "name"
        assert row._fields[2] == "desc"

        assert row[0] == mock_cursor.fetchmany.return_value[i][0]
        assert row[1] == mock_cursor.fetchmany.return_value[i][1]
        assert row[2] == mock_cursor.fetchmany.return_value[i][2]


def test_invalid_field_names():
    mock_cursor.description = (('id', None, None, None, None, None, None),
                               ('count(*)', None, None, None, None, None, None),
                               ('spaced column', None, None, None, None, None, None))

    with pytest.warns(UserWarning) as warn:
        create_row = namedtuple_rowfactory(mock_cursor)
        assert len(warn) == 2
        assert warn[1].message.args[0] == f"Columns renamed: {mock_cursor.description[1][0]} was renamed to _1," \
            f" {mock_cursor.description[2][0]} was renamed to _2"

    rows = (create_row(row) for row in mock_cursor.fetchmany(50))

    for i, row in enumerate(rows):
        assert row._fields[0] == "id"
        assert row._fields[1] == "_1"
        assert row._fields[2] == "_2"

        assert row[0] == mock_cursor.fetchmany.return_value[i][0]
        assert row[1] == mock_cursor.fetchmany.return_value[i][1]
        assert row[2] == mock_cursor.fetchmany.return_value[i][2]
