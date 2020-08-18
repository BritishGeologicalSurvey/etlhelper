"""
Tests for row factory methods.
"""
import sqlite3
from unittest import mock

import pytest

from etlhelper.row_factories import namedtuple_row_factory


FAKE_ROWS = [(1, 2, 3), (4, 5, 6)]


def test_valid_field_names(mock_cursor):
    mock_cursor.description = (('id', None, None, None, None, None, None),
                               ('name', None, None, None, None, None, None),
                               ('desc', None, None, None, None, None, None))

    create_row = namedtuple_row_factory(mock_cursor)
    rows = [create_row(row) for row in mock_cursor.fetchall()]

    assert rows[0]._fields == ("id", "name", "desc")
    assert rows == FAKE_ROWS


def test_invalid_field_names(mock_cursor):
    mock_cursor.description = (('id', None, None, None, None, None, None),
                               ('count(*)', None, None, None, None, None, None),
                               ('spaced column', None, None, None, None, None, None))

    with pytest.warns(UserWarning) as warn:
        create_row = namedtuple_row_factory(mock_cursor)
        assert len(warn) == 2
        assert (warn[1].message.args[0]
                == 'count(*) was renamed to _1\nspaced column was renamed to _2')

    rows = [create_row(row) for row in mock_cursor.fetchall()]

    assert rows[0]._fields == ('id', '_1', '_2')
    assert rows == FAKE_ROWS


@pytest.fixture(scope='function')
def mock_cursor():
    # Test uses sqlite3 cursor, but all dbapi compliant cursors will be the same.
    mock_cursor = mock.MagicMock(sqlite3.Cursor)
    mock_cursor.fetchall.return_value = FAKE_ROWS
    return mock_cursor
