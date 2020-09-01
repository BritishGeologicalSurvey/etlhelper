"""Tests for db_params that require a database connection."""
import pytest

from etlhelper import DbParams

# pylint: disable=unused-argument, missing-docstring


def test_is_reachable(pgtestdb_params):
    # Use pgtestdb_params here as it changes hostname for CI tests
    assert pgtestdb_params.is_reachable()


def test_is_unreachable(pgtestdb_params):
    # Use pgtestdb_params here as it changes hostname for CI tests
    dbparam = pgtestdb_params.copy()  # Copy so real PGTESTDB is not modified
    dbparam['port'] = 1
    assert dbparam.is_reachable() is False


def test_sqlite_dbparam_not_supported():
    sqlitedb = DbParams(
        dbtype='SQLITE',
        filename='sqlite.db')

    with pytest.raises(ValueError):
        sqlitedb.is_reachable()
