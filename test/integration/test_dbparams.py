"""Tests for db_params that require a database connection."""
from copy import copy
import pytest

from etlhelper import DbParams
from ..conftest import PGTESTDB

# pylint: disable=unused-argument, missing-docstring


def test_is_reachable():
    assert PGTESTDB.is_reachable()


def test_is_unreachable():
    dbparam = copy(PGTESTDB)  # Copy so real PGTESTDB is not modified
    dbparam['port'] = 1
    assert dbparam.is_reachable() is False


def test_sqlite_dbparam_not_supported():
    sqlitedb = DbParams(
        dbtype='SQLITE',
        filename='sqlite.db',
        dbname='etlhelper',
        user='etlhelper_user')

    with pytest.raises(ValueError):
        sqlitedb.is_reachable()
