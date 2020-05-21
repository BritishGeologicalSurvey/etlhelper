import pytest

from etlhelper import DbParams
from ..conftest import PGTESTDB


def test_is_reachable():
    assert PGTESTDB.is_reachable()


def test_is_unreachable():
    dbparam = PGTESTDB
    dbparam['port'] = 1
    assert dbparam.is_reachable() is False


def test_sqlite_dbparam_not_supported():
    SQLITEDB = DbParams(
        dbtype='SQLITE',
        filename='sqlite.db',
        dbname='etlhelper',
        user='etlhelper_user')

    with pytest.raises(ValueError) as e:
        SQLITEDB.is_reachable()
