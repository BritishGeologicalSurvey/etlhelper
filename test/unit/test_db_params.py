"""
Test db params
"""
import pytest

from etlhelper.db_params import DbParams
from etlhelper.exceptions import ETLHelperDbParamsError


def test_db_params_validate_params():
    with pytest.raises(ETLHelperDbParamsError, match=r'.* not in valid types .*'):
        DbParams(dbtype='not valid')


def test_db_params_repr():
    """Test DbParams string representation"""
    test_params = DbParams(
        dbtype='PG',
        host='localhost',
        port=5432,
        dbname='etlhelper',
        user='etlhelper_user')
    result = str(test_params)
    expected = ("DbParams(host='localhost', "
                "port='5432', dbname='etlhelper', "
                "user='etlhelper_user', dbtype='PG')")
    assert result == expected


def test_db_params_from_environment(monkeypatch):
    """
    Test capturing db params from environment settings.
    """
    # Arrange
    monkeypatch.setenv('TEST_DB_PARAMS_ENV_DBTYPE', 'ORACLE')
    monkeypatch.setenv('TEST_DB_PARAMS_ENV_HOST', 'test.host')
    monkeypatch.setenv('TEST_DB_PARAMS_ENV_PORT', '1234')
    monkeypatch.setenv('TEST_DB_PARAMS_ENV_DBNAME', 'testdb')
    monkeypatch.setenv('TEST_DB_PARAMS_ENV_USER', 'testuser')

    # Act
    db_params = DbParams.from_environment(prefix='TEST_DB_PARAMS_ENV_')

    # Assert
    db_params.dbtype = 'ORACLE'
    db_params.host = 'test.host'
    db_params.port = '1234'
    db_params.dbname = 'testdb'
    db_params.user = 'testuser'


def test_db_params_from_environment_not_set(monkeypatch):
    """
    Test missing db params from environment settings.
    """
    # Arrange
    monkeypatch.delenv('TEST_DBTYPE', raising=False)

    # Act
    with pytest.raises(ETLHelperDbParamsError,
                       match=r".*environment variable is not set.*"):
        DbParams.from_environment(prefix='TEST_')
