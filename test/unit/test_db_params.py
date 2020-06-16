"""
Test db params
"""
import pytest

from etlhelper.db_params import DbParams
from etlhelper.exceptions import ETLHelperDbParamsError


@pytest.fixture(scope='function')
def test_params():
    """Return an example DbParams class."""
    test_params_example = DbParams(
        dbtype='PG',
        host='localhost',
        port=5432,
        dbname='etlhelper',
        user='etlhelper_user')
    return test_params_example


def test_db_params_validate_params():
    with pytest.raises(ETLHelperDbParamsError, match=r'.* not in valid types .*'):
        DbParams(dbtype='not valid')


def test_db_params_repr(test_params):
    """Test DbParams string representation"""
    result = str(test_params)
    expected = ("DbParams(host='localhost', "
                "port='5432', dbname='etlhelper', "
                "user='etlhelper_user', dbtype='PG')")
    assert result == expected


def test_db_params_setattr(test_params):
    """Test that we can set a DbParams attribute using a __setattr__ approach"""
    # Set a param using dot notation
    test_params.user = "Data McDatabase"

    assert test_params.user == "Data McDatabase"


def test_db_params_setattr_bad_param(test_params):
    """Test that __setattr__ approach fails for bad parameter"""
    with pytest.raises(AttributeError, match=r'.* is not a valid DbParams attribute: .*'):
        # Set a param using dot notation
        test_params.some_bad_param = "Data McDatabase"


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
    assert db_params.dbtype == 'ORACLE'
    assert db_params.host == 'test.host'
    assert db_params.port == '1234'
    assert db_params.dbname == 'testdb'
    assert db_params.user == 'testuser'


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


def test_db_params_copy(test_params):
    """
    Test db_params can copy themselves.
    """
    # Act
    test_params2 = test_params.copy()

    # Assert
    assert test_params2 == test_params
    assert test_params2 is not test_params
    assert isinstance(test_params2, DbParams)
