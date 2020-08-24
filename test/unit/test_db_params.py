"""
Test db params
"""
from unittest.mock import Mock
import pytest

from etlhelper.db_params import DbParams
from etlhelper.exceptions import ETLHelperDbParamsError
import etlhelper.connect as etl_connect


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


def test_db_params_connect(test_params, monkeypatch):
    """
    Assert that the connect function is called with the params object
    and the password variable.
    """
    # Arrange
    mock_connect = Mock()
    monkeypatch.setattr(etl_connect, "connect", mock_connect)

    # Act
    test_params.connect("PASSWORD_VARIABLE", foobarkey="blah")

    # Assert
    mock_connect.assert_called_once_with(test_params, "PASSWORD_VARIABLE", foobarkey="blah")


def test_db_params_get_connection_string(test_params, monkeypatch):
    """
    Test that the correct connection string is given when using the
    DbParams method for connect.
    """
    # Arrange
    monkeypatch.setenv("PASSWORD_VARIABLE", "blahblahblah")

    # Act
    conn_str = test_params.get_connection_string("PASSWORD_VARIABLE")

    # Assert
    assert conn_str == 'host=localhost port=5432 dbname=etlhelper user=etlhelper_user password=blahblahblah'  # noqa


def test_db_params_get_sqlalchemy_connection_string(test_params, monkeypatch):
    """
    Test that the correct connection string is given when using the
    DbParams method for connect, for a SQL Alchemy.
    """
    # Arrange
    monkeypatch.setenv("PASSWORD_VARIABLE", "blahblahblah")

    # Act
    conn_str = test_params.get_sqlalchemy_connection_string("PASSWORD_VARIABLE")

    # Assert
    assert conn_str == 'postgresql://etlhelper_user:blahblahblah@localhost:5432/etlhelper'


def test_db_params_paramstyle(test_params):
    """Test that the correct paramstyle string is returned."""
    assert test_params.paramstyle == 'pyformat'
