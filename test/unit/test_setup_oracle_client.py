"""Unit tests for setup_oracle_client."""
import cx_Oracle

from cx_Oracle import DatabaseError, _Error
import pytest

from unittest.mock import MagicMock
from etlhelper.setup_oracle_client import (
    _oracle_client_is_configured,
    NSL_MESSAGE,
    CLNTSH_MESSAGE,
)


@pytest.mark.parametrize("emsg,expected_return,expected_text",
                         [("ORA-12162: some error", True, ""),
                          ("DPI-1047: libclntsh.so", False, CLNTSH_MESSAGE + "\n")])
def test_oracle_client_is_configured(monkeypatch, capfd, emsg, expected_return, expected_text):
    """
    Tests the oracle_client_is_configured returns False given cetain libs
    missing.

    0) Bad service spec (actually good, because it means the libs are fine - connection 'test' not real anyway)
    1) Bad Oracle client libs (libclntsh)
    2) Bad Network Shared Library (libnsl)
    3) Some other error is handled reasonably

    """
    # Arrange cx_Oracle to raise error when connect() is called
    cx_err = MagicMock(_Error)
    cx_err.message = emsg

    def mock_connect(dummy):
        raise DatabaseError(cx_err)
    monkeypatch.setattr(cx_Oracle, 'connect', mock_connect)

    # Act
    result = _oracle_client_is_configured()
    out, err = capfd.readouterr()
    # Assert
    assert result is expected_return
    assert out == expected_text

@pytest.mark.parametrize("emsg, expected_text",
                         [("DPI-1047: libnsl.so.1", NSL_MESSAGE + "\n"),
                          ("DPI-9999: Some weird error",
                              "cx_Oracle Instant Client Error: DPI-9999: Some weird error\n")])
def test_oracle_client_is_configured_sys_exits(monkeypatch, emsg, expected_text):
    # Arrange cx_Oracle to raise error when connect() is called
    cx_err = MagicMock(_Error)
    cx_err.message = emsg

    def mock_connect(dummy):
        raise DatabaseError(cx_err)
    monkeypatch.setattr(cx_Oracle, 'connect', mock_connect)

    # Act
    with pytest.raises(SystemExit):
        _oracle_client_is_configured()

    # out, err = capfd.readouterr()
    # Assert
    # assert result is expected_return
    # assert out == expected_text
