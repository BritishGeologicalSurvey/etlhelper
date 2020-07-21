"""Tests for setup_oracle_client.py script."""
import pytest
from etlhelper.setup_oracle_client import (
    install_instantclient, check_install_status
)


def test_dummy_zipfile(dummy_zipfile):
    assert dummy_zipfile.exists()
    assert dummy_zipfile.is_file()
    assert dummy_zipfile.name == 'instantclient.zip'


def test_install_instantclient(dummy_zipfile, tmp_path):
    # Arrange
    install_dir = tmp_path / 'oracle_instantclient'

    # Act
    install_instantclient(dummy_zipfile, install_dir, None, None)

    # Assert driver files are present
    assert list(install_dir.glob('libocci.so.*.*')) != []
    assert list(install_dir.glob('libclntsh.so.*.*')) != []

    # Assert symlinks are created
    assert install_dir.joinpath('libocci.so').is_symlink()
    assert install_dir.joinpath('libclntsh.so').is_symlink()
