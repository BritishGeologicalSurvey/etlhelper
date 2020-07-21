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


def test_check_install_status_installed(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'
    script_dir = mock_installation / 'script'
    bin_dir = mock_installation / 'bin'

    # Act
    already_installed = check_install_status(install_dir, script_dir, bin_dir)

    # Assert
    assert already_installed


def test_check_install_status_no_links(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'
    script_dir = mock_installation / 'script'
    bin_dir = mock_installation / 'bin'

    (install_dir / 'libocci.so').unlink()
    (install_dir / 'libclntsh.so').unlink()

    # Act
    already_installed = check_install_status(install_dir, script_dir, bin_dir)

    # Assert
    assert already_installed is False


@pytest.fixture(scope="function")
def mock_installation(tmp_path):
    """A mocked installation with files in temporary directory."""
    # Arrange - create required directories and files
    mock_installation = tmp_path / 'mock_installation'
    install_dir = mock_installation / 'install'
    script_dir = mock_installation / 'script'
    bin_dir = mock_installation / 'bin'
    mock_installation.mkdir()
    install_dir.mkdir()
    script_dir.mkdir()
    bin_dir.mkdir()

    # Dummy drivers
    libocci_19 = install_dir / 'libocci.so.19.1'
    libocci_19.touch()
    libclntsh_19 = install_dir / 'libclnsh.so.19.1'
    libclntsh_19.touch()

    # Symlinks
    (install_dir / 'libocci.so').symlink_to(libocci_19)
    (install_dir / 'libclntsh.so').symlink_to(libclntsh_19)

    # Script files
    script_dir.joinpath('oracle_lib_path_export').touch()
    bin_dir.joinpath('oracle_lib_path_export').touch()

    return mock_installation
