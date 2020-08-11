"""Tests for setup_oracle_client.py script."""
import pytest
from etlhelper.setup_oracle_client import (
    _install_instantclient, _check_install_status, _cleanup
)


def test_dummy_zipfile(dummy_zipfile):
    assert dummy_zipfile.exists()
    assert dummy_zipfile.is_file()
    assert dummy_zipfile.name == 'instantclient.zip'


def test_install_instantclient_happy_path(dummy_zipfile, tmp_path):
    # Arrange
    zipfile_location = str(dummy_zipfile)
    install_dir = tmp_path / 'oracle_instantclient'
    script_dir = tmp_path / 'scripts'
    bin_dir = tmp_path / 'bin'
    for directory in [install_dir, script_dir, bin_dir]:
        directory.mkdir()

    # Act
    _install_instantclient(zipfile_location, install_dir, script_dir, bin_dir)

    # Assert driver files are present
    assert list(install_dir.glob('libocci.so.*.*')) != []
    assert list(install_dir.glob('libclntsh.so.*.*')) != []

    # Assert symlinks are created
    assert install_dir.joinpath('libocci.so').is_symlink()
    assert install_dir.joinpath('libclntsh.so').is_symlink()

    # Final check - use our own check of install status
    assert _check_install_status(install_dir, script_dir, bin_dir)


def test_check_install_status_installed(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'
    script_dir = mock_installation / 'script'
    bin_dir = mock_installation / 'bin'

    # Act
    already_installed = _check_install_status(install_dir, script_dir, bin_dir)

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
    already_installed = _check_install_status(install_dir, script_dir, bin_dir)

    # Assert
    assert already_installed is False


def test_cleanup(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'
    script_dir = mock_installation / 'script'
    bin_dir = mock_installation / 'bin'

    # Act
    _cleanup(install_dir, script_dir, bin_dir)

    # Assert
    assert not install_dir.exists()
    assert not script_dir.joinpath('oracle_lib_path_export').exists()
    assert not bin_dir.joinpath('oracle_lib_path_export').exists()


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
    libclntsh_19 = install_dir / 'libclntsh.so.19.1'
    libclntsh_19.touch()

    # Symlinks
    (install_dir / 'libocci.so').symlink_to(libocci_19)
    (install_dir / 'libclntsh.so').symlink_to(libclntsh_19)

    # Script files
    path_export_script = script_dir.joinpath('oracle_lib_path_export')
    path_export_script.touch()
    path_export_script_link = bin_dir.joinpath('oracle_lib_path_export')
    path_export_script_link.symlink_to(path_export_script)

    return mock_installation
