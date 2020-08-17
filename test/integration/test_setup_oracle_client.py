"""Tests for setup_oracle_client.py script."""
from textwrap import dedent

import pytest
from etlhelper.setup_oracle_client import (
    _install_instantclient, _check_install_status, _cleanup,
    _create_ld_library_prepend_script
)


def test_dummy_zipfile(dummy_zipfile):
    assert dummy_zipfile.exists()
    assert dummy_zipfile.is_file()
    assert dummy_zipfile.name == 'instantclient.zip'


def test_install_instantclient_happy_path(dummy_zipfile, tmp_path):
    # Arrange
    zipfile_location = str(dummy_zipfile)
    install_dir = tmp_path / 'oracle_instantclient'
    install_dir.mkdir()
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    # Act
    _install_instantclient(zipfile_location, install_dir,
                           ld_library_prepend_script)

    # Assert driver files are present
    assert list(install_dir.glob('libocci.so.*.*')) != []
    assert list(install_dir.glob('libclntsh.so.*.*')) != []

    # Assert symlinks are created
    assert install_dir.joinpath('libocci.so').is_symlink()
    assert install_dir.joinpath('libclntsh.so').is_symlink()

    # Assert script is created
    assert ld_library_prepend_script.is_file()

    # Final check - use our own check of install status
    assert _check_install_status(install_dir, ld_library_prepend_script)


def test_check_install_status_installed(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    # Act
    already_installed = _check_install_status(install_dir,
                                              ld_library_prepend_script)

    # Assert
    assert already_installed


def test_check_install_status_no_links(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    (install_dir / 'libocci.so').unlink()
    (install_dir / 'libclntsh.so').unlink()

    # Act
    already_installed = _check_install_status(install_dir,
                                              ld_library_prepend_script)

    # Assert
    assert already_installed is False


def test_check_install_status_no_ld_library_script(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    (install_dir / 'ld_library_prepend.sh').unlink()

    # Act
    already_installed = _check_install_status(install_dir,
                                              ld_library_prepend_script)

    # Assert
    assert already_installed is False


def test_create_ld_library_prepend_script(tmp_path):
    # Arrange
    script_file = tmp_path / 'ld_library_prepend.sh'
    lib_dir = '/my/libs'
    expected = dedent("""\
        if [[ "${LD_LIBRARY_PATH}" != "/my/libs"* ]]
        then
            export LD_LIBRARY_PATH="/my/libs:${LD_LIBRARY_PATH}"
        fi""").strip()

    # Act
    _create_ld_library_prepend_script(lib_dir, script_file)

    # Assert
    assert script_file.read_text() == expected


def test_cleanup(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'

    # Act
    _cleanup(install_dir)

    # Assert
    assert not install_dir.exists()


@pytest.fixture(scope="function")
def mock_installation(tmp_path):
    """A mocked installation with files in temporary directory."""
    # Arrange - create required directories and files
    mock_installation = tmp_path / 'mock_installation'
    install_dir = mock_installation / 'install'
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    mock_installation.mkdir()
    install_dir.mkdir()
    ld_library_prepend_script.write_text('source this file')

    # Dummy drivers
    libocci_19 = install_dir / 'libocci.so.19.1'
    libocci_19.touch()
    libclntsh_19 = install_dir / 'libclntsh.so.19.1'
    libclntsh_19.touch()

    # Symlinks
    (install_dir / 'libocci.so').symlink_to(libocci_19)
    (install_dir / 'libclntsh.so').symlink_to(libclntsh_19)

    return mock_installation
