"""Tests for setup_oracle_client.py script."""
from textwrap import dedent
from urllib.error import URLError

import pytest

from etlhelper.setup_oracle_client import (
    _install_instantclient, _check_install_status, _cleanup,
    _create_ld_library_prepend_script, _check_or_get_zipfile
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


def test_check_install_status_empty_install_dir(mock_installation):
    # Arrange
    install_dir = mock_installation / 'install'
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    # Remove every item in the install_dir to simulate empty install_dir
    for item in install_dir.iterdir():
        item.unlink()

    # Act
    already_installed = _check_install_status(install_dir,
                                              ld_library_prepend_script)

    # Assert
    assert already_installed is False


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


def test_zipfile_bad_url(tmp_path):
    # Arrange
    file_location = "http://bad.url"
    install_dir = tmp_path / 'oracle_instantclient'
    install_dir.mkdir()
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    # Act
    with pytest.raises(Exception) as exc:
        _check_or_get_zipfile(file_location)

    # Assert
    assert str(exc.value) == f"Bad URL given, {file_location}"
    assert not _check_install_status(install_dir, ld_library_prepend_script)


def test_not_found_zipfile_url(tmp_path):
    # Arrange
    file_location = "http://www.bgs.ac.uk/bad-url"
    install_dir = tmp_path / 'oracle_instantclient'
    install_dir.mkdir()
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    # Act
    with pytest.raises(URLError) as exc:
        _check_or_get_zipfile(file_location)

    # Assert
    assert str(exc.value) == "HTTP Error 404: Not Found"
    assert not _check_install_status(install_dir, ld_library_prepend_script)


def test_not_valid_zipfile_url(tmp_path):
    # Arrange
    file_location = "http://www.bgs.ac.uk/about-bgs"
    install_dir = tmp_path / 'oracle_instantclient'
    install_dir.mkdir()
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    # Act
    with pytest.raises(OSError) as exc:
        _check_or_get_zipfile(file_location)

    # Assert
    assert str(exc.value) == f"zip_location /tmp/{file_location.split('/')[-1]} is not a valid zip file"
    assert not _check_install_status(install_dir, ld_library_prepend_script)


def test_not_found_zipfile_local(tmp_path):
    # Arrange
    file_location = "/path/to/zip/file"
    install_dir = tmp_path / 'oracle_instantclient'
    install_dir.mkdir()
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    # Act
    with pytest.raises(FileNotFoundError) as exc:
        _check_or_get_zipfile(file_location)

    # Assert
    assert exc.value.args[0] == f"zip_location {file_location} does not exist"
    assert not _check_install_status(install_dir, ld_library_prepend_script)


def test_not_a_valid_zipfile_local(tmp_path):
    # Arrange
    tmp_text_file = tmp_path / 'not_a_zipfile.txt'
    tmp_text_file.write_text("text")

    install_dir = tmp_path / 'oracle_instantclient'
    install_dir.mkdir()
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    # Act
    with pytest.raises(OSError) as exc:
        _check_or_get_zipfile(str(tmp_text_file))

    # Assert
    assert str(exc.value.args[0]) == f"zip_location {tmp_text_file} is not a valid zip file"
    assert not _check_install_status(install_dir, ld_library_prepend_script)


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
