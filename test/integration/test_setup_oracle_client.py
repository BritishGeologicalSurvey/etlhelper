"""Tests for setup_oracle_client.py script."""
from etlhelper.setup_oracle_client import install


def test_dummy_zipfile(dummy_zipfile):
    assert dummy_zipfile.exists()
    assert dummy_zipfile.is_file()
    assert dummy_zipfile.name == 'instantclient.zip'


def test_install(dummy_zipfile, tmp_path):
    install_dir = tmp_path / 'oracle_instantclient'

    # Act
    install(dummy_zipfile, install_dir)

    # Assert driver files are present
    assert list(install_dir.glob('libocci.so.*.*')) != []
    assert list(install_dir.glob('libclntsh.so.*.*')) != []

    # Assert symlinks are created
    assert install_dir.joinpath('libocci.so').is_symlink()
    assert install_dir.joinpath('libclntsh.so').is_symlink()
