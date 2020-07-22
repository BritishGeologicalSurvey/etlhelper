"""Commandline script to configure Oracle Instant Client"""
import argparse
import logging
from pathlib import Path
import os
import shutil
import sys
from textwrap import dedent
import urllib.request
import tempfile

import cx_Oracle

formatter = logging.Formatter('... %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.basicConfig(handlers=[handler])


ORACLE_DEFAULT_ZIP_URL = ("https://download.oracle.com/otn_software/linux/instantclient/"
                          "19600/instantclient-basic-linux.x64-19.6.0.0.0dbru.zip")


def setup_oracle_client(zipfile_location):
    """
    Install and configure Oracle Instant Client.  The function will:
        + download Oracle libraries from internet or custom URL if required
        + unzip file and create required symlinks
        + create script that prints command to add libraries to LD_LIBRARY_PATH

    :param zip_location: str, path or URL of instantclient zip file
    """
    # Return if configured already
    if _oracle_client_is_configured():
        print('Oracle Client library is correctly configured')
        return

    # Quit with help message if not Linux
    if sys.platform != 'linux':
        # Message for Windows and Mac users
        print(WINDOWS_INSTALL_MESSAGE)
        sys.exit(1)

    # Gather facts
    install_dir, script_dir, bin_dir = _get_working_dirs()
    already_installed = _check_install_status(install_dir, script_dir, bin_dir)

    # Install if required
    # TODO: Add reinstall option
    if not already_installed:
        _install_instantclient(zipfile_location, install_dir, script_dir,
                               bin_dir)

    # Print instructions for setting library path
    if bin_dir != script_dir:
        command = 'oracle_lib_path_export'
    else:
        command = script_dir / 'oracle_lib_path_export'

    print(dedent(f"""
        InstantClient installed.  Run the following to set LD_LIBRARY_PATH:

        export "$({command})"
        """).strip() + '\n')


def _get_working_dirs():
    """
    Return the directories needed for install.
    """
    # Location for driver files
    install_dir = Path(__file__).parent.absolute() / 'oracle_instantclient'

    # Location for path_export_script
    script_dir = Path(__file__).parent.absolute()

    # We want a bin_dir that is writeable and on the $PATH where we can link
    # to the path_export_script.  The location of the Python executable is
    # a good candidate.
    python_dir = Path(sys.executable).parent.absolute()
    try:
        # Try to write a file
        testfile = python_dir / 'test'
        testfile.touch()
        testfile.unlink()
        # Success!  Use this directory
        bin_dir = python_dir
    except PermissionError:
        # Fall back to script dir
        bin_dir = script_dir

    logging.debug("install_dir: %s", install_dir)
    logging.debug("script_dir: %s", script_dir)
    logging.debug("bin_dir: %s", bin_dir)

    return install_dir, script_dir, bin_dir


def _check_install_status(install_dir, script_dir, bin_dir):
    """
    Determine whether files required for installation are present.
    """
    # Drivers are installed
    try:
        libocci_latest = sorted(install_dir.glob('libocci.so.*.*'))[-1]
        libclntsh_latest = sorted(install_dir.glob('libclntsh.so.*.*'))[-1]
        drivers_installed = (libocci_latest.is_file()
                             and libclntsh_latest.is_file())
    except IndexError:  # means no files are found
        return False

    # Symlinks
    libocci_link = install_dir / 'libocci.so'
    libclntsh_link = install_dir / 'libclntsh.so'
    symlinks_created = (libocci_link.is_symlink()
                        and libclntsh_link.is_symlink())

    # Script exists
    script_exists = script_dir.joinpath('oracle_lib_path_export').exists()

    # Export script linked on path (if required)
    if bin_dir == script_dir:
        script_link_on_path = True
    else:
        script_link_on_path = bin_dir.joinpath('oracle_lib_path_export').exists()

    already_installed = (drivers_installed and symlinks_created and script_exists
                         and script_link_on_path)
    logging.debug("Already installed: %s", already_installed)
    return already_installed


def _install_instantclient(zipfile_location, install_dir, script_dir, bin_dir):
    """
    Install Oracle Instant Client files.
    """
    _cleanup(install_dir, script_dir, bin_dir)
    _create_install_dir(install_dir)

    zipfile_path = _check_or_get_zipfile(zipfile_location)

    _install_libraries(zipfile_path, install_dir)
    _symlink_libraries(install_dir)

    _create_path_export_script(install_dir, script_dir)

    # Symlink onto PATH if bin_dir is writeable
    if bin_dir != script_dir:
        (bin_dir / 'oracle_lib_path_export').symlink_to(
            (script_dir / 'oracle_lib_path_export').absolute())


def _cleanup(install_dir, script_dir, bin_dir):
    """
    Remove files that may remain from previous installations.
    """
    logging.debug('Cleaning up previous installation')
    shutil.rmtree(install_dir, ignore_errors=True)

    path_export_script = script_dir / 'oracle_lib_path_export'
    if path_export_script.is_file():
        path_export_script.unlink()

    path_export_script_link = bin_dir / 'oracle_lib_path_export'
    if path_export_script_link.is_symlink():
        path_export_script_link.unlink()


def _create_install_dir(install_dir):
    """Create directory for installation in etlhelper directory."""
    try:
        install_dir.mkdir()
    except PermissionError:
        print(dedent(f"""
            Permission denied to required Python directory: {install_dir}
            Consider using a virtual environment.""".strip()))
        sys.exit(1)
    logging.debug('Install directory created at %s', install_dir)


def _check_or_get_zipfile(zipfile_location):
    """
    args:
        zipfile_location: (str) path to file, URL or None
    returns:
        zipfile_path (Path-object)
    raises: Exception if URL is invalid or path none-existent or not zipfile.
    """
    assert isinstance(zipfile_location, str), "zipfile_location should be string"
    if zipfile_location == '':
        zipfile_path = _download_zipfile(ORACLE_DEFAULT_ZIP_URL)
    elif zipfile_location.startswith("http"):
        zipfile_path = _download_zipfile(zipfile_location)
    else:
        zipfile_path = Path(zipfile_location)

    if not (zipfile_path.is_file() and zipfile_path.suffix == ".zip"):
        raise OSError(f"Zip path {zipfile_path} is not a valid zip file")

    logging.debug("Using zip file at: %s", zipfile_path)
    return zipfile_path


def _download_zipfile(zip_download_source):
    """Download zipfile to specified directory.
    args:
        zipfile_location
    returns:
        path to downloaded zipfile

    Fails gracefully if download fails. Sys exit -1 with helpful msg
    """
    logging.debug("Downloading drivers from: %s", zip_download_source)
    zipfile_name = zip_download_source.split("/")[-1]
    zipfile_download_target = Path(tempfile.gettempdir()) / zipfile_name
    urllib.request.urlretrieve(zip_download_source,
                               filename=zipfile_download_target.absolute())
    return zipfile_download_target.absolute()


def _install_libraries(zipfile_path, install_dir):
    """
    Install zipfile contents to install_dir.
    """
    # Extract all the files
    shutil.unpack_archive(str(zipfile_path), extract_dir=install_dir)

    # Files are initially extracted into a subdirectory - copy them out
    # and remove subdirectory
    subdir = next(install_dir.glob("instantclient_*"))
    for item in subdir.iterdir():
        item.rename(item.parent.parent / item.name)
    subdir.rmdir()
    logging.debug('Library files unzipped to %s', install_dir)


def _symlink_libraries(install_dir):
    """Link specific .so file versions to general name."""
    # Multiple versions of driver exist, choose the highest
    libocci_latest = sorted(install_dir.glob('libocci.so.*.*'))[-1]
    libclntsh_latest = sorted(install_dir.glob('libclntsh.so.*.*'))[-1]

    # Link names
    occi_link = install_dir / 'libocci.so'
    clntsh_link = install_dir / 'libclntsh.so'

    # Some versions of InstantClient will have a placeholder file
    # for where the symlink should be, rahter than a symlink.
    # It must be removed before trying to create a real symlink.

    # unlink(missing_ok=True) would be possible in Python 3.8+
    if occi_link.exists():
        occi_link.unlink()
    if clntsh_link.exists():
        clntsh_link.unlink()

    # Make links (note Pathlib's reverse syntax)
    occi_link.symlink_to(libocci_latest)
    clntsh_link.symlink_to(libclntsh_latest)
    logging.debug('Symlinks created for: %s, %s',
                  libocci_latest, libclntsh_latest)


def _create_path_export_script(install_dir, script_dir):
    """
    Write an executable Python file that prints an updated LD_LIBRARY_PATH
    variable including the directory containing the Oracle libraries.
    :param install_dir: location of Oracle libraries
    :param script_dir: location to write the script
    """
    # Write Python code within 'here document'
    # https://stackoverflow.com/questions/35533473/avoiding-syntax-error-near-unexpected-token-in-bash?rq=1
    logging.debug("Path to add to LD_LIBRARY_PATH: %s", install_dir.absolute())
    contents = dedent(f"""
        #!/bin/sh
        # Script to print PATH variable including Oracle drivers, suitable
        # for use with `export` command.

        python << EOF
        print("LD_LIBRARY_PATH={install_dir.absolute()}:{os.getenv('LD_LIBRARY_PATH', '')}")
        EOF
        """).strip()

    script_file = script_dir / 'oracle_lib_path_export'
    script_file.write_text(contents)
    os.chmod(script_file, 0o755)
    logging.debug("LD_LIBRARY_PATH export printer script written to %s",
                  script_file)


def _oracle_client_is_configured():
    """
    Check if cx_Oracle can communicate with Oracle Instant Client driver.
    :return: boolean
    """
    # See https://cx-oracle.readthedocs.io/en/latest/installation.html?highlight=DPI-1047
    # Check for libocci.so in LD_LIBRARY_PATH on linux
    # Check for oci.dll in PATH directories on windows
    # Check for libnsl.so.1 *must be v.1* - if only libnsl.so.2 is installed, need hint for user.
    try:
        # This will always fail, as 'test' is not a valid database.
        cx_Oracle.connect('test')
    except cx_Oracle.DatabaseError as exc:
        msg = exc.args[0].message

        # Incorrectly specified service error - this is good!
        if msg.startswith('ORA-12162'):
            return True

        # Driver errors
        if msg.startswith('DPI-1047'):
            if 'libclntsh.so' in msg:
                # instructions for missing oracle drivers
                logging.debug(CLNTSH_MESSAGE)
                return False

            if 'libnsl.so.1' in msg:
                # instructions for network services library
                logging.debug(NSL_MESSAGE)
                sys.exit(1)

        # Unhandled error
        logging.error('cx_Oracle Instant Client Error: %s', msg)
        sys.exit(1)


WINDOWS_INSTALL_MESSAGE = dedent("""
    Oracle Instant Client not installed. Download from:

    https://www.oracle.com/technetwork/database/database-technologies/instant-client/downloads/index.html
    """).strip() + '\n'

CLNTSH_MESSAGE = dedent(f"""
    Oracle Instant Client library (libclntsh.so) is not on LD_LIBRARY_PATH.
    Or, the libclntsh.so file is a placeholder and a symlink must be created first.
    Current LD_LIBRARY_PATH: {os.getenv('LD_LIBRARY_PATH', '<not set>')},
                        """).strip()

NSL_MESSAGE = dedent("""
    The Network Services Library, libnsl.so.1, could not be found.

    If you have already installed libnsl, you may have a **newer**, but incompatible version.

    A workaround is to create a symlink to your installed version of libnsl, for example:
    https://github.com/oracle/node-oracledb/issues/892#issuecomment-387082011

    ln -s [PATH TO SYSTEM libnsl.so.2] [PATH TO LOCAL LIBRARY FOLDER]/libnsl.so.1

    etlhelper adds the Oracle Instant Client directory to the LD_LIBRARY_PATH.
    When using a virtual environment, the command will look something like:

    ln -s /usr/lib64/libnsl.so.2 \
        ${VIRTUAL_ENV}/lib/python3.7/site-packages/etlhelper/oracle_instantclient/libnsl.so.1
    """).strip() + '\n'


def main():
    """Parse args and run setup_oracle_client function."""
    parser = argparse.ArgumentParser(
        description="Install Oracle Instant Client to Python environment")
    parser.add_argument(
        'zip_location', type=str, nargs='*',
        help="Path or URL of instantclient-*-linux-*.zip")
    parser.add_argument(
        "--log", dest="log_level", default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    args = parser.parse_args()

    # This syntax is used to maintain backwards compatiblity and to make
    # zip_location optional but without requiring a flag e.g. --zip_location
    if args.zip_location:
        zip_location = args.zip_location[0]
    else:
        zip_location = ''

    if args.log_level:
        logging.getLogger().setLevel(getattr(logging, args.log_level))

    setup_oracle_client(zip_location)


if __name__ == '__main__':
    main()
