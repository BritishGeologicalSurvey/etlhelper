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

logging.basicConfig()


ORACLE_DEFAULT_ZIP_URL = ("https://download.oracle.com/otn_software/linux/instantclient/"
                          "19600/instantclient-basic-linux.x64-19.6.0.0.0dbru.zip")


def setup_oracle_client(zip_location):
    """
    Check platform and install Oracle Instant Client.  Download file if zip
    location is a url.

    :param zip_location: str, path or URL of instantclient zip file
    :param with_utils: bool, determine if utils are also installed
    """
    if _oracle_client_is_configured():
        print('Oracle Client library is correctly configured')
        return

    # Quit if not Linux
    if sys.platform != 'linux':
        # Message for Windows and Mac users
        print(WINDOWS_INSTALL_MESSAGE)
        sys.exit(1)

    # Gather facts
    install_dir, script_dir, bin_dir = get_working_dirs()
    already_installed = check_install_status(install_dir, script_dir, bin_dir)

    """
    get_working_dirs()
    already_installed = check_install_status(directories)

    if not already_installed or reinstall:
      # All the code below in `install_instantclient(zipfile_location, directories)`
    wipe existing directories and scripts (catching errors if files don't exist)
    # Don't get caught by broken installs

      create install dir (if not already)
      confirm zip file (download if required and check is a file)

      unzip into install_dir (making sure *.so.*.* is in root and not in subfolder)
      make symlinks to .so files

      write oracle_path_lib_export
      make symlink to bin_dir()

    if bin_dir == install_dir:
    # this happens if not in virtual environment so script_dir.parent.parent.... /
       bin is non-existent or not writable
       print message (with oracle_lib_path_export)
    else:
       print message (with absolute location of script file)
        """

    # Create install_dir
    _create_install_dir(install_dir)  # Instead of figuring out the dir in the func

    # Check if zip file is downloaded, if not, download
    zipfile_path = _check_or_get_zipfile(zip_location)

    # Install from zipfile
    instantclient_dir = _install_zipped_files(zipfile_path, install_dir)

    # Create symlinks
    # instantclient_dir = _get_instantclient_dir(zipfile_path)
    symlink_libraries(install_dir)

    logging.debug(f"Target directory for script installation: {script_dir}")
    try:
        _create_path_export_script(instantclient_dir, script_dir, bin_dir)
    except PermissionError:
        print(dedent(f"""
            Permission denied to required Python directory: {script_dir}
            Consider using a virtual environment.""".strip()))
        sys.exit(1)


def get_working_dirs():
    """Return the directories needed for install"""
    # Location for driver files
    install_dir = Path(__file__).parent / 'oracle_instantclient'

    # Location for lib_path_export script
    script_dir = Path(__file__).parent

    # We want a bin_dir that is writeable and on the $PATH where we can link
    # to the lib_path_export script.  The location of the Python executable is
    # a good candidate.
    python_dir = Path(sys.executable).parent
    try:
        # Try to write a file
        testfile = python_dir / 'test'
        testfile.touch
        testfile.unlink()
        # Success!  Use this directory
        bin_dir = python_dir
    except PermissionError:
        # Fall back to script dir
        bin_dir = script_dir

    logging.debug(f"Install dir: {install_dir}, script_dir: {script_dir}, bin dir: {bin_dir}")
    return install_dir, script_dir, bin_dir


def check_install_status(install_dir, script_dir, bin_dir):
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

    return (drivers_installed and symlinks_created and script_exists
            and script_link_on_path)


def install_instantclient(zipfile_location, install_dir, script_dir, bin_dir):
    """
    Install Oracle Instant Client files.
    """
    cleanup(install_dir, script_dir, bin_dir)
    _create_install_dir(install_dir)

    zipfile_path = _check_or_get_zipfile(zipfile_location)

    install_libraries(zipfile_path, install_dir)
    symlink_libraries(install_dir)

    _create_path_export_script(install_dir, script_dir)

    # Symlink onto PATH if bin_dir is writeable
    if bin_dir != script_dir:
        (bin_dir / 'oracle_lib_path_export').symlink_to(
            (script_dir / 'oracle_lib_path_export'))


def cleanup(install_dir, script_dir, bin_dir):
    """
    Remove files that may remain from previous installations.
    """
    shutil.rmtree(install_dir, ignore_errors=True)

    path_export_script = script_dir / 'oracle_lib_path_export'
    if path_export_script.exists():
        path_export_script.unlink()

    path_export_script_link = bin_dir / 'oracle_lib_path_export'
    if path_export_script_link.exists():
        path_export_script_link.unlink()


def _create_install_dir(install_dir):
    """Create directory for installation in etlhelper directory."""
    if not install_dir.is_dir():
        try:
            os.mkdir(install_dir)
        except PermissionError:
            print(dedent(f"""
                Permission denied to required Python directory: {install_dir}
                Consider using a virtual environment.""".strip()))
            sys.exit(1)


def _check_or_get_zipfile(zipfile_location):
    """
    args:
        zipfile_location: (str) path to file, URL or None
    returns:
        zipfile_path (Path-object)
    raises: Exception if URL is invalid or path none-existent or not zipfile.
    """
    assert isinstance(zipfile_location, str), "zipfile_location should be string"
    if zipfile_location is None:
        print('Downloading default Oracle zipfile')
        zip_path = _download_zipfile(ORACLE_DEFAULT_ZIP_URL)
    elif zipfile_location.startswith("http"):
        zip_path = _download_zipfile(zipfile_location)
    else:
        zip_path = Path(zipfile_location)

    if not (zip_path.is_file() and zip_path.suffix == ".zip"):
        raise OSError(f"Zip path {zip_path} is not a valid zip file")
    return zip_path


def _download_zipfile(zip_download_source):
    """Download zipfile to specified directory.
    args:
        zipfile_location
    returns:
        path to downloaded zipfile

    Fails gracefully if download fails. Sys exit -1 with helpful msg
    """
    zipfile_name = zip_download_source.split("/")[-1]
    zipfile_download_target = Path(tempfile.gettempdir()) / zipfile_name
    urllib.request.urlretrieve(zip_download_source,
                               filename=zipfile_download_target.absolute())
    return zipfile_download_target.absolute()


def install_libraries(zipfile_path, install_dir):
    """
    Install zipfile contents to install_dir.
    """
    # Extract all the files
    shutil.unpack_archive(zipfile_path, extract_dir=install_dir)

    # Files are initially extracted into a subdirectory - copy them out
    # and remove subdirectory
    subdir = next(install_dir.glob("instantclient_*"))

    for item in subdir.iterdir():
        item.rename(item.parent.parent / item.name)

    subdir.rmdir()


def symlink_libraries(install_dir):
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
    Write an executable Python file
    that prints an updated LD_LIBRARY_PATH variable including
    the Oracle libraries.
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
    msg = (f"LD_LIBRARY_PATH export printer script written to "
           f"{script_file.absolute()}")
    logging.debug(msg)
    os.chmod(script_file, 0o755)


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
                # these are printed anyway if the function returns false? (DV)
                logging.debug(msg)
                print(CLNTSH_MESSAGE)
                return False
            if 'libnsl.so.1' in msg:
                # instructions for network services library
                logging.debug(msg)
                print(NSL_MESSAGE)
                return False

        # Unhandled error
        print(f'cx_Oracle Instant Client Error: {msg}')
        return False


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

    A workaround for Fedora 28+ users is detailed here:
    https://github.com/oracle/node-oracledb/issues/892#issuecomment-387082011

    i.e. create a symlink to the newer library on the LD_LIBRARY_PATH

    ln -s [PATH TO SYSTEM libnsl.so.2] [PATH TO LOCAL LIBRARY FOLDER]/libnsl.so.1

    etlhelper adds the Oracle Instant Client directory to the LD_LIBRARY_PATH.  When
    using a virtual environment, the command will look something like:

    ln -s /usr/lib64/libnsl.so.2 \
        ${VIRTUAL_ENV}/lib/python3.7/site-packages/etlhelper/oracle_instantclient/instantclient_12_2/libnsl.so.1
    """).strip() + '\n'


def main():
    """Parse args and run setup function."""
    parser = argparse.ArgumentParser(
        description="Install Oracle Instant Client to Python environment")
    parser.add_argument(
        'zip_location', type=str,
        help="Path or URL of instantclient-*-linux-*.zip")
    parser.add_argument(
        "--log", dest="log_level", default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    args = parser.parse_args()

    if args.log_level:
        logging.getLogger().setLevel(getattr(logging, args.log_level))

    setup_oracle_client(args.zip_location)


if __name__ == '__main__':
    main()
