"""Commandline script to configure Oracle Instant Client"""
import argparse
import logging
import os
import shutil
import socket
import sys
import tempfile
import urllib.request
from pathlib import Path
from textwrap import dedent
from urllib.error import URLError

import cx_Oracle

formatter = logging.Formatter('setup_oracle_client: %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.basicConfig(handlers=[handler], level=logging.INFO)


# Get the latest basiclite client by default.  Other versions can be specified using
# the --zip_location parameter.
ORACLE_DEFAULT_ZIP_URL = ("https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip")  # noqa


def setup_oracle_client(zip_location, reinstall=False):
    """
    Install and configure Oracle Instant Client.  The function will:
        + download Oracle libraries from internet or custom URL if required
        + unzip file and create required symlinks
        + create script that prepends installation directory to LD_LIBRARY_PATH
          environment variable
        + print the name of the script to <stdout>
        + fail gracefully if an error occurs during installation with useful
          log message

    :param zip_location: str, URL or local file path of instantclient zip file
    :param reinstall: bool, reinstall option
    """
    install_dir, ld_library_prepend_script = _get_install_paths()

    # Return if configured already
    if _oracle_client_is_configured() and not reinstall:
        logging.info('Oracle Client library is correctly configured.')
        print(ld_library_prepend_script.absolute())
        return

    # Quit with help message if not Linux
    if sys.platform != 'linux':
        # Message for Windows and Mac users
        logging.error(WINDOWS_INSTALL_MESSAGE)
        sys.exit(1)

    # Gather facts
    already_installed = _check_install_status(install_dir,
                                              ld_library_prepend_script)

    # Install if required
    if not already_installed or reinstall:
        try:
            _install_instantclient(zip_location, install_dir,
                                   ld_library_prepend_script)
        except (URLError, FileNotFoundError, OSError, Exception) as exc:
            logging.error(str(exc))
            sys.exit(1)

    # Print instructions for setting library path
    logging.info('Oracle Client files installed successfully.  Ensure '
                 'ld_library_prepend script is "sourced" to complete '
                 'configuration.')
    print(ld_library_prepend_script.absolute())


def _get_install_paths():
    """
    Return file paths for the installation directory and ld_library_prepend
    script.  This function can be mocked out for testing.
    """
    install_dir = Path(__file__).parent.absolute() / 'oracle_instantclient'
    ld_library_prepend_script = install_dir / 'ld_library_prepend.sh'

    logging.debug("install_dir: %s", install_dir)
    logging.debug("ld_library_prepend_script: %s", ld_library_prepend_script)

    return install_dir, ld_library_prepend_script


def _check_install_status(install_dir, ld_library_prepend_script):
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
    script_exists = install_dir.joinpath('ld_library_prepend.sh').is_file()

    already_installed = (drivers_installed and symlinks_created
                         and script_exists)

    logging.debug("Already installed: %s", already_installed)
    return already_installed


def _install_instantclient(zip_location, install_dir,
                           ld_library_prepend_script):
    """
    Install Oracle Instant Client files.
    """
    _cleanup(install_dir)
    _create_install_dir(install_dir)

    zipfile_path = _check_or_get_zipfile(zip_location)

    _install_libraries(zipfile_path, install_dir)
    _symlink_libraries(install_dir)

    _create_ld_library_prepend_script(install_dir, ld_library_prepend_script)


def _cleanup(install_dir):
    """
    Remove files that may remain from previous installations.
    """
    logging.debug('Cleaning up previous installation')
    shutil.rmtree(install_dir, ignore_errors=True)


def _create_install_dir(install_dir):
    """Create directory for installation in etlhelper directory."""
    try:
        install_dir.mkdir()
    except PermissionError:
        logging.error(dedent(f"""
            Permission denied to required Python directory: {install_dir}
            Consider using a virtual environment.""".strip()))
        sys.exit(1)
    logging.debug('Install directory created at %s', install_dir)


def _check_or_get_zipfile(zip_location):
    """
    args:
        zip_location: (str) URL or path to local file.
    returns:
        zipfile_path (Path-object)
    raises: Exception if URL is invalid or path none-existent or not zipfile.
    """
    if zip_location.startswith("http"):
        zipfile_path = _download_zipfile(zip_location)
    else:
        zipfile_path = Path(zip_location)
        if not zipfile_path.exists():
            raise FileNotFoundError(f"zip_location '{zipfile_path}' does not exist")

    if not (zipfile_path.is_file() and zipfile_path.suffix == ".zip"):
        raise OSError(f"zip_location '{zipfile_path}' is not a valid zip file")

    logging.debug("Using zip file at: %s", zipfile_path)
    return zipfile_path


def _download_zipfile(zip_download_source):
    """Download zipfile to specified directory.
    args:
        zip_download_source
    returns:
        path to downloaded zipfile
    """
    zipfile_name = zip_download_source.split("/")[-1]
    zipfile_download_target = Path(tempfile.gettempdir()) / zipfile_name
    logging.debug("Downloading drivers from: %s to %s", zip_download_source,
                  zipfile_download_target)

    try:
        urllib.request.urlretrieve(zip_download_source,
                                   filename=zipfile_download_target.absolute())
    except URLError as exc:
        if isinstance(exc.reason, socket.gaierror):
            # urllib throws socket.gaierror if server is unreachable
            # we repackage this with a more friendly message
            raise Exception(f"Server unreachable ({exc.reason.args[1]})")
        else:
            # Otherwise let the original URLError bubble up
            raise

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
    # for where the symlink should be, rather than a symlink.
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


def _create_ld_library_prepend_script(install_dir, ld_library_prepend_script):
    """
    Write Bash script that prepends the directory containing the Oracle
    libraries to the LD_LIBRARY_PATH environment variable, providing that
    it isn't already present.  This file can be "sourced" by the end user to
    allow cx_Oracle to find the libraries.

    :param install_dir: location of Instant Client libraries.
    :param ld_library_prepend_script: location to write Bash script.
    """
    # Ensure inputs are Paths
    install_dir = Path(install_dir)
    ld_library_prepend_script = Path(ld_library_prepend_script)

    # Prepare file contents
    lib_path = install_dir.absolute()
    logging.debug("Path to add to LD_LIBRARY_PATH: %s", lib_path)
    contents = dedent(f"""\
        if [ "${{LD_LIBRARY_PATH}}" != "{lib_path}"* ]
        then
            export LD_LIBRARY_PATH="{lib_path}:${{LD_LIBRARY_PATH}}"
        fi""").strip()

    ld_library_prepend_script.write_text(contents)


def _oracle_client_is_configured():
    """
    Check if cx_Oracle can communicate with Oracle Instant Client driver.
    :return: boolean
    """
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
            # See https://cx-oracle.readthedocs.io/en/latest/installation.html?highlight=DPI-1047
            if 'libclntsh.so' in msg:
                # instructions for missing oracle drivers
                logging.debug("Current LD_LIBRARY_PATH: %s",
                              os.getenv('LD_LIBRARY_PATH', '<not set>'))
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

CLNTSH_MESSAGE = (
    "Oracle Instant Client library (libclntsh.so) is not on LD_LIBRARY_PATH or "
    "libclntsh.so is a placeholder and a symlink must be created first."
    )

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

HELP_DESCRIPTION = dedent("""
    Install Oracle Instant Client from URL or local file into Python
    environment.  After installing files, their location must be added to the
    LD_LIBRARY_PATH.  setup_oracle_client writes a script to do this and outputs
    its location to the console.

    All logging output is sent to <stderr>, only the ld_library_prepend script
    location is sent to <stdout>.  This way, the script file can be "sourced"
    with:

    source $(setup_oracle_client)

    The script defaults to Oracle Instant Client Basic Lite.  See Oracle website
    (https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html)
    for alternatives.
    """).strip()


def main():
    """Parse args and run setup_oracle_client function."""
    parser = argparse.ArgumentParser(description=HELP_DESCRIPTION,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        '-z', '--zip_location', type=str,
        help="URL or local file path of instantclient-*-linux-*.zip")
    parser.add_argument(
        "-v", "--verbose", help="print debug-level logging output",
        action="store_true")
    parser.add_argument(
        "--reinstall", dest="reinstall", action="store_true",
        help="Reinstall the client, whether already installed or not")
    args = parser.parse_args()

    if args.zip_location:
        zip_location = args.zip_location
    else:
        zip_location = ORACLE_DEFAULT_ZIP_URL

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    setup_oracle_client(zip_location, reinstall=args.reinstall)


if __name__ == '__main__':
    main()
