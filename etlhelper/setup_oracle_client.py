"""Commandline script to configure Oracle Instant Client"""
import argparse
import inspect
import logging
from pathlib import Path
import os
from shutil import copyfile
import sys
from textwrap import dedent
import urllib.request
import zipfile

import cx_Oracle

logging.basicConfig()


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
    if not sys.platform == 'linux':
        # Message for Windows and Mac users
        print(WINDOWS_INSTALL_MESSAGE)
        sys.exit(1)

    # Create install_dir
    install_dir = _create_install_dir()

    # Install from zipfile
    zipfile_path = _install_zipped_files(zip_location, install_dir)

    # Create symlinks
    instantclient_dir = _get_instantclient_dir(zipfile_path)
    _create_symlinks(instantclient_dir)

    # Create path export script
    script_dir = Path(inspect.getfile(sys._getframe(1))).parent
    logging.debug(f"Target directory for script installation: {script_dir}")
    try:
        _create_path_export_script(instantclient_dir, script_dir)
    except PermissionError:
        print(dedent(f"""
            Permission denied to required Python directory: {script_dir}
            Consider using a virtual environment.""".strip()))
        sys.exit(1)


def _create_symlinks(instantclient_dir):
    """Link specific .so file versions to general name."""
    # Specific versions
    occi = next(instantclient_dir.glob('libocci.so.*.*'))
    clntsh = next(instantclient_dir.glob('libclntsh.so.*.*'))

    # General names
    occi_link = instantclient_dir / 'libocci.so'
    clntsh_link = instantclient_dir / 'libclntsh.co'

    # Make links
    try:
        os.symlink(occi, occi_link)
        os.symlink(clntsh, clntsh_link)
        logging.debug(f'Symlinks created for: {[occi, clntsh]}')
    except FileExistsError:
        logging.debug(f'Symlinks exist for: {[occi, clntsh]}')
        pass


def _get_instantclient_dir(zipfile_path):
    """Get directory_name for instantclient based on version"""
    version = zipfile_path.stem.split('linux.x64-')[-1].split('.')[:2]
    dirname = '_'.join(['instantclient'] + version)
    return zipfile_path.parent / dirname


def _install_zipped_files(zip_location, install_dir):
    """Unzip files from archive (downloading if required)."""
    # Check file and download if required
    if zip_location.lower().startswith('http'):
        zipfile_path = _download_zipfile(zip_location, install_dir)
    else:
        if not os.path.isfile(zip_location):
            raise FileNotFoundError(zip_location)
        zipfile_path = install_dir / Path(zip_location).name
        if not os.path.isfile(zipfile_path):
            logging.debug(f'Copying zipfile to {zipfile_path.absolute()}')
            copyfile(zip_location, zipfile_path)

    # Check for previous version
    if _get_instantclient_dir(zipfile_path).is_dir():
        logging.debug(
            f'Zipfile already extracted.')
        return zipfile_path

    # Unzip
    print(
        f'Extracting {zipfile_path} into {install_dir.absolute()}')
    with zipfile.ZipFile(zipfile_path, 'r') as zf:
        zf.extractall(install_dir)

    return zipfile_path


def _create_install_dir():
    """Create directory for installation in etlhelper directory."""
    install_dir = Path(__file__).parent / 'oracle_instantclient'
    if not install_dir.is_dir():
        try:
            os.mkdir(install_dir)
        except PermissionError:
            print(dedent(f"""
                Permission denied to required Python directory: {install_dir}
                Consider using a virtual environment.""".strip()))
            sys.exit(1)

    return install_dir


def _download_zipfile(zip_location, install_dir):
    """Download zipfile to specified directory."""
    filename = zip_location.split('/')[-1]
    target = Path(install_dir) / filename
    if target.is_file():
        logging.debug(f'Zipfile already downloaded: {target.absolute()}')
    else:
        print(f'Downloading {zip_location} to {target}')
        urllib.request.urlretrieve(zip_location,
                                   filename=target.absolute())
    return target.absolute()


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
    except cx_Oracle.DatabaseError as e:
        msg = e.args[0].message

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


def _create_path_export_script(instantclient_dir, script_dir):
    """
    Write an executable Python file to the virtual environment path (or current
    directory if not available) that prints an updated PATH variable including
    the Oracle libraries.
    :param lib_dir: location of Oracle libraries
    :param bin_dir: location of created script (on PATH)
    """
    # Write Python code within 'here document'
    # https://stackoverflow.com/questions/35533473/avoiding-syntax-error-near-unexpected-token-in-bash?rq=1
    logging.debug(f"Path to add to LD_LIBRARY_PATH: {instantclient_dir.absolute()}")
    contents = dedent(f"""
        #!/bin/sh
        # Script to print PATH variable including Oracle drivers, suitable
        # for use with `export` command.

        python << EOF
        print("LD_LIBRARY_PATH={instantclient_dir.absolute()}:{os.getenv('LD_LIBRARY_PATH', '')}")
        EOF
        """).strip()

    script_file = script_dir / 'oracle_lib_path_export'
    script_file.write_text(contents)
    msg = (f"LD_LIBRARY_PATH export printer script written to "
           f"{script_file.absolute()}")
    logging.debug(msg)
    os.chmod(script_file, 0o755)

    # Try to add symlink to virtualenv PATH
    virtualenv_bin_dir = script_dir.parent.parent.parent.parent / 'bin'
    if virtualenv_bin_dir.is_dir():
        link_path = virtualenv_bin_dir / script_file.name
        try:
            os.symlink(script_file.absolute(), link_path)
            logging.debug(f"Symlink added to virtualenv PATH: {link_path.absolute()}")
            added_symlink = True
        except PermissionError:
            logging.debug(f"Could not add symlink to PATH")
            added_symlink = False
    else:
        logging.debug(f"Could not add symlink to PATH")
        added_symlink = False

    # Print instructions
    if added_symlink:
        command = 'oracle_lib_path_export'
    else:
        command = script_file.absolute()

    print(dedent(f"""
        Run the following to set LD_LIBRARY_PATH:

        export "$({command})"
        """).strip() + '\n')


WINDOWS_INSTALL_MESSAGE = dedent("""
    Oracle Instant Client not installed. Download from:

    https://www.oracle.com/technetwork/database/database-technologies/instant-client/downloads/index.html
    """).strip() + '\n'

CLNTSH_MESSAGE = dedent(f"""
    Oracle Instant Client library (libclntsh.so) is not on LD_LIBRARY_PATH.
    Current LD_LIBRARY_PATH: {os.getenv('LD_LIBRARY_PATH', '<not set>')}
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
