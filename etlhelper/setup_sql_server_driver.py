"""Commandline script to configure pyodbc for SQL Server"""
import pyodbc

# TODO: Add installation commands for Linux (CentOS and/or Ubuntu/Debian) for auto install


def setup_mssql_driver():
    """
    Check for installed drivers in pyodbc and report instructions if not found.
    """
    drivers = pyodbc.drivers()
    has_mssql_driver = any([d.lower().find('sql server') > -1
                            for d in drivers])

    if has_mssql_driver:
        print(f'pyodbc is correctly configured with following drivers: {drivers}')
        exit(0)
    else:
        print("SQL Server ODBC driver not installed.  Download from:\n"
              "https://docs.microsoft.com/en-gb/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-2017\n"  # noqa
              "and follow instructions at:\n"
              "https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017")  # noqa
        exit(1)


def mssql_odbc_driver_is_loaded(odbc_driver):
    """
    Check if pyodbc can see the specified ODBC driver.
    :return: boolean
    """
    return odbc_driver in pyodbc.drivers()


if __name__ == '__main__':
    setup_mssql_driver()
