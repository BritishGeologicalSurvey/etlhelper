.. _installation:

Installation
============

Python libraries
----------------

.. code:: console

   pip install etlhelper


Database driver packages are not included by default and should be
specified in square brackets. Options are ``oracle`` (installs
oracledb), ``mssql`` (installs pyodbc) and ``postgres`` (installs
psycopg2). Multiple values can be separated by commas.

.. code:: bash

   pip install etlhelper[oracle,postgres]

The ``sqlite3`` driver is included within Python's Standard Library.


Operating system level drivers
-------------------------------


Database-specific configuration
-------------------------------

Oracle
^^^^^^


MS SQL Server
^^^^^^^^^^^^^

Installing Microsoft ODBC drivers
"""""""""""""""""""""""""""""""""

The `pyodbc driver <https://pypi.org/project/pyodbc/>`__ for MS SQL Server requires ODBC drivers provided by Microsoft.
On Linux, these can be installed via the system package manager.
Follow instructions on `Microsoft SQL Docs website <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017>`__,
or see a working example in our Dockerfile.
