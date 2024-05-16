.. _installation:

Installation
============

Python libraries
----------------

ETL Helper is availabe on the Python Package Index and can be installed via ``pip``.

.. code:: bash

   pip install etlhelper


Database driver packages are not included by default and should be
specified in square brackets. Options are ``oracle`` (installs
`oracledb <https://pypi.org/project/oracledb/>`_), ``mssql`` (installs
`pyodbc <https://pypi.org/project/pyodbc/>`_) and ``postgres`` (installs
`psycopg2 <https://pypi.org/project/psycopg2-binary/>`_). Multiple values can be separated by commas.

.. code:: bash

   pip install etlhelper[oracle,postgres]

The ``sqlite3`` driver is included within Python's Standard Library.


Operating system drivers
------------------------

Some databases require additional drivers to be installed at the operating
system level.

MS SQL Server
^^^^^^^^^^^^^

The `pyodbc driver <https://pypi.org/project/pyodbc/>`__ for MS SQL Server requires ODBC
drivers provided by Microsoft.

On Linux, these can be installed via the system package manager.
Follow instructions on `Microsoft SQL Docs website <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017>`__,
or see a working example in our `Dockerfile <https://github.com/BritishGeologicalSurvey/etlhelper/blob/main/Dockerfile>`_.

