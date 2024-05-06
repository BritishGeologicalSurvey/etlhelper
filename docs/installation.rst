.. _installation:

Installation
============

**Python packages**

.. code:: console

   pip install etlhelper

Database driver packages are not included by default and should be
specified in square brackets. Options are ``oracle`` (installs
oracledb), ``mssql`` (installs pyodbc) and ``postgres`` (installs
psycopg2). Multiple values can be separated by commas.

.. code:: bash

   pip install etlhelper[oracle,postgres]

The ``sqlite3`` driver is included within Python's Standard Library.


**Operating system level drivers**

Database-specific configuration
===============================

Oracle
------

Handling of LOBs for Oracle connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Oracle databases have special column types for Character Large Object
(CLOB) and Binary Large Object (BLOB). In ETLHelper, the ``oracledb``
driver has been configured to return these as native Python ``str`` and
``bytes`` objects respectively. This is comparable to the behaviour of
other database drivers e.g. SQLite, PostgreSQL and avoids the user
having to take the extra step of reading the LOB and results in faster
data transfer. However, it is not suitable for LOBs larger than 1 Gb.

To return CLOB and BLOB columns as LOBs, configure the driver as
follows:

.. code:: python

   import etlhelper as etl
   import oracledb

   select_sql = "SELECT my_clob, my_blob FROM my_table"

   with ORACLEDB.connect("ORA_PASSWORD") as conn:
       # By default, ETLHelper returns native types
       result_as_native = etl.fetchall(select_sql, conn)

       # Update oracledb settings to return LOBs
       oracledb.defaults.fetch_lobs = True
       result_as_lobs = etl.fetchall(select_sql, conn)

See the `oracledb
docs <https://python-oracledb.readthedocs.io/en/latest/user_guide/lob_data.html#fetching-lobs-as-strings-and-bytes>`__
for more information.

MS SQL Server
-------------

Installing Microsoft ODBC drivers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `pyodbc driver <https://pypi.org/project/pyodbc/>`__ for MS SQL Server requires ODBC drivers provided by Microsoft.
On Linux, these can be installed via the system package manager.
Follow instructions on `Microsoft SQL Docs website <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017>`__,
or see a working example in our Dockerfile.

Disabling fast_executemany for SQL Server and other pyODBC connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default an ``etlhelper`` pyODBC connection uses a cursor with its
``fast_executemany`` attribute set to ``True``. This setting improves
the performance of the ``executemany`` when performing bulk inserts to a
SQL Server database. However, this overides the default behaviour of
pyODBC and there are some limitations in doing this. Importantly, it is
only recommended for applications that use Microsoft’s ODBC Driver for
SQL Server. See `pyODBC
fast_executemany <https://github.com/mkleehammer/pyodbc/wiki/Features-beyond-the-DB-API#fast_executemany>`__.

Using ``fast_executemany`` may raise a ``MemoryError`` if query involves
columns of types ``TEXT`` and ``NTEXT``, which are now deprecated. Under
these circumstances, ``etlhelper`` falls back on ``fast_executemany``
being set to ``False`` and produces a warning output. See `Inserting
into SQL server with fast_executemany results in
MemoryError <https://github.com/mkleehammer/pyodbc/issues/547>`__.

If required, the ``fast_executemany`` attribute can be set to ``False``
via the ``connect`` function:

.. code:: python

   conn = connect(MSSQLDB, 'MSSQL_PASSWORD', fast_executemany=False)

This keyword argument is used by ``etlhelper``, any further keyword
arguments are passed to the ``connect`` function of the underlying
driver.

Connecting to servers with self-signed certificates with SQL Server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since the ODBC Driver 18 for SQL Server, the default setting has been to
fail certificate validation for servers with self-signed certificates.
It is possible to override this setting within the connection string.

ETLHelper provides an optional argument to the ``connect`` function to
apply the override and trust the server’s self-signed certificate.

.. code:: python

   conn = connect(MSSQLDB, 'MSSQL_PASSWORD', trust_server_certificate=True)

