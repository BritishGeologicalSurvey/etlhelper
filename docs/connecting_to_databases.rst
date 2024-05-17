.. _connecting_to_databases:

Connecting to databases
=======================


ETL Helper functions take `DB API 2.0 Connection <https://peps.python.org/pep-0249/#connection-objects>`_ objects as an argument.

Users are free to create their own connections directly or to supply them from connection pools.

Alternatively, ETL Helper's :func:`DbParams <etlhelper.DbParams>` class can be used.

.. _db_params:

DbParams
^^^^^^^^

:class:`DbParams <etlhelper.DbParams>` classes store connection parameters and provide validation and utility
methods to help with creating connections.
Connections can be made via the :func:`connect <etlhelper.DbParams.connect>` function.

The examples below show how to create :class:`DbParams <etlhelper.DbParams>` objects for different
databases.
The instantiation checks that the correct attributes have been provided for the
specified ``dbtype``.
See :ref:`passwords <passwords>` section for how to provide passwords.

.. code:: python

   import etlhelper as etl

   ORACLEDB = etl.DbParams(dbtype='ORACLE', host="localhost", port=1521,
                           dbname="mydata", user="oracle_user")

   POSTGRESDB = etl.DbParams(dbtype='PG', host="localhost", port=5432,
                             dbname="mydata", user="postgres_user")

   SQLITEDB = etl.DbParams(dbtype='SQLITE', filename='/path/to/file.db')

   MSSQLDB = etl.DbParams(dbtype='MSSQL', host="localhost", port=1433,
                          dbname="mydata", user="mssql_user",
                          odbc_driver="ODBC Driver 17 for SQL Server")


:class:`DbParams <etlhelper.DbParams>` objects can also be created from environment variables, using the
:func:`from_environment() <etlhelper.DbParams.from_environment>` function.

.. code:: python

    ## Export environment variables in the shell before running script
    # export ETLHelper_dbtype="PG"
    # export ETLHelper_host="localhost"
    # export ETLHelper_port=5432
    # export ETLHelper_dbname="mydata"
    # export ETLHelper_user="postgres_user"

    POSTGRESDB = etl.DbParams.from_environment(prefix="ETLHelper")


The :func:`is_reachable() <etlhelper.DbParams.is_reachable>` method checks if a
given database can be reached over the network.
This does not require a username or password.

.. code:: python

   if not ORACLEDB.is_reachable():
       raise ETLHelperError("Network problems")



``connect`` function
^^^^^^^^^^^^^^^^^^^^

The :func:`DbParams.connect() <etlhelper.DbParams.connect>` function returns a DBAPI2 connection as
provided by the underlying driver.
Using context-manager syntax as below ensures that the connection is closed after use.

.. code:: python

   with SQLITEDB.connect() as src_conn:
       with POSTGRESDB.connect('PGPASSWORD') as dest_conn:
           do_something()

A standalone :func:`etlhelper.connect() <etlhelper.connect>` function provides backwards-compatibility with
previous releases of ``etlhelper``:

.. code:: python

   import etlhelper as etl
   oracle_conn = etl.connect(ORACLEDB, 'ORACLE_PASSWORD')

Both versions accept additional keyword arguments that are passed to the
`DB API 2.0-compatible connect function <https://peps.python.org/pep-0249/#connect>`_
of the underlying driver.
For example, the following sets a timeout used by ``sqlite3`` to limit how long
it waits to get a lock on a table.

.. code:: python

   conn = MY_SQLITE_DB.connect(timeout=20)


.. _passwords:

Passwords
^^^^^^^^^

Database passwords must be specified via an environment variable.
This reduces the temptation to store them within scripts.

All connection methods take a ``password_variable`` argument with the name of
the environment variable from which the password should be read.

Environment variables can be set on the command line via:

-  ``export ORACLE_PASSWORD=some-secret-password`` on Linux
-  ``set ORACLE_PASSWORD=some-secret-password`` on Windows

Or in a Python terminal via:

.. code:: python

   import os
   os.environ['ORACLE_PASSWORD'] = 'some-secret-password'

No password is required for SQLite databases.

Database-specific connection options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Handling of LOBs for Oracle connections
"""""""""""""""""""""""""""""""""""""""

Oracle databases have special column types for Character Large Object
(CLOB) and Binary Large Object (BLOB). In ETL Helper, the ``oracledb``
driver has been configured to return these as native Python ``str`` and
``bytes`` objects respectively. This is comparable to the behaviour of
other database drivers e.g. SQLite, PostgreSQL and avoids the user
having to take the extra step of reading the LOB and results in faster
data transfer. However, it is not suitable for LOBs larger than 1 Gb.

To return CLOB and BLOB columns as LOBs, configure the driver as
follows:

.. code:: python

   import etlhelper as etl
   import oracledb

   select_sql = "SELECT my_clob, my_blob FROM my_table"

   with ORACLEDB.connect("ORA_PASSWORD") as conn:
       # By default, ETL Helper returns native types
       result_as_native = etl.fetchall(select_sql, conn)

       # Update oracledb settings to return LOBs
       oracledb.defaults.fetch_lobs = True
       result_as_lobs = etl.fetchall(select_sql, conn)

See the `oracledb
docs <https://python-oracledb.readthedocs.io/en/latest/user_guide/lob_data.html#fetching-lobs-as-strings-and-bytes>`__
for more information.


Disabling fast_executemany for SQL Server and other pyODBC connections
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

By default an ``etlhelper`` pyODBC connection uses a cursor with its
``fast_executemany`` attribute set to ``True``.
This setting improves the performance of the `DB API 2.0 executemany() <https://peps.python.org/pep-0249/#executemany>`__
function when performing bulk inserts to a SQL Server database.
However, this overides the default behaviour of pyODBC and there are some
limitations in doing this.
Importantly, it is only recommended for applications that use Microsoft’s ODBC
Driver for SQL Server. See `pyODBC fast_executemany <https://github.com/mkleehammer/pyodbc/wiki/Features-beyond-the-DB-API#fast_executemany>`__.

Using ``fast_executemany`` may raise a ``MemoryError`` if query involves
columns of types ``TEXT`` and ``NTEXT``, which are now deprecated. Under
these circumstances, ``etlhelper`` falls back on ``fast_executemany``
being set to ``False`` and produces a warning output. See `Inserting
into SQL server with fast_executemany results in
MemoryError <https://github.com/mkleehammer/pyodbc/issues/547>`__.

If required, the ``fast_executemany`` attribute can be set to ``False``
via a keyword argument to the ``connect`` function:

.. code:: python

   conn = MSSQLDB.connect('MSSQL_PASSWORD', fast_executemany=False)

This keyword argument is used by ``etlhelper``, any further keyword
arguments are passed to the ``connect`` function of the underlying
driver.

Connecting to servers with self-signed certificates with SQL Server
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Since the ODBC Driver 18 for SQL Server, the default setting has been to
fail certificate validation for servers with self-signed certificates.
It is possible to override this setting within the connection string.

ETL Helper provides an optional argument to the ``connect`` function to
apply the override and trust the server’s self-signed certificate.

.. code:: python

   conn = MSSQLDB.connect('MSSQL_PASSWORD', trust_server_certificate=True)

