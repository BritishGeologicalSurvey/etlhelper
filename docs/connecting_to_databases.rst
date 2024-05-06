.. _connecting_to_databases:

Connecting to Databases
=======================



Database connections and DbParams
=================================

ETLHelper can use any DB API 2.0 complient connection object.
Users are free to create their own connections e.g., from connection pools,
or they can use the DbParams objects to help.

DbParams
^^^^^^^^

Database connection details are defined by ``DbParams`` objects.
Connections are made via their ``connect`` functions (see below).
``DbParams`` objects are created as follows or from environment
variables using the ``from_environment()`` function. The class
initialisation function checks that the correct attributes have been
provided for a given ``dbtype``.

.. code:: python

   from etlhelper import DbParams

   ORACLEDB = DbParams(dbtype='ORACLE', host="localhost", port=1521,
                       dbname="mydata", user="oracle_user")

   POSTGRESDB = DbParams(dbtype='PG', host="localhost", port=5432,
                         dbname="mydata", user="postgres_user")

   SQLITEDB = DbParams(dbtype='SQLITE', filename='/path/to/file.db')

   MSSQLDB = DbParams(dbtype='MSSQL', host="localhost", port=1433,
                      dbname="mydata", user="mssql_user",
                      odbc_driver="ODBC Driver 17 for SQL Server")

DbParams objects have a function to check if a given database can be
reached over the network. This does not require a username or password.

.. code:: python

   if not ORACLEDB.is_reachable():
       raise ETLHelperError("Network problems")

Other methods/properties are ``get_connection_string``,
``get_sqlalchemy_connection_string``, ``paramstyle`` and ``copy``. See
function docstrings for details.

``connect`` function
^^^^^^^^^^^^^^^^^^^^

The ``DbParams.connect()`` function returns a DBAPI2 connection as
provided by the underlying driver. Using context-manager syntax as below
ensures that the connection is closed after use.

.. code:: python

   with SQLITEDB.connect() as conn1:
       with POSTGRESDB.connect('PGPASSWORD') as conn2:
           do_something()

A standalone ``connect`` function provides backwards-compatibility with
previous releases of ``etlhelper``:

.. code:: python

   from etlhelper import connect
   conn3 = connect(ORACLEDB, 'ORACLE_PASSWORD')

Both versions accept additional keyword arguments that are passed to the
``connect`` function of the underlying driver. For example, the
following sets the character encoding used by oracledb to ensure that
values are returned as UTF-8:

.. code:: python

   conn4 = connect(ORACLEDB, 'ORACLE_PASSWORD', encoding="UTF-8", nencoding="UTF8")

The above is a solution when special characters are scrambled in the
returned data.

Passwords
^^^^^^^^^

Database passwords must be specified via an environment variable. This
reduces the temptation to store them within scripts. This can be done on
the command line via:

-  ``export ORACLE_PASSWORD=some-secret-password`` on Linux
-  ``set ORACLE_PASSWORD=some-secret-password`` on Windows

Or in a Python terminal via:

.. code:: python

   import os
   os.environ['ORACLE_PASSWORD'] = 'some-secret-password'

No password is required for SQLite databases.