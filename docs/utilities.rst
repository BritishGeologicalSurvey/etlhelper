.. _utilities:

Utilities
=========

ETL Helper provides utility functions to provide logging information, table metadata
and to allow flow control in threaded workflows.


Debug SQL and monitor progress with logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ETL Helper provides a custom logging handler. Time-stamped messages
indicating the number of rows processed can be enabled by setting the
log level to ``INFO``. Setting the level to ``DEBUG`` provides
information on the query that was run, example data and the database
connection. To enable the logger, use:

.. code:: python

   import etlhelper as etl

   etl.log_to_console()

Output from a call to ``copy_rows`` will look like:

::

   2019-10-07 15:06:22,411 iter_chunks: Fetching rows
   2019-10-07 15:06:22,413 executemany: 1 rows processed
   2019-10-07 15:06:22,416 executemany: 2 rows processed
   2019-10-07 15:06:22,419 executemany: 3 rows processed
   2019-10-07 15:06:22,420 iter_chunks: 3 rows returned
   2019-10-07 15:06:22,420 executemany: 3 rows processed in total

Note: errors on database connections output messages may include login
credentials in clear text.

To use the etlhelper logger directly, access it via:

.. code:: python

   import logging

   import etlhelper as etl

   etl.log_to_console()
   etl_logger = logging.getLogger("etlhelper")
   etl_logger.info("Hello world!")


Table info
^^^^^^^^^^

The ``table_info`` function provides basic metadata for a table. An
optional schema can be used. Note that for ``sqlite`` the schema value
is currently ignored.

.. code:: python

   from etlhelper.utils import table_info

   with ORACLEDB.connect("ORA_PASSWORD") as conn:
       columns = table_info('my_table', conn, schema='my_schema')

The returned value is a list of named tuples of four values. Each tuple
represents one column in the table, giving its name, type, if it has a
NOT NULL constraint and if is has a DEFAULT value constraint. For
example,

.. code:: python

   [
       Column(name='ID', type='NUMBER', not_null=1, has_default=0),
       Column(name='VALUE', type='VARCHAR2', not_null=0, has_default=1),
   ]

the ID column is of type NUMBER and has a NOT NULL constraint but not a
DEFAULT value, while the VALUE column is of type VARCHAR2, can be NULL
but does have a DEFAULT value.


Aborting running jobs
^^^^^^^^^^^^^^^^^^^^^

When running as a script, ``etlhelper`` jobs can be stopped by pressing
*CTRL-C*. This option is not available when the job is running as a
background process, e.g. in a GUI application. The
``abort_etlhelper_threads()`` function is provided to cancel jobs
running in a separate thread by raising an ``ETLHelperAbort`` exception
within the thread.

The state of the data when the job is cancelled (or crashes) depends on
the arguments passed to ``executemany`` (or the functions that call it
e.g. ``load``, ``copy_rows``).

-  If ``commit_chunks`` is ``True`` (default), all chunks up to the one
   where the error occured are committed.
-  If ``commit_chunks`` is ``False``, everything is rolled back and the
   database is unchanged.
-  If an ``on_error`` function is defined, all rows without errors are
   committed.