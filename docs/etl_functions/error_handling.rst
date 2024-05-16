Error Handling
^^^^^^^^^^^^^^

This section describes Exception classes, ``on_error`` functions and error
handling via SQL.


ETLHelperError
--------------

ETL Helper has a :ref:`variety of Exception classes <exceptions>`, all of which are subclasses
of the :class:`ETLHelperError <etlhelper.exceptions.ETLHelperError>` base class.

To aid debugging,
the :class:`ETLHelperQueryError <etlhelper.exceptions.ETLHelperQueryError>`,
:class:`ETLHelperExtractError <etlhelper.exceptions.ETLHelperExtractError>` and
:class:`ETLHelperInsertError <etlhelper.exceptions.ETLHelperInsertError>`
classes print the SQL query and the required paramstyle as well as the error
message returned by the database.

.. literalinclude:: ../demo_error.py
   :language: python

The output is:

.. code:: bash

    etlhelper.exceptions.ETLHelperExtractError: SQL query raised an error.

    SELECT * FROM bad_table

    Required paramstyle: qmark

    no such table: bad_table

also handling errors in SQL e.g. ON CONFLICT

.. _on_error:

on_error
--------

The default behaviour of ``etlhelper`` is to raise an exception on the
first error and abort the transfer. Sometimes it is desirable to ignore
the errors and to do something else with the failed rows. The
``on_error`` parameter allows a function to be passed that is applied to
the failed rows of each chunk. The input is a list of (row, exception)
tuples.

Different examples are given here. The simplest approach is to collect
all the errors into a list to process at the end.

.. code:: python

   errors = []
   executemany(sql, conn, rows, on_error=errors.extend)

   if errors:
       do_something()

Errors can be logged to the ``etlhelper`` logger.

.. code:: python

   import logging

   import etlhelper as etl

   etl.log_to_console()
   logger = logging.getLogger("etlhelper")


   def log_errors(failed_rows):
       for row, exception in failed_rows:
           logger.error(exception)

   executemany(sql, conn, rows, on_error=log_errors)

The IDs of failed rows can be written to a file.

.. code:: python

   def write_bad_ids(failed_rows):
       with open('bad_ids.txt', 'at') as out_file:
           for row, exception in failed_rows:
               out_file.write(f"{row.id}\n")

   executemany(sql, conn, rows, on_error=write_bad_ids)

``executemany``, ``load``, ``copy_rows`` and ``copy_table_rows`` can all
take an ``on_error`` parameter. They each return a tuple containing the
number of rows processed and the number of rows that failed.


Error handling via SQL
----------------------

The ``on_error`` functions allow individual failed rows to be processed,
however this flexibility can come at the expense of speed.
Each chunk of data that contains a bad row will be retried on a row-by-row
basis.

Databases also have methods for handling errors e.g. duplicate primary keys
using SQL.
By customising an INSERT query (which can be programmatically generated with
:func:`generate_insert_query() <etlhelper.generate_insert_query>`) the database
can be instructed how to process such rows.

The following example for SQLite will ignore duplicate rows.
Different databases have different syntax and capabilities, including
``upsert`` and ``merge``.

.. literalinclude:: ../demo_on_conflict.py
   :language: python

The output is:

.. code:: bash

   {'id': 1, 'name': 'basalt', 'grain_size': 'fine'}
