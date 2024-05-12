Error Handling
^^^^^^^^^^^^^^

This section describes exception classes and on_error functions.

logged errors

also handling errors in SQL e.g. ON CONFLICT

Handling insert errors
----------------------

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