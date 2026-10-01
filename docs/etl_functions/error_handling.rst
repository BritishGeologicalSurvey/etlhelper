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

.. literalinclude:: ../code_demos/error_handling/demo_extract_error.py
   :language: python

The output is:

.. code:: bash

    etlhelper.exceptions.ETLHelperExtractError: SQL query raised an error.

    SELECT * FROM bad_table

    Required paramstyle: qmark

    no such table: bad_table

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
   etl.executemany(sql, conn, rows, on_error=errors.extend)

   if errors:
       do_something()

Errors can be logged to the ``etlhelper`` logger.

.. literalinclude:: ../code_demos/error_handling/demo_log_error.py
   :language: python

The IDs of failed rows can be written to a file.

.. code:: python

   def write_bad_ids(failed_rows):
       with open('bad_ids.txt', 'at') as out_file:
           for row, exception in failed_rows:
               out_file.write(f"{row.id}\n")

   etl.executemany(sql, conn, rows, on_error=write_bad_ids)

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
:func:`generate_insert_sql() <etlhelper.generate_insert_sql>`) the database
can be instructed how to process such rows.

SQLite: ignore conflicting rows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following example uses SQLite's ``ON CONFLICT DO NOTHING`` to skip rows
that would violate a primary key or unique constraint. The first row is
inserted and the duplicate row is skipped.

.. literalinclude:: ../code_demos/error_handling/demo_sql_conflict_error.py
   :language: python

The output is:

.. code:: bash

   {'id': 1, 'name': 'basalt', 'grain_size': 'fine'}

SQLite: update conflicting rows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``ON CONFLICT (id) DO UPDATE`` to update the existing row instead.
The conflict target, ``id``, selects the primary key constraint that triggers
the update. The ``excluded`` prefix refers to the values from the row that
was about to be inserted.

.. literalinclude:: ../code_demos/error_handling/demo_sql_conflict_update.py
   :language: python

The output is:

.. code:: bash

   {'id': 1, 'name': 'basalt', 'grain_size': 'fine-grained'}

These clauses handle uniqueness conflicts, not every database error.
For example, a ``NOT NULL`` or foreign key violation can still raise an
exception. Choose the conflict target carefully: ``ON CONFLICT (id)`` does
not handle a conflict on the separate ``name`` unique constraint.
Use :ref:`on_error` when failed rows need to be logged or processed in Python.

Database-specific syntax
~~~~~~~~~~~~~~~~~~~~~~~~

SQL conflict handling varies between databases. Consult the database's
documentation before adapting an example:

* `SQLite UPSERT <https://www.sqlite.org/lang_upsert.html>`_ documents
  ``ON CONFLICT DO NOTHING`` and ``ON CONFLICT ... DO UPDATE`` (SQLite 3.24.0
  or later).
* `PostgreSQL INSERT <https://www.postgresql.org/docs/current/sql-insert.html>`_
  documents its ``ON CONFLICT`` clause.
* `Oracle MERGE <https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/MERGE.html>`_
  combines insert and update operations based on a matching condition.
* `SQL Server MERGE <https://learn.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql>`_
  describes its matching clauses and concurrency considerations.
