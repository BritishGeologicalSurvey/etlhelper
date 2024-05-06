.. _utilities:

Utilities
=========


The following utility functions provide useful database metadata.

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