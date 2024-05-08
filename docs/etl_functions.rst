ETL Functions
=============

Extract
-------

The ``fetchall`` function returns a list of named tuples containing data
as native Python objects.

.. code:: python

   from my_databases import ORACLEDB
   from etlhelper import fetchall

   sql = "SELECT * FROM src"

   with ORACLEDB.connect("ORA_PASSWORD") as conn:
       fetchall(sql, conn)

returns

.. code:: python

   [Row(id=1, value=1.234, simple_text='text', utf8_text='Öæ°\nz',
        day=datetime.date(2018, 12, 7),
        date_time=datetime.datetime(2018, 12, 7, 13, 1, 59)),
    Row(id=2, value=2.234, simple_text='text', utf8_text='Öæ°\nz',
        day=datetime.date(2018, 12, 8),
        date_time=datetime.datetime(2018, 12, 8, 13, 1, 59)),
    Row(id=3, value=2.234, simple_text='text', utf8_text='Öæ°\nz',
        day=datetime.date(2018, 12, 9),
        date_time=datetime.datetime(2018, 12, 9, 13, 1, 59))]

Data are accessible via index (``row[4]``) or name (``row.day``).

Other functions are provided to select data. ``fetchone`` and
``fetchall`` are equivalent to the cursor methods specified in the DBAPI
v2.0. ETL Helper does not include a ``fetchmany`` function - instead use
``iter_chunks`` to loop over a result set in batches of multiple rows.

iter_rows
^^^^^^^^^

It is recommended to use ``iter_rows`` for looping over large result
sets. It is a generator function that only yields data as requested.
This ensures that the data are not all loaded into memory at once.

::

   with ORACLEDB.connect("ORA_PASSWORD") as conn:
       for row in iter_rows(sql, conn):
           do_something(row)

Parameters
^^^^^^^^^^

Variables can be inserted into queries by passing them as parameters.
These “bind variables” are sanitised by the underlying drivers to
prevent `SQL injection attacks <https://xkcd.com/327/>`__. The required
`paramstyle <https://www.python.org/dev/peps/pep-0249/#paramstyle>`__
can be checked with ``MY_DB.paramstyle``. A tuple is used for positional
placeholders, or a dictionary for named placeholders.

.. code:: python

   select_sql = "SELECT * FROM src WHERE id = :id"

   with ORACLEDB.connect("ORA_PASSWORD") as conn:
       fetchall(sql, conn, parameters={'id': 1})

Row factories
^^^^^^^^^^^^^

Row factories control the output format of returned rows.

For example return each row as a dictionary, use the following:

.. code:: python

   from etlhelper import fetchall
   from etlhelper.row_factories import dict_row_factory

   sql = "SELECT * FROM my_table"

   with ORACLEDB.connect('ORACLE_PASSWORD') as conn:
       for row in fetchall(sql, conn, row_factory=dict_row_factory):
           print(row['id'])

The ``dict_row_factory`` is useful when data are to be serialised to
JSON/YAML, as those formats use dictionaries as input.

Four different row_factories are included, based in built-in Python
types:

+------------------+------------------+---------+------------------+
| Row Factory      | Attribute access | Mutable | Parameter        |
|                  |                  |         | placeholder      |
+==================+==================+=========+==================+
| dict_row_factory | ``row["id"]``    | Yes     | Named            |
| (default)        |                  |         |                  |
+------------------+------------------+---------+------------------+
| t                | ``row[0]``       | No      | Positional       |
| uple_row_factory |                  |         |                  |
+------------------+------------------+---------+------------------+
| list_row_factory | ``row[0]``       | Yes     | Positional       |
+------------------+------------------+---------+------------------+
| namedt           | ``row.id`` or    | No      | Positional       |
| uple_row_factory | ``row[0]``       |         |                  |
+------------------+------------------+---------+------------------+

The choice of row factory depends on the use case. In general named
tuples and dictionaries are best for readable code, while using tuples
or lists can give a slight increase in performance. Mutable rows are
convenient when used with transform functions because they can be
modified without need to create a whole new output row.

When using ``copy_rows``, it is necessary to use appropriate parameter
placeholder style for the chosen row factory in the INSERT query. Using
the ``dict_row_factory`` requires a switch from named to positional
parameter placeholders (e.g. ``%(id)s`` instead of ``%s`` for
PostgreSQL, ``:id`` instead of ``:1`` for Oracle). The ``pyodbc`` driver
for MSSQL only supports positional placeholders.

When using the ``load`` function in conjuction with ``iter_chunks`` data
must be either named tuples or dictionaries.

Transform
^^^^^^^^^

The ``transform`` parameter allows passing of a function to transform
the data before returning it. The function must take a list of rows and
return a list of modified rows. Rows of mutable types (dict, list) can
be modified in-place, while rows of immutable types (tuples,
namedtuples) must be created as new objects from the input rows. See
``transform`` for more details.

Chunk size
^^^^^^^^^^

All data extraction functions use ``iter_chunks`` behind the scenes.
This reads rows from the database in chunks to prevent them all being
loaded into memory at once. The default ``chunk_size`` is 5000 and this
can be set via keyword argument.

Load
----


Insert rows
^^^^^^^^^^^

``execute`` can be used to insert a single row or to execute other
single statements e.g. “CREATE TABLE …”. The ``executemany`` function is
used to insert multiple rows of data. Large datasets are broken into
chunks and inserted in batches to reduce the number of queries. The
INSERT query must container placeholders with an appropriate format for
the input data e.g. positional for tuples, named for dictionaries. The
number of rows that were processed and the number that failed is
returned.

.. code:: python

   from etlhelper import executemany

   rows = [(1, 'value'), (2, 'another value')]
   insert_sql = "INSERT INTO some_table (col1, col2) VALUES (%s, %s)"

   with POSTGRESDB.connect('PGPASSWORD') as conn:
       processed, failed = executemany(insert_sql, conn, rows, chunk_size=1000)

The ``chunk_size`` default is 5,000 and it can be set with a keyword
argument. The ``commit_chunks`` flag defaults to ``True``. This ensures
that an error during a large data transfer doesn’t require all the
records to be sent again. Some work may be required to determine which
records remain to be sent. Setting ``commit_chunks`` to ``False`` will
roll back the entire transfer in case of an error.

Some database engines can return autogenerated values (e.g. primary key
IDs) after INSERT statements. To capture these values, use the
``fetchone`` method to execute the SQL command instead.

.. code:: python

   insert_sql = "INSERT INTO my_table (message) VALUES ('hello') RETURNING id"

   with POSTGRESDB.connect('PGPASSWORD') as conn:
       result = fetchone(insert_sql, conn)

   print(result.id)

The ``load`` function is similar to ``executemany`` except that it
autogenerates an insert query based on the data provided. It uses
``generate_insert_query`` to remove the need to explicitly write the
query for simple cases. By calling this function manually, users can
create a base insert query that can be extended with clauses such as
``ON CONFLICT DO NOTHING``.

NOTE: the ``load`` function uses the first row of data to generate the
list of column for the insert query. If later items in the data contain
extra columns, those columns will not be inserted and no error will be
raised.

As ``generate_insert_query`` creates SQL statements from user-provided
input, it checks the table and column names to ensure that they only
contain valid characters.

Handling insert errors
^^^^^^^^^^^^^^^^^^^^^^

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

Copy table rows
^^^^^^^^^^^^^^^

``copy_table_rows`` provides a simple way to copy all the data from one
table to another. It can take a ``transform`` function in case some
modification of the data, e.g. change of case of column names, is
required.

.. code:: python

   from my_databases import POSTGRESDB, ORACLEDB
   from etlhelper import copy_table_rows

   with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
       with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
       copy_table_rows('my_table', src_conn, dest_conn)

The ``chunk_size``, ``commit_chunks`` and ``on_error`` parameters can
all be set. A tuple with counts of rows processed and failed is
returned.

Note that the target table must already exist. If it doesn’t, you can
use ``execute`` with a ``CREATE TABLE IF NOT EXISTS ...`` statement to
create it first. See the recipes for examples.

Combining ``iter_rows`` with ``load``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For extra control selecting the data to be transferred, ``iter_rows``
can be combined with ``load``.

.. code:: python

   from my_databases import POSTGRESDB, ORACLEDB
   from etlhelper import iter_rows, load

   select_sql = """
       SELECT id, name, value FROM my_table
       WHERE value > :min_value
   """

   with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
       with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
           rows = iter_rows(select_sql, src_conn, parameters={'min_value': 99})
       load('my_table', dest_conn, rows)

Copy rows
^^^^^^^^^

Customising both queries gives the greatest control on data selection
and loading. ``copy_rows`` takes the results from a SELECT query and
applies them as parameters to an INSERT query. The source and
destination tables must already exist. For example, here we use GROUP BY
and WHERE in the SELECT query and insert extra auto-generated values via
the INSERT query.

.. code:: python

   from my_databases import POSTGRESDB, ORACLEDB
   from etlhelper import copy_rows

   select_sql = """
       SELECT
         customer_id,
         SUM (amount) AS total_amount
       FROM payment
       WHERE id > 1000
       GROUP BY customer_id
   """
   insert_sql = """
       INSERT INTO dest (customer_id, total_amount, loaded_by, load_time)
       VALUES (%s, %s, current_user, now())
   """

   with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
       with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
           copy_rows(select_sql, src_conn, insert_sql, dest_conn)

``parameters`` can be passed to the SELECT query as before and the
``commit_chunks``, ``chunk_size`` and ``on_error`` options can be set.

A tuple of rows processed and failed is returned.

.. _transform-1:

Transform
---------

Data can be transformed in-flight by applying a transform function. This
is any Python callable (e.g. function or class) that takes an iterator
and returns another iterator (e.g. list or generator via the ``yield``
statement). Transform functions are applied to data as they are read
from the database (in the case of data fetching functions and
``copy_rows``), or before they are passed as query parameters (to
``executemany`` or ``load``). When used with ``copy_rows`` or
``executemany`` the INSERT query must contain the correct placeholders
for the transform result.

The ``iter_chunks`` and ``iter_rows`` functions that are used internally
return generators. Each chunk or row of data is only accessed when it is
required. This allows data transformation to be performed via
`memory-efficient
iterator-chains <https://dbader.org/blog/python-iterator-chains>`__.

The simplest transform functions modify data returned mutable row
factories e.g., ``dict_row_factory`` in-place. The ``yield`` keyword
makes ``my_transform`` a generator function that returns an ``Iterator``
that can loop over the rows.

.. code:: python

   from typing import Iterator
   from etlhelper.row_factories import dict_row_factory


   def my_transform(chunk: Iterator[dict]) -> Iterator[dict]:
       # Add prefix to id, remove newlines, set lower case email addresses

       for row in chunk:  # each row is a dictionary (mutable)
           row['id'] += 1000
           row['description'] = row['description'].replace('\n', ' ')
           row['email'] = row['email'].lower()
           yield row


   fetchall(select_sql, src_conn, row_factory=dict_row_factory,
            transform=my_transform)

It is also possible to assemble the complete transformed chunk and
return it. This code demonstrates that the returned chunk can have a
different number of rows, and be of different length, to the input.
Because ``namedtuple``\ s are immutable, we have to create a ``new_row``
from each input ``row``.

.. code:: python

   import random
   from typing import Iterator
   from etlhelper.row_factories import namedtuple_row_factory


   def my_transform(chunk: Iterator[tuple]) -> list[tuple]:
       # Append random integer (1-10), filter if <5.

       new_chunk = []
       for row in chunk:  # each row is a namedtuple (immutable)
           extra_value = random.randrange(10)
           if extra_value >= 5:  # some rows are dropped
               new_row = (*row, extra_value)  # new rows have extra column
               new_chunk.append(new_row)

       return new_chunk

   fetchall(select_sql, src_conn, row_factory=namedtuple_row_factory,
            transform=my_transform)

Any Python code can be used within the function and extra data can
result from a calculation, a call to a webservice or a query against
another database. As a standalone function with known inputs and
outputs, the transform functions are also easy to test.

Error Handling
--------------

This section describes exception classes and on_error functions.
