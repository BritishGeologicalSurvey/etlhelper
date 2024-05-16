
Extract
^^^^^^^

Functions
---------

ETL Helper provides four functions for extracting data from a SQL query.

The ``fetch*`` functions *return* results once they have finished with the database.

- :func:`fetchone() <etlhelper.fetchone>`: returns the first result.
- :func:`fetchall() <etlhelper.fetchall>`: returns all results as a list.  This function
  returns once all rows have been fetched into memory.

.. code:: python

    import sqlite3
    import etlhelper as etl

    with sqlite3.connect('igneous_rocks.db') as conn:
        first_row = etl.fetchone('SELECT * FROM igneous_rock', conn)
        all_rows = etl.fetchall('SELECT * FROM igneous_rock', conn):

    print(first_row)
    print(all_rows)

returns

.. code:: python

    {'id': 1, 'name': 'basalt', 'grain_size': 'fine'}
    [{'id': 1, 'name': 'basalt', 'grain_size': 'fine'},
     {'id': 2, 'name': 'granite', 'grain_size': 'coarse'}]


The ``iter_*`` functions *yield* data, either one or many rows at a time.
Results are fetched in *chunks*, and only one chunk of data is held in memory at any time.
Within a data processing pipeline, the next step can begin as soon as the first chunk has
been fetched.
The database connection must remain open until all results have been processed.

- :func:`iter_rows() <etlhelper.iter_rows>`: returns an iterator that yields all results, one at a time.
  This function can be used in place of ``fetchall`` within processing pipelines and when
  looping over large datasets.
- :func:`iter_chunks() <etlhelper.iter_chunks>`: returns an iterator that yields chunks of multiple results.
  This provides similar functionality to the ``fetchmany`` method specified in the DB API 2.0.

The following is an example of :func:`iter_rows() <etlhelper.iter_rows>`:

.. code:: python

    import sqlite3
    import etlhelper as etl

    with sqlite3.connect('igneous_rocks.db') as conn:
        for row in etl.iter_rows('SELECT * FROM igneous_rock', conn)
            print(row)

returns

.. code:: python

    {'id': 1, 'name': 'basalt', 'grain_size': 'fine'}
    {'id': 2, 'name': 'granite', 'grain_size': 'coarse'}


Keyword arguments
-----------------

All extract functions are derived from :func:`iter_chunks() <etlhelper.iter_chunks>`
and take the same keyword arguments, which are passed through.

parameters
""""""""""

Variables can be inserted into queries by passing them as parameters.
These “bind variables” are sanitised by the underlying drivers to
prevent `SQL injection attacks <https://xkcd.com/327/>`__.

It is necessary to use the correct
`paramstyle <https://www.python.org/dev/peps/pep-0249/#paramstyle>`__
for the database type as a placeholder (e.g. ``?``, ``:1``).

The paramstyle for a DbParams object can be checked with the
:func:`paramstyle <DbParams.paramstyle>` attribute.

A dictionary is used for named placeholders,

.. code:: python

   select_sql = "SELECT * FROM src WHERE id = :id"  # SQLite style

   with sqlite3.connect("rocks.db") as conn:
       etl.fetchall(sql, conn, parameters={'id': 1})

or a tuple for positional placeholders.

.. code:: python

   select_sql = "SELECT * FROM src WHERE id = ?"  # SQLite style

   with sqlite3.connect("rocks.db") as conn:
       etl.fetchall(sql, conn, parameters=(1,))


Named parameters result in more readable code.


row_factory
"""""""""""

Row factories control the output format of returned rows.
The default row factory for ETL Helper is a dictionary, but this can be
changed with the ``row_factory`` argument.

.. literalinclude:: ../demo_namedtuple.py
   :language: python

The output is:

.. code:: bash

    Row(id=1, name='basalt', grain_size='fine')
    basalt

Four different row_factories are included, based in built-in Python
types:

+-----------------------+------------------+---------+------------------+
| Row Factory           | Attribute access | Mutable | Parameter        |
|                       |                  |         | placeholder      |
+=======================+==================+=========+==================+
| dict_row_factory      | ``row["id"]``    | Yes     | Named            |
| (default)             |                  |         |                  |
+-----------------------+------------------+---------+------------------+
| tuple_row_factory     | ``row[0]``       | No      | Positional       |
+-----------------------+------------------+---------+------------------+
| list_row_factory      | ``row[0]``       | Yes     | Positional       |
+-----------------------+------------------+---------+------------------+
| namedtuple_row_factory| ``row.id`` or    | No      | Positional (or   |
|                       | ``row[0]``       |         | Named with       |
|                       |                  |         | *load*)          |
+-----------------------+------------------+---------+------------------+

The choice of row factory depends on the use case. In general named
tuples and dictionaries are best for readable code, while using tuples
or lists can give a slight increase in performance. Mutable rows are
convenient when used with transform functions because they can be
modified without need to create a whole new output row.

When loading or copying data, it is necessary to use appropriate parameter
placeholder style for the chosen row factory in the INSERT query.
Using the ``tuple_row_factory`` requires a switch from named to positional
parameter placeholders (e.g. ``%s`` instead of ``%(id)s`` for PostgreSQL,
``:1`` instead of ``:id`` for Oracle).
The ``pyodbc`` driver for MSSQL only supports positional placeholders.

When using the ``load`` function in conjuction with ``iter_chunks`` data
must be either named tuples or dictionaries.

transform
"""""""""

The ``transform`` parameter takes a callable (e.g. function) that
transforms the data before returning it.
See the :ref:`Transform <transform>` section for details.

chunk_size
""""""""""

All data extraction functions use ``iter_chunks`` behind the scenes.
This reads rows from the database in *chunks* to prevent them all being
loaded into memory at once.
The ``chunk_size`` argument sets the number of rows in each chunk.
The default ``chunk_size`` is 5000.
