
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


Parameters
----------

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
-------------

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
---------

The ``transform`` parameter allows passing of a function to transform
the data before returning it. The function must take a list of rows and
return a list of modified rows. Rows of mutable types (dict, list) can
be modified in-place, while rows of immutable types (tuples,
namedtuples) must be created as new objects from the input rows. See
``transform`` for more details.

Chunk size
----------

All data extraction functions use ``iter_chunks`` behind the scenes.
This reads rows from the database in chunks to prevent them all being
loaded into memory at once. The default ``chunk_size`` is 5000 and this
can be set via keyword argument.
