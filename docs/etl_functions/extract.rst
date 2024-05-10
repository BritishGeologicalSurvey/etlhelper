
Extract
^^^^^^^

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
---------

It is recommended to use ``iter_rows`` for looping over large result
sets. It is a generator function that only yields data as requested.
This ensures that the data are not all loaded into memory at once.

::

   with ORACLEDB.connect("ORA_PASSWORD") as conn:
       for row in iter_rows(sql, conn):
           do_something(row)

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
