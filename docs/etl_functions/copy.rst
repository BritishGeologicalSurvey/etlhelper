Copy
^^^^

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
-------------------------------------

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
---------

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
