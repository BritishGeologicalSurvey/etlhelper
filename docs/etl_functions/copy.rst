Copy
^^^^

ETL Helper provides three ways to copy data from one database to another.
These are presented in order of increased control or customisability.

copy_table_rows
---------------

:func:`copy_table_rows() <etlhelper.copy_table_rows>` provides a simple way
to copy all the data from one table to another.
It can take a ``transform`` function in case some modification of the data,
e.g. change of case of column names, is required.

.. literalinclude:: ../code_demos/copy/demo_copy_table_rows.py
   :language: python

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

.. literalinclude:: ../code_demos/copy/demo_copy_iter_rows.py
   :language: python

copy_rows
---------

Customising both queries gives the greatest control on data selection
and loading.
``copy_rows`` takes the results from a SELECT query and applies them as
parameters to an INSERT query.
The source and destination tables must already exist.
For example, here we use GROUP BY and WHERE in the SELECT query and insert extra
auto-generated values via the INSERT query.

.. literalinclude:: ../code_demos/copy/demo_copy_rows.py
   :language: python

``parameters`` can be passed to the SELECT query as before and the
``commit_chunks``, ``chunk_size`` and ``on_error`` options can be set.

A tuple of rows processed and failed is returned.
