
CSV load script template
^^^^^^^^^^^^^^^^^^^^^^^^

The following script is an example of using the ``load`` function to
import data from a CSV file into a database. It shows how a
``transform`` function can perform common parsing tasks such as renaming
columns and converting timestamps into datetime objects. The database
has a ``CHECK`` constraint that rejects any rows with an ID divisible by
1000. An example ``on_error`` function prints the IDs of rows that fail
to insert.

.. literalinclude:: ../code_demos/recipes/csv_files.py
   :language: python

Export data to CSV
^^^^^^^^^^^^^^^^^^

The
`Pandas <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html>`__
library can connect to databases via SQLAlchemy. It has powerful tools
for manipulating tabular data. ETL Helper makes it easy to prepare the
SQL Alchemy connection.

.. literalinclude:: ../code_demos/recipes/csv_files_pandas.py
   :language: python
