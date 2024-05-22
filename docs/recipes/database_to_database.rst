Database to database copy ETL script template
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following is a template for an ETL script. It copies copy all the
sensor readings from the previous day from an Oracle source to
PostgreSQL destination.

.. literalinclude:: ../code_demos/recipes/database_to_database.py
   :language: python

It is valuable to create
`idempotent <https://stackoverflow.com/questions/1077412/what-is-an-idempotent-operation>`__
scripts to ensure that they can be rerun without problems. In this
example, the “CREATE TABLE IF NOT EXISTS” command can be called
repeatedly. The DELETE_SQL command clears existing data prior to
insertion to prevent duplicate key errors. SQL syntax such as “INSERT OR
UPDATE”, “UPSERT” or “INSERT … ON CONFLICT” may be more efficient, but
the the exact commands depend on the target database type.
