
Calling ETL Helper scripts from Apache Airflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following is an `Apache Airflow
DAG <https://airflow.apache.org/docs/stable/concepts.html>`__ that uses
the ``copy_readings`` function defined in the `Database to database
<database_to_database.html>`__ script. The Airflow
scheduler will create tasks for each day since 1 August 2019 and call
``copy_readings`` with the appropriate start and end times.

.. literalinclude:: ../code_demos/recipes/apache_airflow_integration.py
   :language: python
