.. _recipes:

Recipes
========

The following recipes demonstrate how ``etlhelper`` can be used.

Debug SQL and monitor progress with logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ETLHelper provides a custom logging handler. Time-stamped messages
indicating the number of rows processed can be enabled by setting the
log level to ``INFO``. Setting the level to ``DEBUG`` provides
information on the query that was run, example data and the database
connection. To enable the logger, use:

.. code:: python

   import etlhelper as etl

   etl.log_to_console()

Output from a call to ``copy_rows`` will look like:

::

   2019-10-07 15:06:22,411 iter_chunks: Fetching rows
   2019-10-07 15:06:22,413 executemany: 1 rows processed
   2019-10-07 15:06:22,416 executemany: 2 rows processed
   2019-10-07 15:06:22,419 executemany: 3 rows processed
   2019-10-07 15:06:22,420 iter_chunks: 3 rows returned
   2019-10-07 15:06:22,420 executemany: 3 rows processed in total

Note: errors on database connections output messages may include login
credentials in clear text.

To use the etlhelper logger directly, access it via:

.. code:: python

   import logging

   import etlhelper as etl

   etl.log_to_console()
   etl_logger = logging.getLogger("etlhelper")
   etl_logger.info("Hello world!")

Database to database copy ETL script template
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following is a template for an ETL script. It copies copy all the
sensor readings from the previous day from an Oracle source to
PostgreSQL destination.

.. code:: python

   # copy_readings.py

   import datetime as dt
   from etl_helper import copy_rows
   from my_databases import ORACLEDB, POSTGRESDB

   CREATE_SQL = dedent("""
       CREATE TABLE IF NOT EXISTS sensordata.readings
       (
         sensor_data_id bigint PRIMARY KEY,
         measure_id bigint,
         time_stamp timestamp without time zone,
         meas_value double precision
       )
       """).strip()

   DELETE_SQL = dedent("""
       DELETE FROM sensordata.readings
       WHERE time_stamp BETWEEN %(startdate)s AND %(enddate)s
       """).strip()

   SELECT_SQL = dedent("""
       SELECT id, measure_id, time_stamp, reading
       FROM sensor_data
       WHERE time_stamp BETWEEN :startdate AND :enddate
       ORDER BY time_stamp
       """).strip()

   INSERT_SQL = dedent("""
       INSERT INTO sensordata.readings (sensor_data_id, measure_id, time_stamp,
         meas_value)
       VALUES (%s, %s, %s, %s)
       """).strip()


   def copy_readings(startdate, enddate):
       params = {'startdate': startdate, 'enddate': enddate}

       with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
           with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
               execute(CREATE_SQL dest_conn)
               execute(DELETE_SQL, dest_conn, parameters=params)
               copy_rows(SELECT_SQL, src_conn,
                         INSERT_SQL, dest_conn,
                         parameters=params)


   if __name__ == "__main__":
       # Copy data from 00:00:00 yesterday to 00:00:00 today
       today = dt.combine(dt.date.today(), dt.time.min)
       yesterday = today - dt.timedelta(1)

       copy_readings(yesterday, today)

It is valuable to create
`idempotent <https://stackoverflow.com/questions/1077412/what-is-an-idempotent-operation>`__
scripts to ensure that they can be rerun without problems. In this
example, the “CREATE TABLE IF NOT EXISTS” command can be called
repeatedly. The DELETE_SQL command clears existing data prior to
insertion to prevent duplicate key errors. SQL syntax such as “INSERT OR
UPDATE”, “UPSERT” or “INSERT … ON CONFLICT” may be more efficient, but
the the exact commands depend on the target database type.

Calling ETLHelper scripts from Apache Airflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following is an `Apache Airflow
DAG <https://airflow.apache.org/docs/stable/concepts.html>`__ that uses
the ``copy_readings`` function defined in the script above. The Airflow
scheduler will create tasks for each day since 1 August 2019 and call
``copy_readings`` with the appropriate start and end times.

.. code:: python

   # readings_dag.py

   import datetime as dt
   from airflow import DAG
   from airflow.operators.python_operator import PythonOperator
   import copy_readings


   def copy_readings_with_args(**kwargs):
       # Set arguments for copy_readings from context
       start = kwargs.get('prev_execution_date')
       end = kwargs.get('execution_date')
       copy_readings.copy_readings(start, end)

   dag = DAG('readings',
             schedule_interval=dt.timedelta(days=1),
             start_date=dt.datetime(2019, 8, 1),
             catchup=True)

   t1 = PythonOperator(
       task_id='copy_readings',
       python_callable=copy_readings_with_args,
       provide_context=True,
       dag=dag)

Spatial ETL
^^^^^^^^^^^

No specific drivers are required for spatial data if they are
transferred as Well Known Text.

.. code:: python

   select_sql_oracle = """
       SELECT
         id,
         SDO_UTIL.TO_WKTGEOMETRY(geom)
       FROM src
       """

   insert_sql_postgis = """
       INSERT INTO dest (id, geom) VALUES (
         %s,
         ST_Transform(ST_GeomFromText(%s, 4326), 27700)
       )
       """

Other spatial operations e.g. coordinate transforms, intersections and
buffering can be carried out in the SQL. Transform functions can
manipulate geometries using the
`Shapely <https://pypi.org/project/Shapely/>`__ library.

Database to API / NoSQL copy ETL script template
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``etlhelper`` can be combined with Python’s
`aiohttp <https://docs.aiohttp.org/en/stable/>`__ library to create an
ETL for posting data from a database into an HTTP API. The API could be
a NoSQL document store (e.g. ElasticSearch, Cassandra) or some other web
service.

This example transfers data from Oracle to ElasticSearch. It uses
``iter_chunks`` to fetch data from the database without loading it all
into memory at once. A custom transform function creates a dictionary
structure from each row of data. This is “dumped” into JSON and posted
to the API via ``aiohttp``.

``aiohttp`` allows the records in each chunk to be posted to the API
asynchronously. The API is often the bottleneck in such pipelines and we
have seen significant speed increases (e.g. 10x) using asynchronous
transfer as opposed to posting records in series.

.. code:: python

   # copy_sensors_async.py
   import asyncio
   import datetime as dt
   import json
   import logging

   import aiohttp
   from etlhelper import iter_chunks

   from db import ORACLE_DB

   logger = logging.getLogger("copy_sensors_async")

   SELECT_SENSORS = """
       SELECT CODE, DESCRIPTION
       FROM BGS.DIC_SEN_SENSOR
       WHERE date_updated BETWEEN :startdate AND :enddate
       ORDER BY date_updated
       """
   BASE_URL = "http://localhost:9200/"
   HEADERS = {'Content-Type': 'application/json'}


   def copy_sensors(startdate, enddate):
       """Read sensors from Oracle and post to REST API."""
       logger.info("Copying sensors with timestamps from %s to %s",
                   startdate.isoformat(), enddate.isoformat())
       row_count = 0

       with ORACLE_DB.connect('ORACLE_PASSWORD') as conn:
           # chunks is a generator that yields lists of dictionaries
           chunks = iter_chunks(SELECT_SENSORS, conn,
                                parameters={"startdate": startdate,
                                            "enddate": enddate},
                                transform=transform_sensors)

           for chunk in chunks:
               result = asyncio.run(post_chunk(chunk))
               row_count += len(result)
               logger.info("%s items transferred", row_count)

       logger.info("Transfer complete")


   def transform_sensors(chunk):
       """Transform rows to dictionaries suitable for converting to JSON."""
       new_chunk = []

       for row in chunk:
           new_row = {
               'sample_code': row.CODE,
               'description': row.DESCRIPTION,
               'metadata': {
                   'source': 'ORACLE_DB',  # fixed value
                   'transferred_at': dt.datetime.now().isoformat()  # dynamic value
                   }
               }
           logger.debug(new_row)
           new_chunk.append(new_row)

       return new_chunk


   async def post_chunk(chunk):
       """Post multiple items to API asynchronously."""
       async with aiohttp.ClientSession() as session:
           # Build list of tasks
           tasks = []
           for item in chunk:
               tasks.append(post_one(item, session))

           # Process tasks in parallel.  An exception in any will be raised.
           result = await asyncio.gather(*tasks)

       return result


   async def post_one(item, session):
       """Post a single item to API using existing aiohttp Session."""
       # Post the item
       response = await session.post(BASE_URL + 'sensors/_doc', headers=HEADERS,
                                     data=json.dumps(item))

       # Log responses before throwing errors because error info is not included
       # in generated Exceptions and so cannot otherwise be seen for debugging.
       if response.status >= 400:
           response_text = await response.text()
           logger.error('The following item failed: %s\nError message:\n(%s)',
                        item, response_text)
           await response.raise_for_status()

       return response.status


   if __name__ == "__main__":
       # Configure logging
       handler = logging.StreamHandler()
       formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
       handler.setFormatter(formatter)
       logger.setLevel(logging.INFO)
       logger.addHandler(handler)

       # Copy data from 1 January 2000 to 00:00:00 today
       today = dt.datetime.combine(dt.date.today(), dt.time.min)
       copy_sensors(dt.datetime(2000, 1, 1), today)

In this example, failed rows will fail the whole job. Removing the
``raise_for_status()`` call will let them just be logged instead.

CSV load script template
^^^^^^^^^^^^^^^^^^^^^^^^

The following script is an example of using the ``load`` function to
import data from a CSV file into a database. It shows how a
``transform`` function can perform common parsing tasks such as renaming
columns and converting timestamps into datetime objects. The database
has a ``CHECK`` constraint that rejects any rows with an ID divisible by
1000. An example ``on_error`` function prints the IDs of rows that fail
to insert.

.. code:: python

   """
   Script to create database and load observations data from csv file. It also
   demonstrates how an `on_error` function can handle failed rows.

   Generate observations.csv with:
   curl 'https://sensors.bgs.ac.uk/FROST-Server/v1.1/Observations?$select=@iot.id,result,phenomenonTime&$top=20000&$resultFormat=csv' -o observations.csv
   """
   import csv
   import datetime as dt
   from typing import Iterable, List, Tuple

   from etlhelper import execute, load, DbParams


   def load_observations(csv_file, conn):
       """Load observations from csv_file to db_file."""
       # Drop table (helps with repeated test runs!)
       drop_table_sql = """
           DROP TABLE IF EXISTS observations
           """
       execute(drop_table_sql, conn)

       # Create table (reject ids with no remainder when divided by 1000)
       create_table_sql = """
           CREATE TABLE IF NOT EXISTS observations (
             id INTEGER PRIMARY KEY CHECK (id % 1000),
             time TIMESTAMP,
             result FLOAT
             )"""
       execute(create_table_sql, conn)

       # Load data
       with open(csv_file, 'rt') as f:
           reader = csv.DictReader(f)
           load('observations', conn, reader, transform=transform, on_error=on_error)


   # A transform function that takes an iterable of rows and returns an iterable
   # of rows.
   def transform(rows: Iterable[dict]) -> Iterable[dict]:
       """Rename time column and convert to Python datetime."""
       for row in rows:
           # Dictionaries are mutable, so rows can be modified in place.
           time_value = row.pop('phenomenonTime')
           row['time'] = dt.datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%S.%fZ")

       return rows


   # The on_error function is called after each chunk with all the failed rows
   def on_error(failed_rows: List[Tuple[dict, Exception]]) -> None:
       """Print the IDs of failed rows"""
       rows, exceptions = zip(*failed_rows)
       failed_ids = [row['id'] for row in rows]
       print(f"Failed IDs: {failed_ids}")


   if __name__ == "__main__":
       from etlhelper import log_to_console
       log_to_console()

       db = DbParams(dbtype="SQLITE", filename="observations.sqlite")
       with db.connect() as conn:
           load_observations('observations.csv', conn)

Export data to CSV
^^^^^^^^^^^^^^^^^^

The
`Pandas <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html>`__
library can connect to databases via SQLAlchemy. It has powerful tools
for manipulating tabular data. ETLHelper makes it easy to prepare the
SQL Alchemy connection.

.. code:: python

   import pandas as pd
   from sqlalchemy import create_engine

   from my_databases import ORACLEDB

   engine = create_engine(ORACLEDB.get_sqlalchemy_connection_string("ORACLE_PASSWORD"))

   sql = "SELECT * FROM my_table"
   df = pd.read_sql(sql, engine)
   df.to_csv('my_data.csv', header=True, index=False, float_format='%.3f')