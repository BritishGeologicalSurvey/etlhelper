
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
for manipulating tabular data. ETL Helper makes it easy to prepare the
SQL Alchemy connection.

.. code:: python

   import pandas as pd
   from sqlalchemy import create_engine

   from my_databases import ORACLEDB

   engine = create_engine(ORACLEDB.get_sqlalchemy_connection_string("ORACLE_PASSWORD"))

   sql = "SELECT * FROM my_table"
   df = pd.read_sql(sql, engine)
   df.to_csv('my_data.csv', header=True, index=False, float_format='%.3f')
