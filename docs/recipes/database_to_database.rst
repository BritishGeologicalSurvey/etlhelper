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
