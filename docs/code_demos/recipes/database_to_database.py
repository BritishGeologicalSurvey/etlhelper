"""ETL Helper script to demonstrate copying data from an Oracle database into a PostgreSQL database."""
import datetime as dt
from textwrap import dedent
import etlhelper as etl
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


def copy_readings(startdate: dt.datetime, enddate: dt.datetime) -> None:
    params = {"startdate": startdate, "enddate": enddate}

    with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
        with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
            etl.execute(CREATE_SQL, dest_conn)
            etl.execute(DELETE_SQL, dest_conn, parameters=params)
            etl.copy_rows(
                SELECT_SQL,
                src_conn,
                INSERT_SQL,
                dest_conn,
                parameters=params,
            )


if __name__ == "__main__":
    # Copy data from 00:00:00 yesterday to 00:00:00 today
    today = dt.combine(dt.date.today(), dt.time.min)
    yesterday = today - dt.timedelta(1)

    copy_readings(yesterday, today)
