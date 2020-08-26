# etlhelper

> etlhelper is a Python library to simplify data transfer between databases.

## Overview

`etlhelper` makes it easy to run a SQL query via Python and return the results.
It is built upon the [DBAPI2
specification](https://www.python.org/dev/peps/pep-0249/) and takes care of
importing drivers, formatting connection strings and cursor management.
This avoids repeating such "boilerplate" code (with subtle variations) across
each Python program that interacts with a relational database.

### Features

+ `setup_oracle_client` script installs Oracle Instant Client on Linux systems
+ `DbParams` objects provide consistent way to connect to different database types (currently Oracle, PostgreSQL, SQLite and MS SQL Server)
+ `get_rows`, `iter_rows`, `fetchone` and other functions for querying database
+ `execute` and `executemany` functions to insert data
+ `copy_rows` to transfer data from one database to another
+ Support for parameterised queries and in-flight transformation of data
+ Output results as namedtuple or dictionary
+ Timestamped log messages for tracking long-running data transfers
+ Helpful error messages display the failed query SQL

These tools can create easy-to-understand, lightweight, versionable and testable Extract-Transform-Load (ETL) workflows.
`etlhelper` is not a tool for coordinating ETL jobs (use [Apache Airflow](https://airflow.apache.org)), for
converting GIS data formats (use [ogr2ogr](https://gdal.org/programs/ogr2ogr.html) or [fiona](https://pypi.org/project/Fiona/)), for translating between SQL dialects or providing Object Relation Mapping (use [SQLAlchemy](https://www.sqlalchemy.org/)).
However, it can be used in conjunction with each of these.

![screencast](https://github.com/BritishGeologicalSurvey/etlhelper/blob/main/docs/screencast.gif?raw=true)

The documentation below explains how the main features are used.
See the individual function docstrings for full details of parameters and
options.

For a high level introduction to `etlhelper`, see the FOSS4GUK 2019 presentation _Open Source Spatial ETL with Python and Apache Airflow_: [video](https://www.youtube.com/watch?v=12rzUW4ps74&feature=youtu.be&t=6238) (20 mins),
[slides](https://volcan01010.github.io/FOSS4G2019-talk).


### Documentation

 + [Installation](#installation)
 + [Connect to databases](#connect-to-databases)
 + [Transfer data](#transfer-data)
 + [Recipes](#recipes)
 + [Development](#development)
 + [References](#references)


## Installation

### Python packages

```bash
pip install etlhelper
```

Database driver packages are not included by default and should be specified in
square brackets.
Options are `oracle` (installs cx_Oracle), `mssql` (installs pyodbc) and `postgres` (installs psycopg2).
Multiple values can be separated by commas.

```
pip install etlhelper[oracle,postgres]
```

The `sqlite3` driver is included within Python's Standard Library.


### Database driver dependencies

Some database drivers have additional dependencies.
On Linux, these can be installed via the system package manager.

cx_Oracle (for Oracle):

+ `sudo apt install libaio1` (Debian/Ubuntu) or `sudo dnf install libaio`
  (CentOS, RHEL, Fedora)

pyodbc (for MS SQL Server):

+ Follow instructions on [Microsoft SQL Docs website](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017)


### Oracle Instant Client

Oracle Instant Client libraries are required to connect to Oracle databases.
On Linux, `etlhelper` provides a script to download and unzip them from the [Oracle
website](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html).
Once the drivers are installed, their location must be added to LD_LIBRARY_PATH
environment variable before they can be used.  `setup_oracle_client` writes
a file that can then be "sourced" to do this for the current shell.  These two steps
can be executed in a single command as:

```bash
source $(setup_oracle_client)
```

This command must be run in each new shell session.
See `setup_oracle_client --help` for further command line flags, including
specifying an alternative URL or filesystem path for the zipfile location.


## Connect to databases

### DbParams

Database connection details are defined by `DbParams` objects.
Connections are made via their `connect` functions (see below).
`DbParams` objects are created as follows or from environment variables using the
`from_environment()` function.
The class initialisation function checks that the correct attributes have been provided for
a given `dbtype`.

```python
from etlhelper import DbParams

ORACLEDB = DbParams(dbtype='ORACLE', host="localhost", port=1521,
                    dbname="mydata", user="oracle_user")

POSTGRESDB = DbParams(dbtype='PG', host="localhost", port=5432,
                      dbname="mydata", user="postgres_user")

SQLITEDB = DbParams(dbtype='SQLITE', filename='/path/to/file.db')

MSSQLDB = DbParams(dbtype='MSSQL', host="localhost", port=1433,
                   dbname="mydata", user="mssql_user",
                   odbc_driver="ODBC Driver 17 for SQL Server")
```

DbParams objects have a function to check if a given database can be reached
over the network.  This does not require a username or password.

```python
if not ORACLEDB.is_reachable():
    raise ETLHelperError("Network problems")
```

Other methods/properties are `get_connection_string`,
`get_sqlalchemy_connection_string`, `paramstyle` and `copy`.
See function docstrings for details.

###  `connect` function

The `DbParams.connect()` function returns a DBAPI2 connection as provided by the
underlying driver.
Using context-manager syntax as below ensures that the connection is closed
after use.

```python
with SQLITEDB.connect() as conn1:
    with POSTGRESDB.connect('PGPASSWORD') as conn2:
        do_something()
```

A standalone `connect` function provides backwards-compatibility with
previous releases of `etlhelper`:

```python
from etlhelper import connect
conn3 = connect(ORACLEDB, 'ORACLE_PASSWORD')
```

### Passwords

Database passwords must be specified via an environment variable.
This reduces the temptation to store them within scripts.
This can be done on the command line via:

+ `export ORACLE_PASSWORD=some-secret-password` on Linux
+ `set ORACLE_PASSWORD=some-secret-password` on Windows

Or in a Python terminal via:

```python
import os
os.environ['ORACLE_PASSWORD'] = 'some-secret-password'
```

No password is required for SQLite databases.


## Transfer data

### Get rows

The `get_rows` function returns a list of named tuples containing data as
native Python objects.

```python
from my_databases import ORACLEDB
from etlhelper import get_rows

sql = "SELECT * FROM src"

with ORACLEDB.connect("ORA_PASSWORD") as conn:
    get_rows(sql, conn)
```

returns

```
[Row(id=1, value=1.234, simple_text='text', utf8_text='Öæ°\nz',
     day=datetime.date(2018, 12, 7),
     date_time=datetime.datetime(2018, 12, 7, 13, 1, 59)),
 Row(id=2, value=2.234, simple_text='text', utf8_text='Öæ°\nz',
     day=datetime.date(2018, 12, 8),
     date_time=datetime.datetime(2018, 12, 8, 13, 1, 59)),
 Row(id=3, value=2.234, simple_text='text', utf8_text='Öæ°\nz',
     day=datetime.date(2018, 12, 9),
     date_time=datetime.datetime(2018, 12, 9, 13, 1, 59))]
```

Data are accessible via index (`row[4]`) or name (`row.day`).

Other functions are provided to select data.  `fetchone`, `fetchmany` and
`fetchall` are equivalent to the cursor methods specified in the DBAPI v2.0.
`dump_rows` passes each row to a function (default is `print`).  `iter_rows`
returns a generator for looping over results.  This is recommended for large
result sets as it ensures that they are not all loaded into memory at once.


#### Parameters

Variables can be inserted into queries by passing them as parameters.
These "bind variables" are sanitised by the underlying drivers to prevent [SQL
injection attacks](https://xkcd.com/327/).
The required [paramstyle](https://www.python.org/dev/peps/pep-0249/#paramstyle)
can be checked with `MY_DB.paramstyle`.
A tuple is used for positional placeholders, or a dictionary for named
placeholders.

```python
select_sql = "SELECT * FROM src WHERE id = :id"

with ORACLEDB.connect("ORA_PASSWORD") as conn:
    get_rows(sql, conn, parameters={'id': 1})
```

#### Row factories

Row factories control the output format of returned rows.
To return each row as a dictionary, use the following:

```python
from etlhelper import get_rows
from etlhelper.row_factories import dict_row_factory

sql = "SELECT * FROM my_table"

with ORACLEDB.connect('ORACLE_PASSWORD') as conn:
    for row in get_rows(sql, conn, row_factory=dict_row_factory):
        print(row['id'])
```

The `dict_row_factory` is required with `copy_rows` when using named placeholders for the INSERT query.
It is also useful when data are to be serialised to JSON.


### Insert rows

`execute` can be used to insert a single row or to execute other single
statements e.g. "CREATE TABLE ...".
The `executemany` function is used to insert multiple rows of data.
Large datasets are broken into chunks and inserted in batches to reduce the
number of queries.

```python
from etlhelper import executemany

rows = [(1, 'value'), (2, 'another value')]
insert_sql = "INSERT INTO some_table (col1, col2) VALUES (%s, %s)"

with POSTGRESDB.connect('PGPASSWORD') as conn:
    executemany(insert_sql, conn, rows)
```

The `commit_chunks` flag defaults to `True`.
This ensures that an error during a large data transfer doesn't require all the
records to be sent again.
Some work may be required to determine which records remain to be sent.
Setting `commit_chunks` to `False` will roll back the entire transfer in case
of an error.


### Copy rows

Copy rows takes the results from a SELECT query and applies them as parameters
to an INSERT query.
The source and destination tables must already exist.

```python
from my_databases import POSTGRESDB, ORACLEDB
from etlhelper import copy_rows

select_sql = "SELECT id, name FROM src"
insert_sql = "INSERT INTO dest (id, name)
              VALUES (%s, %s)"

src_conn = ORACLEDB.connect("ORA_PASSWORD")
dest_conn = POSTGRESDB.connect("PG_PASSWORD")

copy_rows(select_sql, src_conn, insert_sql, dest_conn)
```

`parameters` can be passed to the SELECT query as before and the
`commit_chunks` flag can be set.


### Transform

Data can be transformed in-flight by applying a transform function.  This is
any Python callable (e.g. function) that takes an iterator (e.g. list) and returns
another iterator.
Transform functions are applied to data as they are read from the database and
can be used with `get_rows`-type methods and with `copy_rows`.

```python
import random

def my_transform(chunk):
    # Append random integer (1-10), filter if <5.

    new_chunk = []
    for row in chunk:
        extra_value = random.randrange(10)
        if extra_value >= 5:
            new_chunk.append((*row, extra_value))

    return new_chunk

copy_rows(select_sql, src_conn, insert_sql, dest_conn
          transform=my_transform)
```

The above code demonstrates that the returned chunk can have a different number
of rows, and of different length, to the input.
When used with `copy_rows`, the INSERT query must contain the correct placeholders for the
transform result.
Extra data can result from a calculation, a call to a webservice or another database.

The `iter_chunks` and `iter_rows` functions used internally return generators.
Each chunk or row of data is only accessed when it is required.
The transform function can also be written to return a generator instead of
a list.
Data transformation can then be performed via [memory-efficient iterator-chains](https://dbader.org/blog/python-iterator-chains).

## Recipes


The following recipes demonstrate how `etlhelper` can be used.


### Debug SQL and monitor progress with logging

ETL Helper provides a custom logging handler.
Time-stamped messages indicating the number of rows processed can be enabled by
setting the log level to INFO.
Setting the level to DEBUG provides information on the query that was run,
example data and the database connection.

```python
import logging
from etlhelper import logger

logger.setLevel(logging.INFO)
```

Output from a call to `copy_rows` will look like:

```
2019-10-07 15:06:22,411 iter_chunks: Fetching rows
2019-10-07 15:06:22,413 executemany: 1 rows processed
2019-10-07 15:06:22,416 executemany: 2 rows processed
2019-10-07 15:06:22,419 executemany: 3 rows processed
2019-10-07 15:06:22,420 iter_chunks: 3 rows returned
2019-10-07 15:06:22,420 executemany: 3 rows processed in total
```

Note: errors on database connections output messages that include login
credentials in clear text.


### ETL script template

The following is a template for an ETL script.
It copies copy all the sensor readings from the previous day from an Oracle
source to PostgreSQL destination.

```python
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
    WHERE time_stamp >= %(start)s
      AND time_stamp < %(end)s
    """).strip()

SELECT_SQL = dedent("""
    SELECT id, measure_id, time_stamp, reading
    FROM sensor_data
    WHERE time_stamp >= TO_DATE(:start, 'YYYY-MM-DD HH24:MI:SS')
      AND time_stamp < TO_DATE(:end, 'YYYY-MM-DD HH24:MI:SS')
    ORDER BY time_stamp
    """).strip()

INSERT_SQL = dedent("""
    INSERT INTO sensordata.readings (sensor_data_id, measure_id, time_stamp,
      meas_value)
    VALUES (%s, %s, %s, %s)
    """).strip()


def copy_readings(startdate, enddate):
    params = {'start': start, 'end': end}

    with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
        with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
            execute(CREATE_SQL dest_conn)
            execute(DELETE_SQL, dest_conn, parameters=params)
            copy_rows(SELECT_SQL, src_conn,
                      INSERT_SQL, dest_conn,
                      parameters=params)


if __name__ == "__main__":
    # Copy data from 00:00:00 yesterday to 00:00:00 today
    today = dt.combine(dt.date.today, dt.time.min)
    yesterday = today - dt.timedelta(1)

    copy_readings(yesterday, today)
```

It is valuable to create [idempotent](https://stackoverflow.com/questions/1077412/what-is-an-idempotent-operation) scripts to ensure that they can be rerun without problems.
In this example, the "CREATE TABLE IF NOT EXISTS" command can be called repeatedly.
The DELETE_SQL command clears existing data prior to insertion to prevent duplicate key errors.
SQL syntax such as "INSERT OR UPDATE", "UPSERT" or "INSERT ... ON CONFLICT" may be more efficient, but the the exact commands depend on the target database type.


### Calling ETL Helper scripts from Apache Airflow

The following is an [Apache Airflow
DAG](https://airflow.apache.org/docs/stable/concepts.html) that uses the `copy_readings` function
defined in the script above.
The Airflow scheduler will create tasks for each day since 1 August 2019 and
call `copy_readings` with the appropriate start and end times.

```python
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
```


### Spatial ETL

No specific drivers are required for spatial data if they are transferred as
Well Known Text.

```python
select_sql_oracle = """
    SELECT
      id,
      SDO_UTIL.TO_WKTGEOMETRY(geom)
    FROM src
    """

insert_sql_postgis = """
    INSERT INTO dest (id, geom) VALUES (
      %s,
      ST_Transform(ST_GeomFromWKT(%s), 27700)
    )
    """
```

Other spatial operations e.g. coordinate transforms, intersections and
buffering can be carried out in the SQL.
Transform functions can manipulate geometries using the [Shapely](https://pypi.org/project/Shapely/) library.


### Export data to CSV

The [Pandas](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html) library can connect to databases via SQLAlchemy.
It has powerful tools for manipulating tabular data.
ETL Helper makes it easy to prepare the SQL Alchemy connection.

```python
import pandas as pd
from sqlalchemy import create_engine

from my_databases import ORACLEDB

engine = create_engine(ORACLEDB.get_sqlalchemy_connection_string("ORACLE_PASSWORD"))

sql = "SELECT * FROM my_table"
df = pd.read_sql(sql, engine)
df.to_csv('my_data.csv', header=True, index=False, float_format='%.3f')
```


## Development

### Maintainers

ETL Helper was created by and is maintained by British Geological Survey Informatics.

+ John A Stevenson ([volcan01010](https://github.com/volcan01010))
+ Jo Walsh ([metazool](https://github.com/metazool))
+ Declan Valters ([dvalters](https://github.com/dvalters))
+ Colin Blackburn ([ximenesuk](https://github.com/ximenesuk))
+ Daniel Sutton ([kerberpolis](https://github.com/kerberpolis))

### Development status

The code is still under active development and breaking changes are possible.
Users should pin the version in their dependency lists and
[watch](https://docs.github.com/en/github/managing-subscriptions-and-notifications-on-github/viewing-your-subscriptions#configuring-your-watch-settings-for-an-individual-repository)
the repository for new releases.
See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute.


### Licence

ETL Helper is distributed under the [LGPL v3.0 licence](LICENSE).
Copyright: © BGS / UKRI 2019


## References

+ [PEP249 DB API2](https://www.python.org/dev/peps/pep-0249/#cursor-objects)
+ [psycopg2](http://initd.org/psycopg/docs/cursor.html)
+ [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/cursor.html)
+ [pyodbc](https://pypi.org/project/pyodbc/)
+ [sqlite3](https://docs.python.org/3/library/sqlite3.html)
