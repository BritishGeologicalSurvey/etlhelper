# etlhelper

> etlhelper is a Python ETL library to simplify data transfer into and out of databases.

> Note: There are a number of breaking changes planned for `etlhelper` version 1.0.
> Please pin the version number in your dependency list to avoid disruption and
> watch the project on GitHub for notification of new releases (in Custom section).


## Overview

`etlhelper` makes it easy to run a SQL query via Python and return the results.
It is built upon the [DBAPI2
specification](https://www.python.org/dev/peps/pep-0249/) and takes care of
importing drivers, formatting connection strings and cursor management.
This reduces the amount of boilerplate code required to query a relational
database with Python.

### Features

+ `setup_oracle_client` script installs Oracle Instant Client on Linux systems
+ `DbParams` objects provide consistent way to connect to different database types (currently Oracle, PostgreSQL, SQLite and MS SQL Server)
+ `get_rows`, `iter_rows`, `fetchone` and other functions for querying database
+ `execute`, `executemany`, and `load` functions to insert data
+ `copy_rows` and `copy_table_rows` to transfer data from one database to another
+ `on_error` function to process rows that fail to insert
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
 + [Utilities](#utilities)
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

Both versions accept additional keyword arguments that are passed to the `connect`
function of the underlying driver.  For example, the following sets the character
encoding used by cx_Oracle to ensure that values are returned as UTF-8:

```python
conn4 = connect(ORACLEDB, 'ORACLE_PASSWORD', encoding="UTF-8", nencoding="UTF8")
```

The above is a solution when special characters are scrambled in the returned data.

#### Disabling fast_executemany for SQL Server and other pyODBC connections

By default an `etlhelper` pyODBC connection uses a cursor with its
`fast_executemany` attribute set to `True`. This setting improves the
performance of the `executemany` when performing bulk inserts to a
SQL Server database. However, this overides the default behaviour
of pyODBC and there are some limitations in doing this. Importantly,
it is only recommended for applications that use Microsoft's ODBC Driver for
SQL Server. See [pyODBC fast_executemany](https://github.com/mkleehammer/pyodbc/wiki/Features-beyond-the-DB-API#fast_executemany).

Using `fast_executemany` may raise a `MemoryError` if query involves columns of types
`TEXT` and `NTEXT`, which are now deprecated.
Under these circumstances, `etlhelper` falls back on `fast_executemany` being set to
`False` and produces a warning output. See [Inserting into SQL server with
fast_executemany results in MemoryError](https://github.com/mkleehammer/pyodbc/issues/547).

If required, the `fast_executemany` attribute can be set to `False` via the
`connect` function:

```python
conn5 = connect(MSSQLDB, 'MSSQL_PASSWORD', fast_executemany=False)
```

This keyword argument is used by `etlhelper`, any further keyword arguments are
passed to the `connect` function of the underlying driver.

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
`dump_rows` passes each row to a function (default is `print`).


#### iter_rows

It is recommended to use `iter_rows` for looping over large result sets.  It
is a generator function that only yields data as requested.  This ensures that
the data are not all loaded into memory at once.

```
with ORACLEDB.connect("ORA_PASSWORD") as conn:
    for row in iter_rows(sql, conn):
        do_something(row)
```


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

For example return each row as a dictionary, use the following:

```python
from etlhelper import get_rows
from etlhelper.row_factories import dict_row_factory

sql = "SELECT * FROM my_table"

with ORACLEDB.connect('ORACLE_PASSWORD') as conn:
    for row in get_rows(sql, conn, row_factory=dict_row_factory):
        print(row['id'])
```

The `dict_row_factory` is useful when data are to be serialised to JSON/YAML,
as those formats use dictionaries as input.

Four different row_factories are included, based in built-in Python types:

|Row Factory|Attribute access|Mutable|Parameter placeholder|
|---|---|---|---|
|namedtuple_row_factory (default)| `row.id` or `row[0]` | No | Positional |
|dict_row_factory| `row["id"]`| Yes | Named |
|tuple_row_factory| `row[0]`| No | Positional |
|list_row_factory| `row[0]`| Yes | Positional |

The choice of row factory depends on the use case.  In general named tuples
and dictionaries are best for readable code, while using tuples or lists can
give a slight increase in performance.
Mutable rows are convenient when used with transform functions because they
can be modified without need to create a whole new output row.

When using `copy_rows`, it is necessary to use approriate parameter placeholder
style for the chosen row factory in the INSERT query.
Using the `dict_row_factory` requires a switch from named to positional
parameter placeholders (e.g. `%(id)s` instead of `%s` for PostgreSQL, `:id`
instead of `:1` for Oracle).
The `pyodbc` driver for MSSQL only supports positional placeholders.

When using the `load` function in conjuction with `iter_chunks` data must be
either named tuples or dictionaries.


#### Transform

The `transform` parameter allows passing of a function to transform the data
before returning it.
The function must take a list of rows and return a list of modified rows.
See `copy_rows` for more details.


#### Chunk size

All data extraction functions use `iter_chunks` behind the scenes.
This reads rows from the database in chunks to prevent them all being loaded
into memory at once.
The default `chunk_size` is 5000 and this can be set via keyword argument.


### Insert rows

`execute` can be used to insert a single row or to execute other single
statements e.g. "CREATE TABLE ...".
The `executemany` function is used to insert multiple rows of data.
Large datasets are broken into chunks and inserted in batches to reduce the
number of queries.
A tuple with counts of rows processed and failed is returned.

```python
from etlhelper import executemany

rows = [(1, 'value'), (2, 'another value')]
insert_sql = "INSERT INTO some_table (col1, col2) VALUES (%s, %s)"

with POSTGRESDB.connect('PGPASSWORD') as conn:
    processed, failed = executemany(insert_sql, conn, rows, chunk_size=1000)
```

The `chunk_size` default is 5,000 and it can be set with a keyword argument.
The `commit_chunks` flag defaults to `True`.
This ensures that an error during a large data transfer doesn't require all the
records to be sent again.
Some work may be required to determine which records remain to be sent.
Setting `commit_chunks` to `False` will roll back the entire transfer in case
of an error.

Some database engines can return autogenerated values (e.g. primary key IDs)
after INSERT statements.
To capture these values, use the `fetchone` method to execute the SQL command
instead.

```python
insert_sql = "INSERT INTO my_table (message) VALUES ('hello') RETURNING id"

with POSTGRESDB.connect('PGPASSWORD') as conn:
    result = fetchone(insert_sql, conn)

print(result.id)
```

The `load` function is similar to `executemany` except that it autogenerates
an insert query based on the data provided. It uses `generate_insert_query`
to remove the need to explicitly write the query for simple cases. By
calling this function manually, users can create a base insert query that can
be extended with clauses such as `ON CONFLICT DO NOTHING`.

As `generate_insert_query` creates SQL statements from user-provided input,
it checks the table and column names to ensure that they only contain valid
characters.


#### Handling insert errors

The default behaviour of `etlhelper` is to raise an exception on the first
error and abort the transfer.
Sometimes it is desirable to ignore the errors and to do something else with
the failed rows.
The `on_error` parameter allows a function to be passed that is applied to the
failed rows of each chunk.
The input is a list of (row, exception) tuples.

Different examples are given here.  The simplest approach is to collect all the
errors into a list to process at the end.

```python
errors = []
executemany(sql, conn, rows, on_error=errors.extend)

if errors:
    do_something()
```

Errors can be logged to the `etlhelper` logger.

```python
from etlhelper import logger

def log_errors(failed_rows):
    for row, exception in failed_rows:
        logger.error(exception)

executemany(sql, conn, rows, on_error=log_errors)
```

The IDs of failed rows can be written to a file.

```python
def write_bad_ids(failed_rows):
    with open('bad_ids.txt', 'at') as out_file:
        for row, exception in failed_rows:
            out_file.write(f"{row.id}\n")

executemany(sql, conn, rows, on_error=write_bad_ids)
```

`executemany`, `load`, `copy_rows` and `copy_table_rows` can all take an
`on_error` parameter.  They each return a tuple containing the number of rows
processed and the number of rows that failed.

### Copy table rows

`copy_table_rows` provides a simple way to copy all the data from one table to
another.
It can take a `transform` function in case some modification of the data, e.g.
change of case of column names, is required.

```python
from my_databases import POSTGRESDB, ORACLEDB
from etlhelper import copy_table_rows

with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
    with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
	copy_table_rows('my_table', src_conn, dest_conn)
```

The `chunk_size`, `commit_chunks` and `on_error` parameters can all be set.
A tuple with counts of rows processed and failed is returned.


### Combining `iter_rows` with `load`

For extra control selecting the data to be transferred, `iter_rows` can be
combined with `load`.

```python
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
```

### Copy rows

Customising both queries gives the greatest control on data selection and loading.
`copy_rows` takes the results from a SELECT query and applies them as parameters
to an INSERT query.
The source and destination tables must already exist.
For example, here we use GROUP BY and WHERE in the SELECT query and insert extra
auto-generated values via the INSERT query.

```python
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
```

`parameters` can be passed to the SELECT query as before and the
`commit_chunks`, `chunk_size` and `on_error` options can be set.

A tuple of rows processed and failed is returned.


### Transform

Data can be transformed in-flight by applying a transform function.  This is
any Python callable (e.g. function) that takes an iterator (e.g. list) and returns
another iterator.
Transform functions are applied to data as they are read from the database and
can be used with `get_rows`-type methods and with `copy_rows`.

The following code demonstrates that the returned chunk can have a different number
of rows, and be of different length, to the input.
When used with `copy_rows`, the INSERT query must contain the correct placeholders for the
transform result.
Extra data can result from a calculation, a call to a webservice or another database.

```python
import random

def my_transform(chunk):
    # Append random integer (1-10), filter if <5.

    new_chunk = []
    for row in chunk:  # each row is a namedtuple
        extra_value = random.randrange(10)
        if extra_value >= 5:
            new_chunk.append((*row, extra_value))

    return new_chunk

copy_rows(select_sql, src_conn, insert_sql, dest_conn,
          transform=my_transform)
```

It can be easier to modify individual columns when using the
`dict_row_factory` (see above).

```python
from etlhelper.row_factories import dict_row_factory

def my_transform(chunk):
    # Add prefix to id, remove newlines, set lower case email addresses

    new_chunk = []
    for row in chunk:  # each row is a dictionary
        row['id'] += 1000
        row['description'] = row['description'].replace('\n', ' ')
        row['email'] = row['email'].lower()
        new_chunk.append(row)

    return new_chunk

get_rows(select_sql, src_conn, row_factory=dict_row_factory,
         transform=my_transform)
```

The `iter_chunks` and `iter_rows` functions that are used internally return
generators.  Each chunk or row of data is only accessed when it is required.
The transform function can also be written to return a generator instead of
a list.  Data transformation can then be performed via [memory-efficient
iterator-chains](https://dbader.org/blog/python-iterator-chains).


### Aborting running jobs

When running as a script, `etlhelper` jobs can be stopped by pressing _CTRL-C_.
This option is not available when the job is running as a background process,
e.g. in a GUI application.
The `abort_etlhelper_threads()` function is provided to cancel jobs running in
a separate thread by raising an `ETLHelperAbort` exception within the thread.

The state of the data when the job is cancelled (or crashes) depends on the
arguments passed to `executemany` (or the functions that call it e.g. `load`,
`copy_rows`).

+ If `commit_chunks` is `True` (default), all chunks up to the one where the
  error occured are committed.
+ If `commit_chunks` is `False`, everything is rolled back and the database is
  unchanged.
+ If an `on_error` function is defined, all rows without errors are committed.


## Utilities

The following utility functions provide useful database metadata.


### Table info


The `table_info` function provides basic metadata for a table. An optional schema
can be used. Note that for `sqlite` the schema value is currently ignored.

```python
from etlhelper.utils import table_info

with ORACLEDB.connect("ORA_PASSWORD") as conn:
    columns = table_info('my_table', conn, schema='my_schema')
```

The returned value is a list of named tuples of four values. Each tuple represents
one column in the table, giving its name, type, if it has a NOT NULL constraint
and if is has a DEFAULT value constraint. For example,

```python
[
    Column(name='ID', type='NUMBER', not_null=1, has_default=0),
    Column(name='VALUE', type='VARCHAR2', not_null=0, has_default=1),
]
```

the ID column is of type NUMBER and has a NOT NULL constraint but not a DEFAULT value,
while the VALUE column is of type VARCHAR2, can be NULL but does have a DEFAULT value.


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


### Database to database copy ETL script template

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
      ST_Transform(ST_GeomFromText(%s, 4326), 27700)
    )
    """
```

Other spatial operations e.g. coordinate transforms, intersections and
buffering can be carried out in the SQL.
Transform functions can manipulate geometries using the [Shapely](https://pypi.org/project/Shapely/) library.


### Database to API / NoSQL copy ETL script template

`etlhelper` can be combined with Python's
[aiohttp](https://docs.aiohttp.org/en/stable/) library to create an ETL
for posting data from a database into an HTTP API.
The API could be a NoSQL document store (e.g. ElasticSearch, Cassandra) or some other
web service.

This example transfers data from Oracle to ElasticSearch.
It uses `iter_chunks` to fetch data from the database without loading it all into
memory at once.
A custom transform function creates a dictionary structure from each row
of data.
This is "dumped" into JSON and posted to the API via `aiohttp`.

`aiohttp` allows the records in each chunk to be posted to the API
asynchronously.
The API is often the bottleneck in such pipelines and we have seen significant
speed increases (e.g. 10x) using asynchronous transfer as opposed to posting
records in series.


```python
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
```

In this example, failed rows will fail the whole job.  Removing the
`raise_for_status()` call will let them just be logged instead.


### CSV load script template

The following script is an example of using the `load` function to import data
from a CSV file into a database.
It shows how a `transform` function can perform common parsing tasks such as
renaming columns and converting timestamps into datetime objects.
The database has a `CHECK` constraint that rejects any rows with an ID
divisible by 1000.
An example `on_error` function prints the IDs of rows that fail to insert.

```python
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
        load('observations', conn, transform(reader), on_error=on_error)


# The on_error function is called after each chunk with all the failed rows
def on_error(failed_rows: List[Tuple[dict, Exception]]) -> None:
    """Print the IDs of failed rows"""
    rows, exceptions = zip(*failed_rows)
    failed_ids = [row['id'] for row in rows]
    print(f"Failed IDs: {failed_ids}")


# A transform function that takes an iterable and yields one row at a time
# returns a "generator".  The generator is also iterable, and records are
# processed as they are read so the whole file is never held in memory.
def transform(rows: Iterable[dict]) -> Iterable[dict]:
    """Rename time column and convert to Python datetime."""
    for row in rows:
        row['time'] = row.pop('phenomenonTime')
        row['time'] = dt.datetime.strptime(row['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
        yield row


if __name__ == "__main__":
    import logging
    from etlhelper import logger
    logger.setLevel(logging.INFO)

    db = DbParams(dbtype="SQLITE", filename="observations.sqlite")
    with db.connect() as conn:
        load_observations('observations.csv', conn)
```


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
