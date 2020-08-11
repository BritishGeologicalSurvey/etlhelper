# etlhelper

> etlhelper is a Python library to simplify data transfer between databases.

`etlhelper` provides a unified way to connect to different database types (currently Oracle, PostgreSQL, SQLite and MS SQL Server).
It is a thin wrapper around Python's [DBAPI2](https://www.python.org/dev/peps/pep-0249/) specification.
The `get_rows` function returns the result of a SQL query and can be used to create simple HTTP APIs.
The `copy_rows` function transfers data from one database to another.
It is possible to apply a transform function to manipulate data in flight.
These tools make it simple to create easy-to-understand, lightweight, versionable and testable Extract-Transform-Load (ETL) workflows.

`etlhelper` is not a tool for coordinating ETL jobs (use [Apache Airflow](https://airflow.apache.org)), for
converting GIS data formats (use [ogr2ogr](https://gdal.org/programs/ogr2ogr.html) or [fiona](https://pypi.org/project/Fiona/)) or an Object Relation Mapper (use [SQLAlchemy](https://www.sqlalchemy.org/)).
However, it can be used in conjunction with each of these.

For an introduction to `etlhelper`, see the FOSS4GUK 2019 presentation _Open Source Spatial ETL with Python and Apache Airflow_: [video](https://www.youtube.com/watch?v=12rzUW4ps74&feature=youtu.be&t=6238) (20 mins),
[slides](https://volcan01010.github.io/FOSS4G2019-talk).

 + [Installation](#installation)
 + [Quick Start](#quick-start)
 + [Recipes](#recipes)
 + [Development](#development)
 + [Reference](#reference)

## Installation

```bash
pip install etlhelper[oracle]
```

Required database drivers are specified in the square brackets.  Options are:

```
[oracle]
[mssql]
[postgres]
```

Multiple values can be separated by commas, e.g.: `[oracle,mssql]` would install both sets of drivers.
The `sqlite3` driver is included within Python's Standard Library.


### Dependencies

Linux systems require additional packages to be installed on the system.

Debian / Ubuntu:

  + `sudo apt install libaio1` for cxOracle.
  + `sudo apt install build-essential unixodbc-dev` for pyodbc.

Centos / Fedora:

  + `sudo yum install libaio` for Oracle
  + `sudo yum install gcc gcc-c++ make python36-devel pyodbc unixODBC-devel` for pyodbc


#### Oracle Instant Client

Oracle Instant Client libraries are required to connect to Oracle databases.
`etlhelper` provides a script to download and unzip them from the [Oracle website](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html).
Once the drivers are installed, a second step is required to add them to the
LD_LIBRARY_PATH so they can be used.
The required commands are:

```bash
setup_oracle_client
export "$(oracle_lib_path_export)"
```

See `setup_oracle_client --help` for further command line flags, including
specifying an alternative URL or filesystem path for the zipfile location.
If you are outside a virtual environment, the export command may be different.
See terminal output for details.

Run `setup_oracle_client` again to confirm setup has worked.


#### pyodbc for Microsoft SQL Server

The `setup_mssql_driver` tool checks that appropriate drivers are installed.

```bash
setup_mssql_driver
```

It provides links to installation instructions for drivers.
The [Dockerfile](Dockerfile) contains an example for Debian systems.


## Quick Start

#### Password Definition

Passwords (e.g. Oracle password) must be specified via an environment variable.
This can be done on the command line via:

+ `export ORACLE_PASSWORD=some-secret-password` on Linux
+ `set ORACLE_PASSWORD=some-secret-password` on Windows

Or in a Python terminal via:

```python
import os
os.environ['ORACLE_PASSWORD'] = 'some-secret-password'
```

No password is required for SQLite databases.


#### DbParams

Database connection information is defined by `DbParams` objects.

```
from etlhelper import DbParams

ORACLEDB = DbParams(dbtype='ORACLE', host="localhost", port=1521,
                    dbname="mydata", user="oracle_user")

POSTGRESDB = DbParams(dbtype='PG', host="localhost", port=5432,
                      dbname="mydata", user="postgres_user")

SQLITEDB = DbParams(dbtype='SQLITE', filename='/path/to/file.db')

MSSQLDB = DbParams(dbtype='MSSQL', host="localhost", port=5432,
                   dbname="mydata", user="mssql_user",
                   odbc_driver="ODBC Driver 17 for SQL Server")
```

DbParams objects can also be created from environment variables using the
`from_environment()` function.

DbParams objects have a function to check if it can connect to a database given its attributes. 
```
if not ORACLEDB.is_reachable():
    raise ETLHelperError("network problems")
```


#### Get rows

Connections are created by `connect` function.
The `get_rows` function returns a list of named tuples containing data as
native Python objects.

```python
from my_databases import ORACLEDB
from etlhelper import connect, get_rows

sql = "SELECT * FROM src"

with connect(ORACLEDB, "ORA_PASSWORD") as conn:
    result = get_rows(sql, conn)
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
`dump_rows` passes each row to a function (default is `print`), while `iter_rows`
returns a generator for looping over results.

#### Copy rows

Copy rows takes the results from a SELECT query and applies them as parameters
to an INSERT query.
The source and destination tables must already exist.

```python
from my_databases import POSTGRESDB, ORACLEDB
from etlhelper import connect, copy_rows

select_sql = "SELECT id, name FROM src"
insert_sql = "INSERT INTO dest (id, name)
              VALUES (%s, %s)"

src_conn = connect(ORACLEDB, "ORA_PASSWORD")
dest_conn = connect(POSTGRESDB, "PG_PASSWORD")

copy_rows(select_sql, src_conn, insert_sql, dest_conn)
```

#### Transform

Data can be transformed in-flight by applying a transform function.  This is
any Python callable (function) that takes an iterator (e.g. list) and returns
another iterator.

```python
import random

def my_transform(chunk):
    # Append random integer (1-10), filter if <5.

    new_chunk = []
    for row in chunk:
        external_value = random.randrange(10)
        if external_value >= 6:
            new_chunk.append((*row, external_value))

    return new_chunk

copy_rows(select_sql, src_conn, insert_sql, dest_conn
          transform=my_transform)
```

The above code demonstrates that the returned chunk can have a different number
of rows of different length.
The external data can result from a call to a webservice or other database.

The `iter_chunks` and `iter_rows` functions return generators.
Each chunk or row of data is only accessed when it is required.
Using `yield` instead of `return` in the transform function makes it
a generator, too.
Data transformation can then be performed via [memory-efficient iterator-chains](https://dbader.org/blog/python-iterator-chains).


#### Spatial ETL

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


#### ETL script example

The following is an example ETL script.

```python
from my_databases import ORACLEDB, POSTGRESDB
from etl_helper import connect, copy_rows

DELETE_SQL = "..."
SELECT_SQL = "..."
INSERT_SQL = "..."

def copy_src_to_dest():
    with connect(ORACLEDB, "ORA_PASSWORD") as src_conn:
        with connect(POSTGRESDB, "PG_PASSWORD") as dest_conn:
            execute(DELETE_SQL, dest_conn)
            copy_rows(SELECT_SQL, src_conn,
                      INSERT_SQL, dest_conn)

if __name__ == "__main__":
    copy_src_to_dest()
```

The DELETE_SQL command clears existing data prior to insertion.  This makes the
script idempotent.


## Recipes

`etlhelper` has other useful functions.


#### Logging progress

ETLHelper does not emit log messages by default.
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


#### Getting a SQLAlchemy engine

SQLAlchemy allows you to read/write data from [Pandas](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html).
It can be installed separately with `pip install sqlalchemy`.
For example, to export a CSV file of data:

```python
from my_databases import ORACLEDB
from etlhelper import get_sqlalchemy_connection_string
from sqlalchemy import create_engine

sqla_conn_str = get_sqlalchemy_connection_string(ORACLEDB, "ORACLE_PASSWORD")
engine = create_engine(sqla_conn_str)

sql = "SELECT * FROM my_table"
df = pd.read_sql(sql, engine)
df.to_csv('my_data.csv', header=True, index=False, float_format='%.3f')
```


#### Row factories

A row factory can be specified to change the output style.
For example, to return each row as a dictionary, use the following:

```python
from etlhelper import connect, iter_rows
from etlhelper.row_factories import dict_rowfactory

conn = connect(ORACLEDB, 'ORACLE_PASSWORD')
sql = "SELECT * FROM my_table"
for row in iter_rows(sql, conn, row_factory=dict_rowfactory):
    print(row['id'])
```

The `dict_rowfactory` is useful when getting data to be serialised
into JSON.
When combined with [Hug](http://pypi.org/project/hug), an HTTP API can be
created in fewer than 20 lines of code.


#### Insert rows

The `executemany` function can be used to insert data to the database.
Large datasets are broken into chunks and inserted in batches to reduce the
number of queries to the database that are required.

```python
from etlhelper import connect, executemany
 
rows = [(1, 'value'), (2, 'another value')]
insert_sql = "INSERT INTO some_table (col1, col2) VALUES (%s, %s)"

with connect(some_db, 'SOME_DB_PASSWORD') as conn:
    executemany(insert_sql, rows, conn)
```

## Development

### Maintainers

ETL Helper was created by and is maintained by British Geological Survey Informatics.

+ John A Stevenson ([volcan01010](https://github.com/volcan01010))
+ Jo Walsh ([metazool](https://github.com/metazool))
+ Declan Valters ([dvalters](https://github.com/dvalters))
+ Colin Blackburn ([ximenesuk](https://github.com/ximenesuk))

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to the
software.


### Licence

ETL Helper is distributed under the [LGPL v3.0 licence](LICENSE).
Copyright: © BGS / UKRI 2019


## References

+ [PEP249 DB API2](https://www.python.org/dev/peps/pep-0249/#cursor-objects)
+ [psycopg2](http://initd.org/psycopg/docs/cursor.html)
+ [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/cursor.html)
+ [pyodbc](https://pypi.org/project/pyodbc/)
