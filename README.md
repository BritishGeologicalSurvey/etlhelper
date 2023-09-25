# etlhelper

> etlhelper is a Python ETL library to simplify data transfer into and out of databases.

`etlhelper` makes it easy to run a SQL queries via Python and return the results.
It is built upon the [DBAPI2
specification](https://www.python.org/dev/peps/pep-0249/) and takes care of cursor management, importing drivers and formatting connection strings, while providing memory-efficient functions to read, write and transform data.
This reduces the amount of boilerplate code required to manipulate data within relational
databases with Python.

---

+ **Documentation**: https://britishgeologicalsurvey.github.io/etlhelper/
+ **Source code**: https://github.com/BritishGeologicalSurvey/etlhelper

---

## Version 1.0 coming soon!

The code for ETL Helper version 1.0 has now been merged into `main`, although not yet deployed to PyPI.
It contains many breaking changes.
The [documentation pages](https://britishgeologicalsurvey.github.io/etlhelper/) now correspond to the upcoming version 1.0.
To use this now, install directly from GitHub:

```bash
pip install git+https://github.com/BritishGeologicalSurvey/etlhelper.git@main
```


### Draft release notes for v1

ETL Helper version 1.0 contains many breaking changes and new ways of working.

#### Breaking changes

+ The `cxOracle` driver has been replaced with the [python-oracledb](https://oracle.github.io/python-oracledb/) driver.  `python-oracledb` does not depend on Oracle Instant Client and all related code e.g., `setup_oracle_client`, has been removed as it is no longer required.  This is a great sign of progress, as one of the initial purposes of ETL Helper was to give an easy way to jump through the hoops of installing Oracle Instant Client in CI pipelines.  The `read_lob` flag has been deprecated as part of this change.  See the Oracle driver documentation for details.
+ The default row factory has been changed from `namedtuple_row_factory`, to `dict_row_factory`.  This is because dictionaries are mutable and so easier to use in transform functions.  They are also more widely understood.  To recreate the current behaviour in existing scripts, set `row_factory=namedtuple_row_factory`.
+ `get_rows` is deprecated; use `fetchall` instead.  This does the same thing, but is better aligned with the DB API 2.0.
+ `fetchmany` is deprecated; use `iter_chunks` instead.  The behaviour of ETL Helper's `fetchmany` was different to `cursor.fetchmany` as defined by the DB API 2.0 and this was confusing.  It is also possible to use `chunks = etl.iter_chunks(...); do_something(next(chunk))`.
+ `dump_rows` is deprecated; use `for row in iter_rows(...); my_func(row)` instead.  
+ The ETL Helper logger is no longer given a handler by default.  This is the correct behaviour for a library.  Activate logging with `etl.log_to_console()`.
+ The minimum supported Python version is 3.9.  This allows us to benefit from security and typing improvements.

#### Non-breaking changes

+ There is a new Sphinx-based documentation webpage at https://britishgeologicalsurvey.github.io/etlhelper.
+ There is a new preferred syntax, using `import etlhelper as etl` to provide easy access to the public functions.
+ `executemany` and `load` functions can now take a transform function.
+ Type hints have been added to the ETL functions
+ The build system uses `pyproject.toml` instead of `setup.py` and `versioneer`.
+ The CI pipeline has moved from Travis to GitHub actions
+ The CI pipeline runs the PostgreSQL integration tests (which cover the bulk of the application logic)



## Installation

ETL Helper is available on PyPI (version 0.14.3).
Documentation for the most recent version on PyPI is [here](https://github.com/BritishGeologicalSurvey/etlhelper/tree/v0.14.3#readme).

```bash
pip install etlhelper
```

Database driver packages are not included by default and should be specified in square brackets. Options are oracle (installs oracledb), mssql (installs pyodbc) and postgres (installs psycopg2). Multiple values can be separated by commas.

```bash
pip install etlhelper[oracle,postgres]
```

See the individual database driver configuration pages for any OS-level dependencies.


TODO: Add simple example script.



## Development

ETL Helper was created by and is maintained by British Geological Survey Informatics.

+ John A Stevenson ([volcan01010](https://github.com/volcan01010))
+ Jo Walsh ([metazool](https://github.com/metazool))
+ Declan Valters ([dvalters](https://github.com/dvalters))
+ Colin Blackburn ([ximenesuk](https://github.com/ximenesuk))
+ Daniel Sutton ([kerberpolis](https://github.com/kerberpolis))
+ Leo Rudczenko ([leorudczenko](https://github.com/leorudczenko))

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute.

## Licence

ETL Helper is distributed under the [LGPL v3.0 licence](LICENSE).
Copyright: © BGS / UKRI 2019
