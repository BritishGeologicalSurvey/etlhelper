# Contributing to etlhelper

## Roadmap

`etlhelper` is currently at Beta status.
All main features are complete, however contributions and suggestions are
welcome, particularly in the following areas:

+ support for more database types (MySQL, IBM Informix)
+ additional recipes / case studies
+ improved type annotations
+ run more integration tests in GitHub Actions
+ performance optimisation
+ improved documentation

See the [issues list](https://github.com/BritishGeologicalSurvey/etlhelper/issues) for details.

#### Support for more database types

The `DbHelper` class provides a uniform interface to different database types.

Implementing a new `DbHelper` requires the following:

+ New DbHelper class for database
  - `__init__` method imports the driver and defines error types
  - `get_connection_string` and `get_sqlalchemy_connection_string` are defined
  - `required_parameters` defines set of names of parameters required by DbParams
  - `paramstyle` attribute
  - other specific function overrides are in place (e.g. `executemany` for
    PostgreSQL, `connect` for SQLite.)
+ DbHelper class is registered with DB_HELPER_FACTORY
+ Tests at `test/integration/db/test_xxx.py` that cover at least the `connect()` and
  `copy_rows()` functions

See [etlhelper/db_helpers/oracle.py](etlhelper/db_helpers/oracle.py) and
[test/integration/db/test_oracle.py](test/integration/db/test_oracle.py) for examples.


## Developer setup

[https://www.github.com/BritishGeologicalSurvey/etlhelper](https://www.github.com/BritishGeologicalSurvey/etlhelper) is a the source-of-truth copy of ETL Helper.
There is a mirror on the internal BGS GitLab server.
It is used to run integration tests against internal Oracle and MS SQL
databases.
Pull requests and issues should be targeted at the GitHub repository.


### Prerequisites

+ Python 3.9+ virtual environment
+ Git
+ Docker


### Installation for development

Install locally for development by cloning repository and running the following
in the root:

```bash
pip install -r requirements-dev.txt
pip install -e .
```

Development files will be linked to the virtual environment/system folder
hierarchy allowing changes to take effect directly.


### Running tests

Integration tests require access to a writer account on a PostGIS database.
The easiest way to run these is using a Docker container.

A Dockerised PostGIS version can be started with:

```bash
export TEST_PG_PORT=5432
export TEST_PG_PASSWORD=etlhelper_pw
docker run -e POSTGRES_USER=etlhelper_user -e POSTGRES_DB=etlhelper \
  -e POSTGRES_PASSWORD=$TEST_PG_PASSWORD --name etlhelper_postgis \
  -d --rm -p $TEST_PG_PORT:5432 postgis/postgis:15-3.4
```

Tests are run with:

```bash
bash bin/run_tests_for_developer.sh
```

The test-runner script will run tests within a dedicated container and provide
HTML coverage output.  It can be viewed with `firefox htmlcov/index.html`.

#### Running additional BGS integration tests

Additional integration tests can be run against internal BGS Oracle and SQL Server
databases.
The DbParams for these databases are defined by environment variables stored
within the Continuous Integration system.
To run these:

+ Go to Settings > CI / CD > Variables in GitLab
+ Use "Reveal values" and copy the contents of TEST_ENV_VARS
+ Paste into the terminal to set environment variables
+ Run tests as before

### Building documentation

The documentation is created using Sphinx.
To locally build the documentation, run the following:
 
```bash
sphinx-build -a docs docs/_build
```

The documentation can then be viewed at `docs/_build/index.html`


## Creating a new release

Releases are created manually from the main branch via tags.
This should be done via the GitHub web interface.
The GitLab CI system will automatically perform the release following the next
repository mirror synchronisation.
