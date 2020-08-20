# Contributing to etlhelper

## Roadmap

`etlhelper` is currently at Beta status.
All main features are complete, however contributions and suggestions are
welcome, particularly in the following areas:

+ support for more database types
+ additional recipes / case studies
+ performance optimisation
+ improved documentation

See the [issues list](https://github.com/BritishGeologicalSurvey/etlhelper/issues) for details.

#### Support for more database types

The `DbHelper` class provides a uniform interface to different database types.

Implementing a new one requires the following:

+ New DbHelper class for database
  - `__init__` method imports the driver and defines error types
  - `get_connection_string` and `get_sqlalchemy_connection_string` are defined
  - `required_parameters` defines set of names of parameters required by DbParams
  - other specific function overrides are in place (e.g. executemany for
    PostgreSQL)
+ DbHelper class is registered with DB_HELPER_FACTORY
+ Tests at `test/integration/db/test_xxx.py` that cover at least the `connect()` and
  `copy_rows()` functions
+ _Optional:_ `setup_xxx_driver.py` script to check driver installation

See [etlhelper/db_helpers/oracle.py](etlhelper/db_helpers/oracle.py) and
[test/integration/db/test_oracle.py](test/integration/db/test_oracle.py) for examples.


## Developer setup

Note: [https://www.github.com/BritishGeologicalSurvey/etlhelper](https://www.github.com/BritishGeologicalSurvey/etlhelper) is a mirror of an internal repository.
Pull requests are applied to BGS GitLab and then mirrored to GitHub.


### Prerequisites

+ Python 3.6+ virtual environment
+ Git
+ Docker


### Installation for development

Install locally for development by cloning repository and running the following
in the root:

```bash
pip install -r requirements.txt
pip install -e .
```

Development files will be linked to the virtual environment/system folder
hierarchy allowing changes to take affect directly.

Proprietary Oracle Instant Client drivers are required.
These can be installed with:

```bash
setup_oracle_client /path/or/url/to/instantclient-xxxx.zip
export "$(oracle_lib_path_export)"
```


### Running tests

Integration tests require access to a writer account on a PostGIS database.
The easiest way to run these is using a Docker container.

A Dockerised PostGIS version can be started with:

```bash
export TEST_PG_PORT=5432
export TEST_PG_PASSWORD=etlhelper_pw
docker run -e POSTGRES_USER=etlhelper_user -e POSTGRES_DB=etlhelper \
  -e POSTGRES_PASSWORD=$TEST_PG_PASSWORD --name etlhelper_postgis \
  -d --rm -p $TEST_PG_PORT:5432 mdillon/postgis:11-alpine
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

## Creating a new release

Releases are created manually from the main branch via tags.
This should be done via the GitHub web interface.
The GitLab CI system will automatically perform the release following the next
repository mirror synchronisation.

The instructions below explain how to do a manual release.

#### Tagging

`etlhelper` uses the [semantic versioning](https://semver.org/) notation.
We use the versioneer package to automatically manage version numbers. The
version numbers are set using the latest `git tag`. To set a new git tag,
and hence a new version number, run (for example):

```
git tag -a v0.3.2 -m "Version 0.3.2"
```

replacing the numbers with an appropriate version number. Then run:

```
git push --tags
```

to ensure the tag is pushed to the remote repository. Release notes
can then be created in the GitHub web interface (easiest), or via the API.


#### Upload to PyPI

A source distribution is created via `setup.py`.
Twine is used to upload to PyPI.  A PyPI account is required.

```bash
python setup.py sdist --formats=zip
twine upload dist/etlhelper-0.x.x.zip
```
