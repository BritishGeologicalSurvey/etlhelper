## Developer setup

Note: [https://www.github.com/BritishGeologicalSurvey/etlhelper](https://www.github.com/BritishGeologicalSurvey/etlhelper) is a mirror of an internal repository.
Pull requests are applied there and then mirrored to GitHub.

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
setup_oracle_client
export "$(oracle_lib_path_export)"
```


### Running tests

Integration tests require access to a writer account on a PostGIS database.
The easiest way to run these is using a Docker container.

A Dockerised PostGIS version can be started with:

```bash
export TEST_PG_PORT=5432
docker run -e POSTGRES_USER=etlhelper_user -e POSTGRES_DB=etlhelper \
  -e POSTGRES_PASSWORD=etlhelper_pw --name etlhelper_postgis \
  -d --rm -p $TEST_PG_PORT:5432 mdillon/postgis:11-alpine
```

Tests are run with:

```bash
export TEST_PG_PASSWORD=etlhelper_pw
bash bin/run_tests_for_developer.sh
```

The test-runner script will run tests within a dedicated container and provide
HTML coverage output.  It can be viewed with `firefox htmlcov/index.html`.


#### Additional integration tests

There is a full suite of integration tests that also test Oracle and SQL Server
databases.  These were developed prior to publication of `etlhelper` are only
run within BGS agains internal databases.  They will be migrated to run in
Docker containers or against environment-variable-defined connections in the
future.


### Creating a new release

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
can then be created in the GitLab web interface (easiest), or via the API.

Pushing a tag will start the CI process to make a release.


#### Building distribution files

A source distribution file can be created and uploaded to a repository e.g.
PyPI.
It is created as follows:

```bash
python setup.py sdist --formats=zip
```

