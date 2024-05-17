# etlhelper

> etlhelper is a Python ETL library to simplify data transfer into and out of databases.

ETL Helper makes it easy to run SQL queries via Python and return the
results.
It takes care of cursor management, importing drivers and formatting connection strings,
while providing memory-efficient functions to read, write and transform data.
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
Specify a [release candidate tag](https://github.com/BritishGeologicalSurvey/etlhelper/tags) or use `--pre` flag with `pip` to install pre-release versions.

## Installation

ETL Helper is available on PyPI (version 0.14.3).
Documentation for v0.14.3 is [here](https://github.com/BritishGeologicalSurvey/etlhelper/tree/v0.14.3#readme).

```bash
pip install etlhelper
```

Database driver packages are not included by default and should be specified in square brackets. Options are oracle (installs oracledb), mssql (installs pyodbc) and postgres (installs psycopg2). Multiple values can be separated by commas.

```bash
pip install etlhelper[oracle,postgres]
```

See the individual database driver configuration pages for any OS-level dependencies.


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
