Welcome to ETLHelper's documentation!
=====================================

.. image:: https://img.shields.io/pypi/v/etlhelper?label=Current%20Release
   :target: https://pypi.org/project/etlhelper
.. image:: https://img.shields.io/pypi/dm/etlhelper?label=Downloads%20pypi

ETLHelper is a Python ETL library to simplify data transfer into and out of databases.

.. note::  There are a number of breaking changes planned for
   ``etlhelper`` version 1.0. Please pin the version number in your
   dependency list to avoid disruption and watch the project on GitHub
   for notification of new releases.

.. toctree::
   :maxdepth: 2
   :caption: Table of contents

   installation
   etl_functions
   connecting_to_databases
   utilities
   recipes
   about
   api

See :doc:`installation` for install instructions.

``etlhelper`` makes it easy to run SQL queries via Python and return the
results.
It is built upon the `DBAPI2 specification <https://www.python.org/dev/peps/pep-0249/>`__ and takes care of cursor management, importing drivers and formatting connection strings, while providing memory-efficient functions to read, write and transform data.
This reduces the amount of boilerplate code required to manipulate data within relational databases with Python.

**Features**

-  ``fetchall``, ``iter_rows``, ``fetchone`` functions for
   querying databases
-  ``execute``, ``executemany``, and ``load`` functions to insert data
-  ``copy_rows`` and ``copy_table_rows`` to transfer data between databases
-  Data transfer uses memory-efficient generators (``iter_chunks`` and ``iter_rows``)
-  User-defined transform functions transform data in-flight
-  ``DbParams`` objects provide consistent way to connect to different
   database types (currently Oracle, PostgreSQL, SQLite and MS SQL
   Server)
-  Timestamped log messages for tracking long-running data transfers
-  Helpful error messages display the failed query SQL
-  ``on_error`` function to process rows that fail to insert

These tools can create easy-to-understand, lightweight, versionable and
testable Extract-Transform-Load (ETL) workflows.
This documentation site explains how the main features are used.
See the individual function docstrings for full details of parameters and
options.

**ETLHelper components**

ETLHelper has three components.

The *ETL functions* are used to extract, transform and load rows of data from relational databases.
They can be used with any DB API 2.0-compliant database connections.
Logging and helpful error messages are provided out-of-the-box.

The *DbParams* class provides a convenient way to define database connections.
For any given database system, it identifies the correct driver, the required parameters and defines connection strings.
It provides convenience methods for checking databases are reachable over a network and for connecting to them.

The *DbHelper* classes work behind the scenes to smooth out inconsistencies between different database systems.
They also apply database-specific optimisations e.g., using the faster ``executebatch`` function for PostgreSQL connections instead of ``executemany``.
In normal use, users do not interact with the DbHelper classes.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`