Welcome to ETL Helper's documentation!
======================================

.. toctree::
   :maxdepth: 3
   :hidden:

   Home <self>
   installation
   etl_functions
   connecting_to_databases
   utilities
   recipes
   about
   api

.. image:: https://img.shields.io/pypi/v/etlhelper?label=Current%20Release
   :target: https://pypi.org/project/etlhelper
.. image:: https://img.shields.io/pypi/dm/etlhelper?label=Downloads%20pypi

ETL Helper is a Python ETL (Extract, Transform, Load) library to simplify data transfer into and out of databases.


.. note::  This documentation is a work in progress in preparation for the upcoming 1.0 release.
   Refer to the `v0.14.3 GitHub README.md <https://github.com/BritishGeologicalSurvey/etlhelper/tree/v0.14.3>`_ for the current codebase.
   There are a number of breaking changes planned for
   ``etlhelper`` version 1.0. Please pin the version number in your
   dependency list to avoid disruption and watch the project on GitHub
   for notification of new releases.

ETL Helper makes it easy to run SQL queries via Python and return the
results.
It takes care of cursor management, importing drivers and formatting connection strings,
while providing memory-efficient functions to read, write and transform data.
This reduces the amount of boilerplate code required to manipulate data within relational
databases with Python.

Features
^^^^^^^^

-  ``fetchall``, ``iter_rows``, ``fetchone`` functions for
   querying databases
-  Data transfer uses memory-efficient generators (``iter_chunks`` and ``iter_rows``)
-  ``executemany``, and ``load`` functions to insert or update data
-  ``copy_rows`` and ``copy_table_rows`` to transfer data between databases
-  User-defined transform functions transform data in-flight
-  ``execute`` function for one-off commands
-  Helpful error messages display the failed query SQL
-  ``on_error`` function to process rows that fail to insert
-  ``DbParams`` objects provide consistent way to connect to different
   database types (currently Oracle, PostgreSQL, SQLite and MS SQL
   Server)
-  Timestamped log messages for tracking long-running data transfers
-  Built upon the `DBAPI2 specification <https://www.python.org/dev/peps/pep-0249/>`__
   for database drivers in Python

These tools can create easy-to-understand, lightweight, versionable and
testable Extract-Transform-Load (ETL) workflows.

ETL Helper components
^^^^^^^^^^^^^^^^^^^^^

ETL Helper has three components.

The :doc:`etl_functions` are used to extract, transform and load rows of data from relational databases.
They can be used with any DB API 2.0-compliant database connections.
Logging and helpful error messages are provided out-of-the-box.

A :ref:`DbParams <db_params>` class provides a convenient way to define database connections.
For any given database system, it identifies the correct driver, the required parameters and defines connection strings.
It provides convenience methods for checking databases are reachable over a network and for connecting to them.

The :ref:`DbHelper <db_helpers>` classes work behind the scenes to smooth out inconsistencies between different database systems.
They also apply database-specific optimisations e.g., using the faster ``executebatch`` function for PostgreSQL connections instead of ``executemany``.
In normal use, users do not interact with the DbHelper classes.

Quickstart examples
^^^^^^^^^^^^^^^^^^^

Loading data
------------

The following script uses the ``execute``, ``load`` and ``fetchall`` functions to
create a database table and populate it with data.

.. literalinclude:: demo_load.py
   :language: python

The output is:

.. code:: bash

   {'id': 1, 'name': 'basalt', 'grain_size': 'fine'}
   {'id': 2, 'name': 'granite', 'grain_size': 'coarse'}

Copying data
------------

This script copies data to another database, with transformation and logging.

.. literalinclude:: demo_copy.py
   :language: python

The output is:

.. code:: bash

   # 2024-05-08 14:57:42,046 execute: Executing query
   # 2024-05-08 14:57:42,053 iter_chunks: Fetching rows (chunk_size=5000)
   # 2024-05-08 14:57:42,054 executemany: Executing many (chunk_size=5000)
   # 2024-05-08 14:57:42,054 iter_chunks: All rows returned
   # 2024-05-08 14:57:42,055 executemany: 2 rows processed (0 failed)
   # 2024-05-08 14:57:42,057 executemany: 2 rows processed in total

   {'id': 1, 'name': 'basalt', 'category': 'igneous', 'last_update': '2024-05-08 14:59:54.878726'}
   {'id': 2, 'name': 'granite', 'category': 'igneous', 'last_update': '2024-05-08 14:59:54.879034'}

The :doc:`recipes` section has more example code.