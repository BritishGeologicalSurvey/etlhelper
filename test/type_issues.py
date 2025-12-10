"""
This module is used to test that type hints are applied correctly.

It is not run as an automated test, but used for manual checking.  Opening
the file in VS Code with pylance type checking enabled should show no errors.

Running `mypy test/type_issues.py` can diagnose other type problems.
"""

import sqlite3
import psycopg2
import oracledb
import pyodbc

from etlhelper.types import Connection, Cursor

sqlite_conn: Connection = sqlite3.Connection(":memory:")
sqlite_cursor: Cursor = sqlite_conn.cursor()

postgres_conn: Connection = psycopg2.extensions.connection(dsn="something")
postgres_cursor: Cursor = postgres_conn.cursor()

oracle_conn: Connection = oracledb.Connection()
oracle_cursor: Cursor = oracle_conn.cursor()

pyodbc_conn: Connection = pyodbc.Connection()
pyodbc_cursor: Cursor = pyodbc_conn.cursor()
