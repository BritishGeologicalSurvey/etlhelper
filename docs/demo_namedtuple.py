"""Extract script using namedtuple_row_factory"""
import sqlite3
import etlhelper as etl
from etlhelper.row_factories import namedtuple_row_factory

with sqlite3.connect("igneous_rocks.db") as conn:
    row = etl.fetchone('SELECT * FROM igneous_rock', conn,
                       row_factory=namedtuple_row_factory)

print(row)
print(row.name)
