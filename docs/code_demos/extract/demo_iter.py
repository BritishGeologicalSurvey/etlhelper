"""ETL Helper script to demonstrate iter_rows."""
import sqlite3
import etlhelper as etl

db_file = "igneous_rocks.db"

with sqlite3.connect(db_file) as conn:
    for row in etl.iter_rows("SELECT * FROM igneous_rock", conn):
        print(row)
