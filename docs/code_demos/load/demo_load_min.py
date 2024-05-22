"""ETL Helper script to demonstrate load."""
import sqlite3
import etlhelper as etl

db_file = "igneous_rocks.db"

rows = [
    {"name": "basalt", "grain_size": "fine"},
    {"name": "granite", "grain_size": "coarse"}
]

with sqlite3.connect(db_file) as conn:
    # Note that table must already exist
    processed, failed = etl.load("igneous_rock", conn, rows)
