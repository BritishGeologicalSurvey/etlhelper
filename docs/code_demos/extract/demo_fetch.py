"""ETL Helper script to demonstrate using fetch functions."""
import sqlite3
import etlhelper as etl

db_file = "igneous_rocks.db"

with sqlite3.connect(db_file) as conn:
    first_row = etl.fetchone("SELECT * FROM igneous_rock", conn)
    all_rows = etl.fetchall("SELECT * FROM igneous_rock", conn)

print(first_row)
print(all_rows)
