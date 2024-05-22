"""ETL Helper script to demonstrate using executemany with a named placeholder query."""
import sqlite3
import etlhelper as etl

db_file = "igneous_rocks.db"

# Insert query changes case and adds update_at column
insert_sql = """
    INSERT INTO igneous_rocks (name, grain_size, updated_at)
    VALUES (:name, UPPER(:grainsize), datetime('now'))
"""

rows = [
    {"name": "basalt", "grain_size": "fine"},
    {"name": "granite", "grain_size": "coarse"}
]

with sqlite3.connect(db_file) as conn:
    # Note that table must already exist
    processed, failed = etl.executemany(insert_sql, conn, rows)
