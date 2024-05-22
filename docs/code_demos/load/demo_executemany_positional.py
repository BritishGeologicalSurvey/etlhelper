"""ETL Helper script to demonstrate using executemany with a positional placeholder query."""
import sqlite3
import etlhelper as etl

db_file = "igneous_rocks.db"

# Positional placeholders for data in tuple format
insert_sql = """
    INSERT INTO igneous_rocks (name, grain_size, updated_at)
    VALUES (?, UPPER(?), datetime('now'))
"""

rows = [("basalt", "fine"), ("granite", "coarse")]

with sqlite3.connect(db_file) as conn:
    # Note that table must already exist
    processed, failed = etl.executemany(insert_sql, conn, rows)
