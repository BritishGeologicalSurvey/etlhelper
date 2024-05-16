"""ETL Helper script to load records to a database table."""
import sqlite3
import etlhelper as etl

db_file = "igneous_rocks.db"
create_sql = """
    CREATE TABLE IF NOT EXISTS igneous_rock (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        grain_size TEXT
    )"""
insert_sql = """
    INSERT INTO igneous_rock (id, name, grain_size)
    VALUES (:id, :name, :grain_size)
    ON CONFLICT DO NOTHING
"""

igneous_rocks = [
    {"id": 1, "name": "basalt", "grain_size": "fine"},
    {"id": 1, "name": "basalt", "grain_size": "fine"}  # duplicate row
]


with sqlite3.connect(db_file) as conn:
    # Create table
    etl.execute(create_sql, conn)

    # Insert rows
    etl.executemany(insert_sql, conn, rows=igneous_rocks)

    # Confirm selection
    for row in etl.fetchall('SELECT * FROM igneous_rock', conn):
        print(row)
