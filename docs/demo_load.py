import sqlite3

import etlhelper as etl

db_file = "igneous_rocks.db"
create_sql = """
    CREATE TABLE IF NOT EXISTS igneous_rocks (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        grain_size TEXT
    )"""

igneous_rocks = [
    {"name": "basalt", "grain_size": "fine"},
    {"name": "granite", "grain_size": "coarse"}
]


with sqlite3.connect(db_file) as conn:
    # Create table
    etl.execute(create_sql, conn)

    # Insert rows
    etl.load('igneous_rocks', conn, igneous_rocks)

    # Confirm selection
    for row in etl.fetchall('SELECT * FROM igneous_rocks', conn):
        print(row)
