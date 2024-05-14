"""ETL Helper script to copy records between databases."""
import datetime as dt
import sqlite3
import etlhelper as etl

igneous_db_file = "igneous_rocks.db"
rock_db_file = "rocks.db"

create_sql = """
    CREATE TABLE IF NOT EXISTS rock (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        category TEXT,
        last_update DATETIME
    )"""


select_sql = "SELECT name FROM igneous_rock"


def transform(chunk):
    for row in chunk:
        row['category'] = 'igneous'
        row['last_update'] = dt.datetime.now()
        yield row


etl.log_to_console()

with sqlite3.connect(igneous_db_file) as src:
    with sqlite3.connect(rock_db_file) as dest:
        # Create target table
        etl.execute(create_sql, dest)

        # Copy data
        rows = etl.iter_rows(select_sql, src, transform=transform)
        etl.load('rock', dest, rows)

        # Confirm transfer
        for row in etl.fetchall('SELECT * FROM rock', dest):
            print(row)
