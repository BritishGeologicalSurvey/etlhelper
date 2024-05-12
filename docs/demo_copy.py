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


def transform(chunk):
    for row in chunk:
        new_row = {
            "name": row["name"],
            "category": "igneous",
            "last_update": dt.datetime.now()
        }
        yield new_row


etl.log_to_console()

with sqlite3.connect(igneous_db_file) as src:
    with sqlite3.connect(rock_db_file) as dest:
        # Create target table
        etl.execute(create_sql, dest)

        # Copy data
        rows = etl.copy_table_rows('igneous_rock', src, dest,
                                    target='rock', transform=transform)

        # Confirm transfer
        for row in etl.fetchall('SELECT * FROM rock', dest):
            print(row)
