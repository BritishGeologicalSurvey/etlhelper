"""
Script to create database and load observations data from csv file. It also
demonstrates how an `on_error` function can handle failed rows.

Generate observations.csv with:
curl 'https://sensors.bgs.ac.uk/FROST-Server/v1.1/Observations?$select=@iot.id,result,phenomenonTime&$top=20000&$resultFormat=csv' -o observations.csv  # noqa
"""
import csv
import sqlite3
import datetime as dt
from typing import Iterable

import etlhelper as etl


def load_observations(csv_file: str, conn: sqlite3.Connection) -> None:
    """Load observations from csv_file to db_file."""
    # Drop table (helps with repeated test runs!)
    drop_table_sql = """
        DROP TABLE IF EXISTS observations
        """
    etl.execute(drop_table_sql, conn)

    # Create table (reject ids with no remainder when divided by 1000)
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY CHECK (id % 1000),
            time TIMESTAMP,
            result FLOAT
            )"""
    etl.execute(create_table_sql, conn)

    # Load data
    with open(csv_file, "rt") as f:
        reader = csv.DictReader(f)
        etl.load("observations", conn, reader, transform=transform, on_error=on_error)


# A transform function that takes an iterable of rows and returns an iterable
# of rows.
def transform(rows: Iterable[dict]) -> Iterable[dict]:
    """Rename time column and convert to Python datetime."""
    for row in rows:
        # Dictionaries are mutable, so rows can be modified in place.
        time_value = row.pop("phenomenonTime")
        row["time"] = dt.datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%S.%fZ")

    return rows


# The on_error function is called after each chunk with all the failed rows
def on_error(failed_rows: list[tuple[dict, Exception]]) -> None:
    """Print the IDs of failed rows"""
    rows, exceptions = zip(*failed_rows)
    failed_ids = [row["id"] for row in rows]
    print(f"Failed IDs: {failed_ids}")


if __name__ == "__main__":
    etl.log_to_console()

    db = etl.DbParams(dbtype="SQLITE", filename="observations.sqlite")
    with db.connect() as conn:
        load_observations("observations.csv", conn)
