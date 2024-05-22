"""ETL Helper script to demonstrate using a transform function which yields individual rows."""
import sqlite3
from typing import Iterator
import etlhelper as etl
from etlhelper.row_factories import dict_row_factory

db_file = "igneous_rocks.db"
select_sql = "SELECT * FROM igneous_rock"


def my_transform(chunk: Iterator[dict]) -> Iterator[dict]:
    # Add prefix to id, remove newlines, set lower case names

    for row in chunk:  # each row is a dictionary (mutable)
        row["id"] += 1000
        row["description"] = row["description"].replace("\n", " ")
        row["name"] = row["name"].lower()
        yield row


with sqlite3.connect(db_file) as conn:
    rows = etl.fetchall(
        select_sql,
        conn,
        row_factory=dict_row_factory,
        transform=my_transform,
    )
