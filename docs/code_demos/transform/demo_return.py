"""ETL Helper script to demonstrate using a transform function which returns a list of rows."""
import random
import sqlite3
from typing import Iterator
import etlhelper as etl
from etlhelper.row_factories import namedtuple_row_factory

db_file = "igneous_rocks.db"
select_sql = "SELECT * FROM igneous_rock"


def my_transform(chunk: Iterator[tuple]) -> list[tuple]:
    # Append random integer (1-10), filter if <5.

    new_chunk = []
    for row in chunk:  # each row is a namedtuple (immutable)
        extra_value = random.randrange(10)
        if extra_value >= 5:  # some rows are dropped
            new_row = (*row, extra_value)  # new rows have extra column
            new_chunk.append(new_row)

    return new_chunk


with sqlite3.connect(db_file) as conn:
    rows = etl.fetchall(
        select_sql,
        conn,
        row_factory=namedtuple_row_factory,
        transform=my_transform,
    )
