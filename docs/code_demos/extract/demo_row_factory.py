"""ETL Helper script to demonstrate using fetch functions with a given row factory."""
import sqlite3
import etlhelper as etl
from etlhelper.row_factories import namedtuple_row_factory

db_file = "igneous_rocks.db"

with sqlite3.connect(db_file) as conn:
    row = etl.fetchone(
            "SELECT * FROM igneous_rock",
            conn,
            row_factory=namedtuple_row_factory,
        )

print(row)
print(row.name)
