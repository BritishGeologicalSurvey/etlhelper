"""ETL Helper script to demonstrate logging errors."""
import logging
import sqlite3
import etlhelper as etl

etl.log_to_console()
logger = logging.getLogger("etlhelper")

db_file = "igneous_rocks.db"

rows = [
    {"name": "basalt", "grain_size": "fine"},
    {"name": "granite", "grain_size": "coarse"}
]


def log_errors(failed_rows: list[tuple[dict, Exception]]) -> None:
    for row, exception in failed_rows:
        logger.error(exception)


with sqlite3.connect(db_file) as conn:
    etl.load("igneous_rock", conn, rows, on_error=log_errors)
