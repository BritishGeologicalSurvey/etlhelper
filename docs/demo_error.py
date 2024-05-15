"""ETL Helper script to demonstrate an extract error."""
import sqlite3
import etlhelper as etl

db_file = "igneous_rocks.db"
select_sql = "SELECT * FROM bad_table"

with sqlite3.connect(db_file) as conn:
    rows = etl.fetchall(select_sql, conn)
