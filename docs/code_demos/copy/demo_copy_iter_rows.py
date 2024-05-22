"""ETL Helper script to demonstrate copying records between databases with iter_rows and load."""
import etlhelper as etl
from my_databases import POSTGRESDB, ORACLEDB

select_sql = """
    SELECT id, name, value FROM my_table
    WHERE value > :min_value
"""

with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
    with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
        rows = etl.iter_rows(select_sql, src_conn, parameters={"min_value": 99})
        etl.load("my_table", dest_conn, rows)
