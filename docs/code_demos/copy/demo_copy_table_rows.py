"""ETL Helper script to demonstrate copy_table_rows."""
import etlhelper as etl
from my_databases import POSTGRESDB, ORACLEDB

with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
    with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
        etl.copy_table_rows("my_table", src_conn, dest_conn)
