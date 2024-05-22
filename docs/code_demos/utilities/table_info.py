"""ETL Helper script to demonstrate table_info."""
from etlhelper.utils import table_info
from my_databases import ORACLEDB

with ORACLEDB.connect("ORA_PASSWORD") as conn:
    columns = table_info("my_table", conn, schema="my_schema")
