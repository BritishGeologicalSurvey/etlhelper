"""ETL Helper script to demonstrate oracle handling Large Objects (LOBs)."""
import etlhelper as etl
import oracledb
from my_databases import ORACLEDB

select_sql = "SELECT my_clob, my_blob FROM my_table"

with ORACLEDB.connect("ORA_PASSWORD") as conn:
    # By default, ETL Helper returns native types
    result_as_native = etl.fetchall(select_sql, conn)

    # Update oracledb settings to return LOBs
    oracledb.defaults.fetch_lobs = True
    result_as_lobs = etl.fetchall(select_sql, conn)
