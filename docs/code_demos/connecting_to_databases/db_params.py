"""ETL Helper script to demonstrate DbParams."""
import etlhelper as etl

ORACLEDB = etl.DbParams(
    dbtype="ORACLE",
    host="localhost",
    port=1521,
    dbname="mydata",
    user="oracle_user",
)

POSTGRESDB = etl.DbParams(
    dbtype="PG",
    host="localhost",
    port=5432,
    dbname="mydata",
    user="postgres_user",
)

SQLITEDB = etl.DbParams(
    dbtype="SQLITE",
    filename="/path/to/file.db",
)

MSSQLDB = etl.DbParams(
    dbtype="MSSQL",
    host="localhost",
    port=1433,
    dbname="mydata",
    user="mssql_user",
    odbc_driver="ODBC Driver 17 for SQL Server",
)
