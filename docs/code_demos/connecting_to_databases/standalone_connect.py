import etlhelper as etl
from my_databases import ORACLEDB
oracle_conn = etl.connect(ORACLEDB, "ORACLE_PASSWORD")
