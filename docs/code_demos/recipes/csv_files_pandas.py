"""ETL Helper script to demonstrate compatibility when creating an SQLAlchemy connection."""
import pandas as pd
from sqlalchemy import create_engine

from my_databases import ORACLEDB

engine = create_engine(ORACLEDB.get_sqlalchemy_connection_string("ORACLE_PASSWORD"))

sql = "SELECT * FROM my_table"
df = pd.read_sql(sql, engine)
df.to_csv("my_data.csv", header=True, index=False, float_format="%.3f")
