"""ETL Helper script to demonstrate copy_rows."""
import etlhelper as etl
from etlhelper.row_factories import namedtuple_row_factory
from my_databases import POSTGRESDB, ORACLEDB

select_sql = """
    SELECT
        customer_id,
        SUM (amount) AS total_amount
    FROM payment
    WHERE id > 1000
    GROUP BY customer_id
"""

# This insert query uses positional parameters, so a namedtuple_row_factory
# is used.
insert_sql = """
    INSERT INTO dest (
        customer_id,
        total_amount,
        loaded_by,
        load_time
    )
    VALUES (
        %s,
        %s,
        current_user,
        now()
    )
"""

with ORACLEDB.connect("ORA_PASSWORD") as src_conn:
    with POSTGRESDB.connect("PG_PASSWORD") as dest_conn:
        etl.copy_rows(
            select_sql,
            src_conn,
            insert_sql,
            dest_conn,
            row_factory=namedtuple_row_factory,
        )
