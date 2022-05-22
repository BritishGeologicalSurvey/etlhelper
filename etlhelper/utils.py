"""
Utility functions to help with tasks such as programatically generating SQL queries.
"""
from collections import namedtuple

from etlhelper import fetchall
from etlhelper.exceptions import ETLHelperQueryError
from etlhelper.db_helper_factory import DB_HELPER_FACTORY

Column = namedtuple('Column', ['name', 'type', 'not_null', 'has_default'])


def table_info(table, conn, schema=None):
    """
    Return basic metadata for each of the columns of 'table' on 'conn'.

    :param table: str, the table to describe
    :param conn: dbapi connection
    :param schema: str, optional name of schema for table
    :returns columns: list, tuples of (name, type, not_null, has_default)
    """
    helper = DB_HELPER_FACTORY.from_conn(conn)

    params = (table, schema)
    result = fetchall(helper.table_info_query, conn, parameters=params)
    columns = [Column(*row) for row in result]

    if not columns:
        schema_table = f"{schema}.{table}" if schema else table
        msg = f"Table name '{schema_table}' not found."
        raise ETLHelperQueryError(msg)

    # If same table exists in another schema, duplicate columns may be returned
    if len(columns) > len(set(col.name for col in columns)):
        msg = (f"Table name {table} is not unique in database.  "
               "Please specify the schema.")
        raise ETLHelperQueryError(msg)

    return columns
