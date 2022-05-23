"""
Database helper for PostgreSQL
"""
from textwrap import dedent
import warnings
from etlhelper.db_helpers.db_helper import DbHelper


class PostgresDbHelper(DbHelper):
    """
    Postgres db helper class
    """
    table_info_query = dedent("""
        SELECT
            pg_attribute.attname AS name,
            pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS type,
            (case when pg_attribute.attnotnull then 1 else 0 end) as not_null,
            (case when pg_attribute.atthasdef then 1 else 0 end) as has_default
        FROM
            pg_catalog.pg_attribute
        INNER JOIN
            pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid
        INNER JOIN
            pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE
            pg_attribute.attnum > 0
            AND NOT pg_attribute.attisdropped
            AND pg_class.relname = %s
            AND pg_namespace.nspname ~ COALESCE(%s, '.*')
        ORDER BY
            attnum ASC;
        """).strip()

    def __init__(self):
        super().__init__()
        self.required_params = {'host', 'port', 'dbname', 'user'}
        self.missing_driver_msg = (
            "Could not import psycopg2 module required for PostgreSQL connections.  "
            "See https://github.com/BritishGeologicalSurvey/etlhelper for installation instructions")
        self.named_paramstyle = 'pyformat'
        self.positional_paramstyle = 'format'

        try:
            import psycopg2
            self.sql_exceptions = (psycopg2.DatabaseError,
                                   psycopg2.InterfaceError)
            self.connect_exceptions = (psycopg2.DatabaseError,
                                       psycopg2.InterfaceError)
            self.paramstyle = psycopg2.paramstyle
            self._connect_func = psycopg2.connect
        except ImportError:
            warnings.warn(self.missing_driver_msg)

    def get_connection_string(self, db_params, password_variable):
        """
        Return a connection string
        :param db_params: DbParams
        :param password_variable: str, password
        :return: str
        """
        # Prepare connection string
        password = self.get_password(password_variable)
        return (f'host={db_params.host} port={db_params.port} '
                f'dbname={db_params.dbname} '
                f'user={db_params.user} password={password}')

    def get_sqlalchemy_connection_string(self, db_params, password_variable):
        """
        Returns connection string for SQLAlchemy engine.
        """
        password = self.get_password(password_variable)
        return (f'postgresql://{db_params.user}:{password}@'
                f'{db_params.host}:{db_params.port}/{db_params.dbname}')

    @staticmethod
    def executemany(cursor, query, chunk):
        """
        Call execute_batch method for PostGres.

        :param cursor: Open database cursor.
        :param query: str, SQL query
        :param chunk: list, Rows of parameters.
        """
        # Here we use execute_batch to send multiple inserts to db at once.
        # This is faster than executemany() because it results in fewer db
        # calls.  execute_values() or preparing single statement with
        # mogrify() were not used because resulting input statement is less
        # clear and selective formatting of inputs for spatial vs non-spatial
        # tables adds significant code complexity.
        # See following for background:
        # https://github.com/psycopg/psycopg2/issues/491#issuecomment-276551038
        # https://www.compose.com/articles/formatted-sql-in-python-with-psycopgs-mogrify/
        from psycopg2.extras import execute_batch

        execute_batch(cursor, query, chunk, page_size=len(chunk))
