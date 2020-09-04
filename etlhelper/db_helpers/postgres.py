"""
Database helper for PostgreSQL
"""
import warnings
from etlhelper.db_helpers.db_helper import DbHelper


class PostgresDbHelper(DbHelper):
    """
    Postgres db helper class
    """
    def __init__(self):
        super().__init__()
        self.required_params = {'host', 'port', 'dbname', 'user'}
        self.missing_driver_msg = (
            "Could not import psycopg2 module required for PostgreSQL connections.  "
            "See https://github.com/BritishGeologicalSurvey/etlhelper for installation instructions")
        try:
            import psycopg2
            self.sql_exceptions = (psycopg2.ProgrammingError,
                                   psycopg2.InterfaceError,
                                   psycopg2.InternalError)
            self.connect_exceptions = (psycopg2.OperationalError)
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
