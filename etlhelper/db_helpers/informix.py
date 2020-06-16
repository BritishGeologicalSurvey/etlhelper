"""
Database helper for Postgres
"""
from contextlib import contextmanager

from etlhelper.db_helpers.db_helper import DbHelper


class InformixDbHelper(DbHelper):
    """
    Postgres db helper class
    """
    def __init__(self):
        super().__init__()
        try:
            import ibm_db_dbi
            self.sql_exceptions = (ibm_db_dbi.ProgrammingError)
            self._connect_func = ibm_db_dbi.connect
            self.connect_exceptions = (ibm_db_dbi.OperationalError)
            self.required_params = {'hostname', 'port', 'database', 'uid'}
        except ImportError:
            print("The Informix Python libraries could not be found.\n"
                  "Run: python -m pip install ibm_db")

    def get_connection_string(self, db_params, password_variable):
        """
        Return a connection string
        :param db_params: DbParams
        :param password: str, password
        :return: str
        """
        # Prepare connection string
        password = self.get_password(password_variable)
        return f'database={db_params.database};hostname={db_params.hostname};' \
               f'port={db_params.port};protocol=tcpip;uid={db_params.uid};pwd={password}'

    def get_sqlalchemy_connection_string(self, db_params, password_variable):
        """
        Returns connection string for sql alchemy
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
        # This is faster than execute_many() because it results in fewer db
        # calls.  execute_values() or preparing single statement with
        # mogrify() were not used because resulting input statement is less
        # clear and selective formatting of inputs for spatial vs non-spatial
        # tables adds significant code complexity.
        # See following for background:
        # https://github.com/psycopg/psycopg2/issues/491#issuecomment-276551038
        # https://www.compose.com/articles/formatted-sql-in-python-with-psycopgs-mogrify/
        from psycopg2.extras import execute_batch

        execute_batch(cursor, query, chunk, page_size=len(chunk))


    @staticmethod
    @contextmanager
    def cursor(conn):
        """
        Return a cursor on current connection.  This implementation allows
        SQLite cursor to be used as context manager as with other db types.
        """
        try:
            cursor = conn.cursor()
            yield cursor
        finally:
            cursor.close()
