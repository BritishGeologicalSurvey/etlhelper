"""
Database helper for SQLite
"""
from contextlib import contextmanager
from textwrap import dedent
import warnings
from etlhelper.db_helpers.db_helper import DbHelper


class SQLiteDbHelper(DbHelper):
    """
    SQLite DB helper class
    """
    # schema_name is not used for SQLite but is required as parameter to be
    # consistent with other databases.  The WHERE clause is always true,
    # whether schema_name is NULL or not.
    table_info_query = dedent("""
        SELECT
            name,
            lower(type),
            "notnull" as not_null,
            (case when dflt_value is not null then 1 else 0 end) as has_default
        FROM pragma_table_info(:table_name)
        -- this effectively ignores the unused schema_name
        -- parameter since schemas are not used in sqlite
        WHERE COALESCE(TRUE, :schema_name)
        ;""").strip()

    def __init__(self):
        super().__init__()
        self.required_params = {'filename'}
        self.missing_driver_msg = (
            "Could not import sqlite3 module required for SQLite connections.  "
            "Check Python configuration - this should be part of Standard Library.")
        self.named_paramstyle = 'named'
        self.positional_paramstyle = 'qmark'

        try:
            import sqlite3
            self.sql_exceptions = (sqlite3.DatabaseError,
                                   sqlite3.InterfaceError)
            self.connect_exceptions = (sqlite3.DatabaseError,
                                       sqlite3.InterfaceError)
            self.paramstyle = sqlite3.paramstyle
            self._connect_func = sqlite3.connect
        except ImportError:
            warnings.warn(self.missing_driver_msg)

    def get_connection_string(self, db_params, password_variable=None):
        """
        Return a connection string
        :param db_params: DbParams
        :return: str
        """
        # Prepare connection string
        # Accept unused password_variable for consistency with other databases
        return (f'{db_params.filename}')

    def get_sqlalchemy_connection_string(self, db_params,
                                         password_variable=None):
        """
        Returns connection string for SQLAlchemy type connections
        :param db_params: DbParams
        :return: str
        """
        return (f'sqlite:///{db_params.filename}')

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
