"""DB Helper factory

Factory pattern that generates a DbHelper for each DB type

"""
from __future__ import annotations
from functools import lru_cache

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from etlhelper.db_params import DbParams
    from etlhelper.db_helpers import DbHelper

from etlhelper.db_helpers import OracleDbHelper
from etlhelper.db_helpers import PostgresDbHelper
from etlhelper.db_helpers import MSSQLDbHelper
from etlhelper.db_helpers import SQLiteDbHelper
from etlhelper.exceptions import ETLHelperHelperError
from etlhelper.types import Connection


class DbHelperFactory:
    """
    The DB Helper Factory class.
    """
    def __init__(self):
        self.helpers: dict[str, type[DbHelper]] = {}
        self._conn_types: dict[str, str] = {}

    def register_helper(self, dbtype: str, conn_type: str, db_helper: type[DbHelper]) -> None:
        """
        Store db helper in internal list.
        """
        self.helpers[dbtype] = db_helper
        self._conn_types[conn_type] = dbtype

    def from_db_params(self, db_params: DbParams) -> DbHelper:
        """
        Return initialised db_helper
        """
        if not hasattr(db_params, 'dbtype'):
            msg = f"Expected DbParams-like object, got {type(db_params)}"
            raise ETLHelperHelperError(msg)

        return self.from_dbtype(db_params.dbtype)

    def from_conn(self, conn: Connection) -> DbHelper:
        """
        Return initialised db_helper based on connection.
        """
        if not hasattr(conn, 'cursor'):
            msg = f"Expected connection-like object, got {type(conn)}"
            raise ETLHelperHelperError(msg)

        conn_type = str(conn.__class__)
        try:
            dbtype = self._conn_types[conn_type]
        except KeyError:
            msg = f"Unsupported connection type: {conn_type}"
            raise ETLHelperHelperError(msg)
        return self.from_dbtype(dbtype)

    @lru_cache(maxsize=16)
    def from_dbtype(self, dbtype: str) -> DbHelper:
        """
        Return initialised db helper based on type
        """
        try:
            helper = self.helpers[dbtype]()
        except KeyError:
            msg = f"Unsupported DbParams.dbtype: {dbtype}"
            raise ETLHelperHelperError(msg)
        return helper


DB_HELPER_FACTORY = DbHelperFactory()
DB_HELPER_FACTORY.register_helper('ORACLE', "<class 'oracledb.Connection'>",
                                  OracleDbHelper)
DB_HELPER_FACTORY.register_helper('PG', "<class 'psycopg2.extensions.connection'>",
                                  PostgresDbHelper)
DB_HELPER_FACTORY.register_helper('MSSQL', "<class 'pyodbc.Connection'>",
                                  MSSQLDbHelper)
DB_HELPER_FACTORY.register_helper('SQLITE', "<class 'sqlite3.Connection'>",
                                  SQLiteDbHelper)
