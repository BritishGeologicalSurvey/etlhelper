"""
Database helper for Oracle
"""
import warnings
from textwrap import dedent

from etlhelper.db_helpers.db_helper import DbHelper


class OracleDbHelper(DbHelper):
    """
    Oracle DB helper class
    """
    table_info_query = dedent("""
        SELECT
            column_name as name,
            data_type as type,
            (case when nullable = 'N' then 1 else 0 end) as not_null,
            (case when data_default is not null then 1 else 0 end) as has_default
        FROM all_tab_columns
        WHERE LOWER(table_name) = LOWER(:1)
        AND REGEXP_LIKE(LOWER(owner), '^' || COALESCE(LOWER(:2), '.*')  || '$')
        """).strip()

    def __init__(self):
        super().__init__()
        self.required_params = {'host', 'port', 'dbname', 'user'}
        self.missing_driver_msg = (
            "Could not import oracledb module required for Oracle connections.  "
            "See https://github.com/BritishGeologicalSurvey/etlhelper for installation instructions")
        self.named_paramstyle = 'named'
        self.positional_paramstyle = 'numeric'

        try:
            import oracledb
            self.sql_exceptions = (oracledb.DatabaseError,
                                   oracledb.InterfaceError)
            self.connect_exceptions = (oracledb.DatabaseError,
                                       oracledb.InterfaceError)
            self.paramstyle = oracledb.paramstyle
            self._connect_func = oracledb.connect

            # Fetch large objects as Python strings/bytes instead of LOBs
            # The oracledb default is LOB, but native types are faster to
            # transfer and are in line with return types of other databases
            # e.g. SQLite or PostgreSQL.
            # https://python-oracledb.readthedocs.io/en/latest/user_guide/lob_data.html#fetching-lobs-as-strings-and-bytes
            oracledb.defaults.fetch_lobs = False
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
        return (f'{db_params.user}/{password}@'
                f'{db_params.host}:{db_params.port}/{db_params.dbname}')

    def get_sqlalchemy_connection_string(self, db_params, password_variable):
        """
        Returns connection string for SQLAlchemy engine.
        """
        password = self.get_password(password_variable)
        return (f'oracle+oracledb://{db_params.user}:{password}@'
                f'{db_params.host}:{db_params.port}/{db_params.dbname}')
