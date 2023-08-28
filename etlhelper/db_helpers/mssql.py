"""
Database helper for mssql
"""
import warnings
from textwrap import dedent

from etlhelper.db_helpers.db_helper import DbHelper
from etlhelper.exceptions import ETLHelperInsertError


class MSSQLDbHelper(DbHelper):
    """
    MS Sql server helper class
    """
    table_info_query = dedent("""
        SELECT
            column_name as name,
            data_type as type,
            (case when is_nullable = 'NO' then 1 else 0 end) as not_null,
            (case when column_default is not null then 1 else 0 end) as has_default
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE LOWER(table_name) = LOWER(?)
        AND LOWER(table_schema) LIKE COALESCE(LOWER(?), '%%')
        """).strip()

    def __init__(self):
        super().__init__()
        self.required_params = {'host', 'port', 'dbname', 'user', 'odbc_driver'}
        self.missing_driver_msg = (
            "Could not import pyodbc module required for MS SQL connections.  "
            "See https://github.com/BritishGeologicalSurvey/etlhelper for installation instructions")
        self.named_paramstyle = None  # pyodbc doesn't support named parameters
        self.positional_paramstyle = 'qmark'

        try:
            import pyodbc
            self.sql_exceptions = (pyodbc.DatabaseError,
                                   pyodbc.InterfaceError)
            self.connect_exceptions = (pyodbc.DatabaseError,
                                       pyodbc.InterfaceError)
            self.paramstyle = pyodbc.paramstyle
            self._connect_func = pyodbc.connect
            self.use_fast_executemany = True
            self.trust_server_certificate = False
        except ImportError:
            warnings.warn(self.missing_driver_msg)

    def connect(self, db_params, password_variable=None,
                fast_executemany=True, trust_server_certificate=False, **kwargs):
        self.use_fast_executemany = fast_executemany
        self.trust_server_certificate = trust_server_certificate
        return super().connect(db_params, password_variable, **kwargs)

    def get_connection_string(self, db_params, password_variable):
        """
        Return a connection string
        :param db_params: DbParams
        :param password_variable: str, password
        :return: str
        """
        # Prepare connection string
        password = self.get_password(password_variable)
        conn_str = (f'DRIVER={db_params.odbc_driver};SERVER=tcp:{db_params.host};PORT={db_params.port};'
                    f'DATABASE={db_params.dbname};UID={db_params.user};PWD={password}')
        if self.trust_server_certificate:
            conn_str += ';TrustServerCertificate=yes'
        return conn_str

    def get_sqlalchemy_connection_string(self, db_params, password_variable):
        """
        Returns connection string for SQLAlchemy engine.
        """
        password = self.get_password(password_variable)
        driver = db_params.odbc_driver.replace(" ", "+")
        return (f'mssql+pyodbc://{db_params.user}:{password}@'
                f'{db_params.host}:{db_params.port}/{db_params.dbname}?'
                f'driver={driver}')

    def executemany(self, cursor, query, chunk):
        """
        Try to use fast_executemany for SQL Server if flag in DbParams is set.

        :param cursor: Open database cursor.
        :param query: str, SQL query
        :param chunk: list, Rows of parameters.
        """
        try:
            cursor.fast_executemany = self.use_fast_executemany
            cursor.executemany(query, chunk)
        except MemoryError:
            warnings.warn(
                "fast_executemany execution failed.  Retrying with default executemany.  "
                "See https://github.com/BritishGeologicalSurvey/etlhelper/issues/86 for more information"
            )
            cursor.fast_executemany = False
            cursor.executemany(query, chunk)
        except TypeError:
            msg = ("pyodbc driver for MS SQL only supports positional placeholders.  "
                   "Use namedtuple, tuple or list (via row_factory setting for copy_rows).")
            raise ETLHelperInsertError(msg)
