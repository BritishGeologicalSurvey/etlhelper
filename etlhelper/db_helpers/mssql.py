"""
Database helper for mssql
"""
from etlhelper.db_helpers.db_helper import DbHelper


class MSSQLDbHelper(DbHelper):
    """
    MS Sql server helper class
    """
    def __init__(self):
        super().__init__()
        try:
            import pyodbc
            self.sql_exceptions = (pyodbc.DatabaseError)
            self.connect_exceptions = (pyodbc.DatabaseError, pyodbc.InterfaceError)
            self.paramstyle = pyodbc.paramstyle
            self._connect_func = pyodbc.connect
            self.required_params = {'host', 'port', 'dbname', 'user', 'odbc_driver'}
        except ImportError:
            print("The pyodc Python package could not be found.\n"
                  "Run: python -m pip install pyodbc")

    def get_connection_string(self, db_params, password_variable):
        """
        Return a connection string
        :param db_params: DbParams
        :param password_variable: str, password
        :return: str
        """
        # Prepare connection string
        password = self.get_password(password_variable)
        return (f'DRIVER={db_params.odbc_driver};SERVER=tcp:{db_params.host};PORT={db_params.port};'
                f'DATABASE={db_params.dbname};UID={db_params.user};PWD={password}')

    def get_sqlalchemy_connection_string(self, db_params, password_variable):
        """
        Returns connection string for sql alchemy type
        """
        password = self.get_password(password_variable)
        driver = db_params.odbc_driver.replace(" ", "+")
        return (f'mssql+pyodbc://{db_params.user}:{password}@'
                f'{db_params.host}:{db_params.port}/{db_params.dbname}?'
                f'driver={driver}')
