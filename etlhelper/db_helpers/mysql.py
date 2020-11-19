"""
Database helper for MySQL
"""
import warnings

from etlhelper.db_helpers.db_helper import DbHelper


class MySQLDbHelper(DbHelper):
    """
    MySQL DB helper class
    """
    def __init__(self):
        super().__init__()
        self.required_params = {'host', 'port', 'dbname', 'user'}
        self.missing_driver_msg = (
            "Could not import pymysql module required for PostgreSQL connections.  "
            "See https://github.com/BritishGeologicalSurvey/etlhelper for installation instructions")
        try:
            import mysql.connector
            import mysql.connector.pooling
            self._pooling = mysql.connector.pooling
            self.sql_exceptions = (mysql.connector.Error)
            self.connect_exceptions = (mysql.connector.Error)
            self.paramstyle = mysql.connector.paramstyle
            self._connect_func = mysql.connector.connect
            self._pool_connect = 'get_connection'
        except ImportError:
            warnings.warn(self.missing_driver_msg)

    def get_connection(self, connection_hash, db_params, password_variable):
        if connection_hash not in self._pools:
            self._pools[connection_hash] = self._pooling.MySQLConnectionPool(
                pool_size=10,
                pool_reset_session=False,
                host=db_params.host,
                database=db_params.dbname,
                port=db_params.port,
                user=db_params.user,
                password=self.get_password(password_variable))

        return getattr(self._pools[connection_hash], self._pool_connect)()

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
        return (f'mysql://{db_params.user}:{password}@' f'{db_params.host}:{db_params.port}/{db_params.dbname}')
