import logging
import os
import socket
from collections import namedtuple
from itertools import islice, zip_longest

from etlhelper import exceptions
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.row_factories import namedtuple_row_factory

CHUNKSIZE = 5000

logger = logging.getLogger('etlhelper')


class Database:
    _db_helper = None
    _db_params = None
    _db_type = None
    _password_variable = None

    def __init__(self, dbtype='dbtype not set', **kwargs):
        self._db_type = dbtype.upper()
        self._db_helper = DB_HELPER_FACTORY.from_dbtype(self._db_type)
        self._validate_params(**kwargs)
        DbParams = namedtuple('DbParams', self._db_helper.required_params)
        self._db_params = DbParams(**kwargs)

    def __repr__(self):
        key_val_str = ", ".join([f"{key}='{getattr(self._db_params, key)}'" for key in self._db_params._fields])
        return f"Database(dbtype='{self._db_type}', {key_val_str})"

    def __str__(self):
        return self.__repr__()

    def _validate_params(self, **kwargs):
        """
        Validate database parameters.

        Should validate that a dbtype is a valid one and that the appropriate
        params have been passed for a particular db_type.

        :raises ETLHelperParamsError: Error if params are invalid
        """
        # Get a set of the attributes to compare against required attributes.
        given = set(kwargs.keys())

        required_params = self._db_helper.required_params

        unset_params = (given ^ required_params) & required_params
        if unset_params:
            msg = f'{unset_params} not set. Required parameters are {required_params}'
            raise exceptions.ETLHelperDbParamsError(msg)

        bad_params = given ^ required_params
        if bad_params:
            msg = f"Invalid parameter(s): {bad_params}"
            raise exceptions.ETLHelperDbParamsError(msg)

    @classmethod
    def from_environment(cls, prefix='ETLHelper_'):
        """
        Create Database object from parameters specified by environment
        variables e.g. ETLHelper_dbtype, ETLHelper_host, ETLHelper_port, etc.
        :param prefix: str, prefix to environment variable names
        """
        dbparams_keys = [key for key in os.environ if key.startswith(prefix)]
        dbparams_from_env = {key.replace(prefix, '').lower(): os.environ[key] for key in dbparams_keys}

        return cls(**dbparams_from_env)

    def is_reachable(self):
        """
        Test whether network allows opening of tcp/ip connection to database. No
        username or password are required.

        :return bool:
        """
        if self._db_type == 'SQLITE':
            raise ValueError("SQLITE DbParam does not require connection over network")

        s = socket.socket()
        try:
            # Connection succeeds
            s.connect((self._db_params.host, int(self._db_params.port)))
            return True
        except OSError:
            # Failed to connect
            return False
        finally:
            s.close()

    def connect(self, password_variable=None, **kwargs):
        """
        Return database connection.

        :param password_variable: str, name of environment variable with password
        :param kwargs: connection specific keyword arguments e.g. row_factory
        :return: Connection object
        """
        self._password_variable = password_variable
        return self._db_helper.connect(self._db_params, self._password_variable, **kwargs)

    def get_connection_string(self, password_variable=None):
        """
        Return the connection string.

        :param password_variable: str, name of environment variable with password
        :return: str, Connection string
        """
        return self._db_helper.get_connection_string(
            self._db_params,
            password_variable if password_variable else self._password_variable,
        )

    def get_sqlalchemy_connection_string(self, password_variable=None):
        """
        Get a SQLAlchemy connection string.

        :param password_variable: str, name of environment variable with password
        :return: str, Connection string
        """
        if hasattr(self._db_helper, 'get_sqlalchemy_connection_string'):
            return self._db_helper.get_sqlalchemy_connection_string(
                self._db_params, password_variable if password_variable else self._password_variable)
        else:
            return None

    def copy(self):
        """
        Return a shallow copy of Database object.

        :return Database: Database object with same attributes as original.
        """
        return self.__class__(**self)

    @property
    def paramstyle(self):
        """The DBAPI2 paramstyle attribute for database type"""
        return self._db_helper.paramstyle

    def iter_chunks(self,
                    select_query,
                    parameters=(),
                    row_factory=namedtuple_row_factory,
                    transform=None,
                    read_lob=False,
                    chunksize=CHUNKSIZE):
        """
        Run SQL query against connection and return iterator object to loop over
        results in batches of chunksize (default 5000).

        The row_factory changes the output format of the results.  Other row
        factories e.g. dict_row_factory are available.

        The transform function is applied to chunks of data as they are extracted
        from the database.

        The read_lob parameter will convert Oracle LOB objects to strings. It is
        required to access results of some Oracle Spatial functions.

        :param select_query: str, SQL query to execute
        :param parameters: sequence or dict of bind variables to insert in the query
        :param row_factory: function that accepts a cursor and returns a function
                            for parsing each row
        :param transform: function that accepts an iterable (e.g. list) of rows and
                        returns an iterable of rows (possibly of different shape)
        :param read_lob: bool, convert Oracle LOB objects to strings
        """

        logger.info("Fetching rows")
        logger.debug(f"Fetching:\n\n{select_query}\n\nwith parameters:\n\n"
                     f"{parameters}\n\nagainst\n\n{self._db_helper.connection}")

        with self._db_helper.connection.cursor() as cursor:
            # Run query
            try:
                cursor.execute(select_query, parameters)
            except self._db_helper.sql_exceptions as exc:
                # Even though we haven't modified data, we have to rollback to
                # clear the failed transaction before any others can be started.
                self._db_helper.connection.rollback()
                msg = (f"SQL query raised an error.\n\n{select_query}\n\n"
                       f"Required paramstyle: {self._db_helper.paramstyle}\n\n{exc}\n")
                raise exceptions.ETLHelperExtractError(msg)

            # Set row factory
            create_row = row_factory(cursor)

            # Parse results
            first_pass = True
            while True:
                rows = cursor.fetchmany(chunksize)

                # No more rows to process
                if not rows:
                    if first_pass:
                        msg = "No rows returned"
                    else:
                        if cursor.rowcount == -1:
                            # SQLite3 drive doesn't support row count (always -1)
                            msg = "All rows returned"
                        else:
                            msg = f"{cursor.rowcount} rows returned"
                    logger.info(msg)
                    return

                # Convert Oracle LOBs to strings if required
                if read_lob:
                    rows = self._read_lob(rows)

                # Apply row_factory
                rows = (create_row(row) for row in rows)

                # Apply transform
                if transform:
                    rows = transform(rows)

                # Return data
                yield rows
                first_pass = False

    def iter_rows(self,
                  select_query,
                  parameters=(),
                  row_factory=namedtuple_row_factory,
                  transform=None,
                  read_lob=False,
                  chunksize=CHUNKSIZE):
        """
        Run SQL query against connection and return iterator object to loop over
        results, row-by-row.

        :param select_query: str, SQL query to execute
        :param parameters: sequence or dict of bind variables to insert in the query
        :param row_factory: function that accepts a cursor and returns a function
                            for parsing each row
        :param transform: function that accepts an iterable (e.g. list) of rows and
                        returns an iterable of rows (possibly of different shape)
        :param read_lob: bool, convert Oracle LOB objects to strings
        """
        for chunk in self.iter_chunks(select_query,
                                      row_factory=row_factory,
                                      parameters=parameters,
                                      transform=transform,
                                      read_lob=read_lob,
                                      chunksize=chunksize):
            for row in chunk:
                yield row

    def get_rows(
            self,
            select_query,
            parameters=(),
            row_factory=namedtuple_row_factory,
            transform=None,
            chunksize=CHUNKSIZE,
    ):
        """
        Get results of query as a list.  See iter_rows for details.
        :param select_query: str, SQL query to execute
        :param parameters: sequence or dict of bind variables to insert in the query
        :param row_factory: function that accepts a cursor and returns a function
                            for parsing each row
        :param transform: function that accepts an iterable (e.g. list) of rows and
                        returns an iterable of rows (possibly of different shape)
        """
        return list(
            self.iter_rows(select_query,
                           row_factory=row_factory,
                           parameters=parameters,
                           transform=transform,
                           chunksize=chunksize))

    def fetchone(self, select_query, parameters=(), row_factory=namedtuple_row_factory, transform=None):
        """
        Get first result of query.  See iter_rows for details.  Note: iter_rows is
        recommended for looping over rows individually.

        :param select_query: str, SQL query to execute
        :param parameters: sequence or dict of bind variables to insert in the query
        :param row_factory: function that accepts a cursor and returns a function
                            for parsing each row
        :param transform: function that accepts an iterable (e.g. list) of rows and
                        returns an iterable of rows (possibly of different shape)
        """
        try:
            result = next(self.iter_rows(
                select_query,
                row_factory=row_factory,
                parameters=parameters,
                transform=transform,
            ))
        except StopIteration:
            result = None

        return result

    def fetchmany(self, select_query, size=1, parameters=(), row_factory=namedtuple_row_factory, transform=None):
        """
        Get first 'size' results of query as a list.  See iter_rows for details.
        Note: iter_chunks is recommended for looping over rows in batches.

        :param select_query: str, SQL query to execute
        :param parameters: sequence or dict of bind variables to insert in the query
        :param size: number of rows to return (defaults to 1)
        :param row_factory: function that accepts a cursor and returns a function
                            for parsing each row
        :param transform: function that accepts an iterable (e.g. list) of rows and
                        returns an iterable of rows (possibly of different shape)
        """
        return list(
            islice(
                self.iter_rows(select_query, row_factory=row_factory, parameters=parameters, transform=transform),
                size,
            ))

    def fetchall(self, select_query, parameters=(), row_factory=namedtuple_row_factory, transform=None):
        """
        Get all results of query as a list.  See iter_rows for details.
        :param select_query: str, SQL query to execute
        :param parameters: sequence or dict of bind variables to insert in the query
        :param row_factory: function that accepts a cursor and returns a function
                            for parsing each row
        :param transform: function that accepts an iterable (e.g. list) of rows and
                        returns an iterable of rows (possibly of different shape)
        """
        return list(self.iter_rows(select_query, row_factory=row_factory, parameters=parameters, transform=transform))

    def dump_rows(
            self,
            select_query,
            output_func=print,
            parameters=(),
            row_factory=namedtuple_row_factory,
            transform=None,
    ):
        """
        Call output_func(row) one-by-one on results of query.  See iter_rows for
        details.

        :param select_query: str, SQL query to execute
        :param output_func: function to be called for each row (default is print)
        :param parameters: sequence or dict of bind variables to insert in the query
        :param row_factory: function that accepts a cursor and returns a function
                            for parsing each row
        :param transform: function that accepts an iterable (e.g. list) of rows and
                        returns an iterable of rows (possibly of different shape)
        """
        for row in self.iter_rows(select_query, parameters=parameters, row_factory=row_factory, transform=transform):
            output_func(row)

    def executemany(self, query, rows, commit_chunks=True, chunksize=CHUNKSIZE):
        """
        Use query to insert/update data from rows to database at conn.  This
        method uses the executemany or execute_batch (PostgreSQL) commands to
        process the data in chunks and avoid creating a new database connection for
        each row.  Row data are passed as parameters into query.

        commit_chunks controls if chunks the transaction should be committed after
        each chunk has been inserted.  Committing chunks means that errors during
        a long-running insert do not require all data to be loaded again.  The
        disadvantage is that investigation may be required to determine exactly
        which records have been successfully transferred.

        :param query: str, SQL insert command with placeholders for data
        :param rows: List of tuples containing data to be inserted/updated
        :param commit_chunks: bool, commit after each chunk has been inserted/updated
        :return row_count: int, number of rows inserted/updated
        """
        logger.info(f"Executing many (chunksize={chunksize})")
        logger.debug(f"Executing:\n\n{query}\n\nagainst\n\n{self._db_helper.connection}")

        processed = 0

        with self._db_helper.connection.cursor() as cursor:
            for chunk in self._chunker(rows, chunksize):
                # Run query
                try:
                    # Chunker pads to whole chunk with None; remove these
                    chunk = [row for row in chunk if row is not None]

                    # Show first row as example of data
                    if processed == 0:
                        logger.debug(f"First row: {chunk[0]}")

                    # Execute query
                    self._db_helper.executemany(cursor, query, chunk)
                    processed += len(chunk)

                except self._db_helper.sql_exceptions as exc:
                    # Rollback to clear the failed transaction before any others can
                    # be # started.
                    self._db_helper.connection.rollback()
                    msg = (f"SQL query raised an error.\n\n{query}\n\n"
                           f"Required paramstyle: {self._db_helper.paramstyle}\n\n{exc}\n")
                    raise exceptions.ETLHelperInsertError(msg)

                logger.info(f'{processed} rows processed')

                # Commit changes so far
                if commit_chunks:
                    self._db_helper.connection.commit()

        # Commit changes where not already committed
        if not commit_chunks:
            self._db_helper.connection.commit()

        logger.info(f'{processed} rows processed in total')

    def execute(self, query, parameters=()):
        """
        Run SQL query against connection.

        :param query: str, SQL query to execute
        :param conn: dbapi connection
        :param parameters: sequence or dict of bind variables to insert in the query
        """
        logger.info("Executing query")
        logger.debug(f"Executing:\n\n{query}\n\nwith parameters:\n\n"
                     f"{parameters}\n\nagainst\n\n{self._db_helper.connection}")

        with self._db_helper.connection.cursor() as cursor:
            # Run query
            try:
                cursor.execute(query, parameters)
                self._db_helper.connection.commit()
            except self._db_helper.sql_exceptions as exc:
                # Even though we haven't modified data, we have to rollback to
                # clear the failed transaction before any others can be started.
                self._db_helper.connection.rollback()
                msg = (f"SQL query raised an error.\n\n{query}\n\n"
                       f"Required paramstyle: {self._db_helper.paramstyle}\n\n{exc}\n")
                raise exceptions.ETLHelperQueryError(msg)

    def _chunker(self, iterable, n_chunks, fillvalue=None):
        """Collect data into fixed-length chunks or blocks.
        Code from recipe at https://docs.python.org/3.6/library/itertools.html
        """
        # _chunker('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
        args = [iter(iterable)] * n_chunks
        return zip_longest(*args, fillvalue=fillvalue)

    def _read_lob(self, rows):
        """
        Replace Oracle LOB objects within rows with their string representation.
        :param rows: list of tuples of output data
        :return: list of tuples with LOB objects converted to strings
        """
        clean_rows = []
        for row in rows:
            clean_row = [x.read() if str(x.__class__) == "<class 'cx_Oracle.LOB'>" else x for x in row]
            clean_rows.append(clean_row)

        return clean_rows
