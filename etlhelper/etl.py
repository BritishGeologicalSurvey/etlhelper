"""
Functions for transferring data in and out of databases.
"""
from collections import namedtuple
from itertools import zip_longest, islice, chain
import logging

from etlhelper.row_factories import namedtuple_row_factory
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.exceptions import (
    ETLHelperExtractError,
    ETLHelperInsertError,
    ETLHelperQueryError,
)

logger = logging.getLogger('etlhelper')
CHUNKSIZE = 5000


# iter_chunks is where data are retrieved from source database
# All data extraction processes call this function.
def iter_chunks(select_query, conn, parameters=(),
                row_factory=namedtuple_row_factory,
                transform=None, read_lob=False, chunk_size=CHUNKSIZE):
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
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param read_lob: bool, convert Oracle LOB objects to strings
    :param chunk_size: int, size of chunks to group data by
    """
    logger.info("Fetching rows (chunk_size=%s)", chunk_size)
    logger.debug(f"Fetching:\n\n{select_query}\n\nwith parameters:\n\n"
                 f"{parameters}\n\nagainst\n\n{conn}")

    helper = DB_HELPER_FACTORY.from_conn(conn)
    with helper.cursor(conn) as cursor:
        # Run query
        try:
            cursor.execute(select_query, parameters)
        except helper.sql_exceptions as exc:
            # Even though we haven't modified data, we have to rollback to
            # clear the failed transaction before any others can be started.
            conn.rollback()
            msg = (f"SQL query raised an error.\n\n{select_query}\n\n"
                   f"Required paramstyle: {helper.paramstyle}\n\n{exc}\n")
            raise ETLHelperExtractError(msg)

        # Set row factory
        create_row = row_factory(cursor)

        # Parse results
        first_pass = True
        while True:
            rows = cursor.fetchmany(chunk_size)

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

                # Close the active transaction
                conn.commit()
                return

            # Convert Oracle LOBs to strings if required
            if read_lob:
                rows = _read_lob(rows)

            # Apply row_factory
            rows = (create_row(row) for row in rows)

            # Apply transform
            if transform:
                rows = transform(rows)

            # Return data
            yield rows
            first_pass = False


def iter_rows(select_query, conn, parameters=(),
              row_factory=namedtuple_row_factory,
              transform=None, read_lob=False, chunk_size=CHUNKSIZE):
    """
    Run SQL query against connection and return iterator object to loop over
    results, row-by-row.

    :param select_query: str, SQL query to execute
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param read_lob: bool, convert Oracle LOB objects to strings
    :param chunk_size: int, size of chunks to group data by
    """
    for chunk in iter_chunks(select_query, conn, row_factory=row_factory,
                             parameters=parameters, transform=transform,
                             read_lob=read_lob, chunk_size=chunk_size):
        for row in chunk:
            yield row


def get_rows(select_query, conn, parameters=(),
             row_factory=namedtuple_row_factory, transform=None,
             chunk_size=CHUNKSIZE):
    """
    Get results of query as a list.  See iter_rows for details.
    :param select_query: str, SQL query to execute
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param chunk_size: int, size of chunks to group data by
    """
    return list(iter_rows(select_query, conn, row_factory=row_factory,
                          parameters=parameters, transform=transform,
                          chunk_size=chunk_size))


def fetchone(select_query, conn, parameters=(),
             row_factory=namedtuple_row_factory, transform=None,
             chunk_size=1):
    """
    Get first result of query.  See iter_rows for details.  Note: iter_rows is
    recommended for looping over rows individually.

    :param select_query: str, SQL query to execute
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    """
    try:
        result = next(iter_rows(select_query, conn, row_factory=row_factory,
                                parameters=parameters, transform=transform,
                                chunk_size=chunk_size))
    except StopIteration:
        result = None
    finally:
        # Commit to close the transaction before the iterator has been exhausted
        conn.commit()

    return result


def fetchmany(select_query, conn, size=1, parameters=(),
              row_factory=namedtuple_row_factory, transform=None,
              chunk_size=CHUNKSIZE):
    """
    Get first 'size' results of query as a list.  See iter_rows for details.
    Note: iter_chunks is recommended for looping over rows in batches.

    :param select_query: str, SQL query to execute
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    :param size: number of rows to return (defaults to 1)
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param chunk_size: int, size of chunks to group data by
    """
    try:
        result = list(
            islice(iter_rows(select_query, conn, row_factory=row_factory,
                             parameters=parameters, transform=transform,
                             chunk_size=chunk_size), size))
    finally:
        # Commit to close the transaction before the iterator has been exhausted
        conn.commit()

    return result


def fetchall(select_query, conn, parameters=(),
             row_factory=namedtuple_row_factory, transform=None,
             chunk_size=CHUNKSIZE):
    """
    Get all results of query as a list.  See iter_rows for details.
    :param select_query: str, SQL query to execute
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param chunk_size: int, size of chunks to group data by
    """
    return list(iter_rows(select_query, conn, row_factory=row_factory,
                          parameters=parameters, transform=transform,
                          chunk_size=chunk_size))


def dump_rows(select_query, conn, output_func=print, parameters=(),
              row_factory=namedtuple_row_factory, transform=None,
              chunk_size=CHUNKSIZE):
    """
    Call output_func(row) one-by-one on results of query.  See iter_rows for
    details.

    :param select_query: str, SQL query to execute
    :param conn: dbapi connection
    :param output_func: function to be called for each row (default is print)
    :param parameters: sequence or dict of bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param chunk_size: int, size of chunks to group data by
    """
    for row in iter_rows(select_query, conn, parameters=parameters,
                         row_factory=row_factory, transform=transform,
                         chunk_size=chunk_size):
        output_func(row)


def executemany(query, conn, rows, on_error=None, commit_chunks=True,
                chunk_size=CHUNKSIZE):
    """
    Use query to insert/update data from rows to database at conn.  This
    method uses the executemany or execute_batch (PostgreSQL) commands to
    process the data in chunks and avoid creating a new database connection for
    each row.  Row data are passed as parameters into query.

    Default behaviour is to raise an exception in the case of SQL errors such
    as primary key violations.  If the on_error parameter is specified, the
    exception will be caught then then rows of each chunk re-tried individually.
    Further errors will be caught and appended to a list of (row, exception)
    tuples.  on_error is a function that is called at the end of each chunk,
    with the list as the only argument.

    commit_chunks controls if chunks the transaction should be committed after
    each chunk has been inserted.  Committing chunks means that errors during
    a long-running insert do not require all data to be loaded again.  The
    disadvantage is that investigation may be required to determine exactly
    which records have been successfully transferred.

    :param query: str, SQL insert command with placeholders for data
    :param conn: dbapi connection
    :param rows: List of tuples containing data to be inserted/updated
    :param on_error: Function to be applied to failed rows in each chunk
    :param commit_chunks: bool, commit after each chunk has been inserted/updated
    :param chunk_size: int, size of chunks to group data by
    :return processed, failed: (int, int) number of rows processed, failed
    """
    logger.info("Executing many (chunk_size=%s)", chunk_size)
    logger.debug("Executing:\n\n%s\n\nagainst\n\n%s", query, conn)

    helper = DB_HELPER_FACTORY.from_conn(conn)
    processed = 0
    failed = 0

    with helper.cursor(conn) as cursor:
        for chunk in _chunker(rows, chunk_size):
            # Run query
            try:
                # Chunker pads to whole chunk with None; remove these
                chunk = [row for row in chunk if row is not None]

                # Show first row as example of data
                if processed == 0:
                    logger.debug(f"First row: {chunk[0]}")

                # Execute query
                helper.executemany(cursor, query, chunk)

            except helper.sql_exceptions as exc:
                # Rollback to clear the failed transaction before any others can
                # be started.
                conn.rollback()

                # Collect and process failed rows if on_error function provided
                if on_error:
                    # Temporarily disable logging
                    old_level = logger.level
                    logger.setLevel(logging.ERROR)

                    try:
                        failed_rows = _execute_by_row(query, conn, chunk)
                    finally:
                        # Restore logging
                        logger.setLevel(old_level)

                    failed += len(failed_rows)
                    logger.debug("Calling on_error function on %s failed rows",
                                 failed)
                    on_error(failed_rows)
                else:
                    msg = (f"SQL query raised an error.\n\n{query}\n\n"
                           f"Required paramstyle: {helper.paramstyle}\n\n{exc}\n")
                    raise ETLHelperInsertError(msg)

            processed += len(chunk)
            logger.info(
                '%s rows processed (%s failed)', processed, failed)

            # Commit changes so far
            if commit_chunks:
                conn.commit()

    # Commit changes where not already committed
    if not commit_chunks:
        conn.commit()

    logger.info(f'{processed} rows processed in total')
    return processed, failed


def _execute_by_row(query, conn, chunk):
    """
    Retry execution of rows individually and return failed rows along with
    their errors.  Successful inserts are committed.  This is because
    (and other?)

    :param query: str, SQL command with placeholders for data
    :param chunk: list, list of row parameters
    :param conn: open dbapi connection, used for transactions
    :returns failed_rows: list of (row, exception) tuples
    """
    FailedRow = namedtuple('FailedRow', 'row, exception')
    failed_rows = []

    for row in chunk:
        try:
            # Use etlhelper execute to isolate transactions
            execute(query, conn, parameters=row)
        except ETLHelperQueryError as exc:
            failed_rows.append(FailedRow(row, exc))

    return failed_rows


def copy_rows(select_query, source_conn, insert_query, dest_conn,
              parameters=(), row_factory=namedtuple_row_factory,
              transform=None, on_error=None, commit_chunks=True,
              read_lob=False, chunk_size=CHUNKSIZE):
    """
    Copy rows from source_conn to dest_conn.  select_query and insert_query
    specify the data to be transferred.

    Note: ODBC driver requires separate connections for source_conn and
    dest_conn, even if they represent the same database.

    Geometry columns from Oracle spatial should be selected with:
      SDO_UTIL.TO_WKTGEOMETRY(shape_bng) AS geom_wkt
    and inserted into PostGIS with:
      ST_GeomFromText(%s, 27700)

    Default behaviour is to raise an exception in the case of SQL errors such
    as primary key violations.  If the on_error parameter is specified, the
    exception will be caught then then rows of each chunk re-tried individually.
    Further errors will be caught and appended to a list of (row, exception)
    tuples.  on_error is a function that is called at the end of each chunk,
    with the list as the only argument.

    :param select_query: str, select rows from Oracle.
    :param source_conn: open dbapi connection
    :param insert_query:
    :param dest_conn: open dbapi connection
    :param parameters: sequence or dict of bind variables for select query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param on_error: Function to be applied to failed rows in each chunk
    :param commit_chunks: bool, commit after each chunk (see executemany)
    :param read_lob: bool, convert Oracle LOB objects to strings
    :param chunk_size: int, size of chunks to group data by
    :return processed, failed: (int, int) number of rows processed, failed
    """
    rows_generator = iter_rows(select_query, source_conn,
                               parameters=parameters, row_factory=row_factory,
                               transform=transform, read_lob=read_lob,
                               chunk_size=chunk_size)
    processed, failed = executemany(insert_query, dest_conn,
                                    rows_generator,
                                    on_error=on_error,
                                    commit_chunks=commit_chunks,
                                    chunk_size=chunk_size)
    return processed, failed


def execute(query, conn, parameters=()):
    """
    Run SQL query against connection.

    :param query: str, SQL query to execute
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    """
    logger.info("Executing query")
    logger.debug(f"Executing:\n\n{query}\n\nwith parameters:\n\n"
                 f"{parameters}\n\nagainst\n\n{conn}")

    helper = DB_HELPER_FACTORY.from_conn(conn)
    with helper.cursor(conn) as cursor:
        # Run query
        try:
            cursor.execute(query, parameters)
            conn.commit()
        except helper.sql_exceptions as exc:
            # Even though we haven't modified data, we have to rollback to
            # clear the failed transaction before any others can be started.
            conn.rollback()
            msg = (f"SQL query raised an error.\n\n{query}\n\n"
                   f"Required paramstyle: {helper.paramstyle}\n\n{exc}\n")
            raise ETLHelperQueryError(msg)


def copy_table_rows(table, source_conn, dest_conn, target=None,
                    row_factory=namedtuple_row_factory,
                    transform=None, on_error=None, commit_chunks=True,
                    read_lob=False, chunk_size=CHUNKSIZE):
    """
    Copy rows from 'table' in source_conn to same or target table in dest_conn.
    This is a simple copy of all columns and rows using `load` to insert data.
    It is possible to apply a transform e.g. to change the case of table names.
    For more control, use `copy_rows`.

    Note: ODBC driver requires separate connections for source_conn and
    dest_conn, even if they represent the same database.

    Default behaviour is to raise an exception in the case of SQL errors such
    as primary key violations.  If the on_error parameter is specified, the
    exception will be caught then then rows of each chunk re-tried individually.
    Further errors will be caught and appended to a list of (row, exception)
    tuples.  on_error is a function that is called at the end of each chunk,
    with the list as the only argument.

    :param source_conn: open dbapi connection
    :param dest_conn: open dbapi connection
    :param target: name of target table, if different from source
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param on_error: Function to be applied to failed rows in each chunk
    :param commit_chunks: bool, commit after each chunk (see executemany)
    :param read_lob: bool, convert Oracle LOB objects to strings
    :param chunk_size: int, size of chunks to group data by
    :param select_sql_suffix: str, SQL clause(s) to append to select statement
                              e.g. WHERE, ORDER BY, LIMIT
    :return processed, failed: (int, int) number of rows processed, failed
    """
    select_query = f"SELECT * FROM {table}"
    if not target:
        target = table

    rows_generator = iter_rows(select_query, source_conn, row_factory=row_factory,
                               transform=transform, read_lob=read_lob,
                               chunk_size=chunk_size)
    processed, failed = load(target, dest_conn, rows_generator, on_error=on_error,
                             commit_chunks=commit_chunks, chunk_size=chunk_size)
    return processed, failed


def load(table, conn, rows, on_error=None, commit_chunks=True,
         chunk_size=CHUNKSIZE):
    """
    Load data from iterable of named tuples or dictionaries into pre-existing
    table in database on conn.

    Default behaviour is to raise an exception in the case of SQL errors such
    as primary key violations.  If the on_error parameter is specified, the
    exception will be caught then then rows of each chunk re-tried individually.
    Further errors will be caught and appended to a list of (row, exception)
    tuples.  on_error is a function that is called at the end of each chunk,
    with the list as the only argument.

    :param table: name of table
    :param conn: open dbapi connection
    :param rows: iterable of named tuples or dictionaries of data
    :param on_error: Function to be applied to failed rows in each chunk
    :param commit_chunks: bool, commit after each chunk (see executemany)
    :param chunk_size: int, size of chunks to group data by
    :return processed, failed: (int, int) number of rows processed, failed
    """
    # Return early if rows is empty
    if not rows:
        return 0, 0

    # Get first row without losing it from row iteration, returning early if
    # the generator was empty.
    try:
        rows = iter(rows)
        first_row = next(rows)  # This line throws the exception if empty
        rows = chain([first_row], rows)
    except StopIteration:
        return 0, 0

    # Generate insert query
    query = generate_insert_sql(table, first_row, conn)

    # Insert data
    processed, failed = executemany(query, conn, rows,
                                    on_error=on_error,
                                    commit_chunks=commit_chunks,
                                    chunk_size=chunk_size)
    return processed, failed


def generate_insert_sql(table, row, conn):
    """Generate insert SQL for table, getting column names from row and the
    placeholder style from the connection.  `row` is either a namedtuple or
    a dictionary."""
    helper = DB_HELPER_FACTORY.from_conn(conn)
    paramstyles = {
        "qmark": "?",
        "numeric": ":{number}",
        "named": ":{name}",
        "format": "%s",
        "pyformat": "%({name})s"
    }

    # Namedtuples use a query with positional placeholders
    if not hasattr(row, 'keys'):
        paramstyle = helper.positional_paramstyle

        # Convert namedtuple to dictionary to easily access keys
        try:
            row = row._asdict()
        except AttributeError:
            msg = f"Row is not a dictionary or namedtuple ({type(row)})"
            raise ETLHelperInsertError(msg)

        columns = row.keys()
        if paramstyle == "numeric":
            placeholders = [paramstyles[paramstyle].format(number=i + 1)
                            for i in range(len(columns))]
        else:
            placeholders = [paramstyles[paramstyle]] * len(columns)

    # Dictionaries use a query with named placeholders
    else:
        paramstyle = helper.named_paramstyle
        if not paramstyle:
            msg = (f"Database connection ({str(conn.__class__)}) doesn't support named parameters.  "
                   "Pass data as namedtuples instead.")
            raise ETLHelperInsertError(msg)

        columns = row.keys()
        placeholders = [paramstyles[paramstyle].format(name=c) for c in columns]

    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

    return sql


def _chunker(iterable, n_chunks, fillvalue=None):
    """Collect data into fixed-length chunks or blocks.
    Code from recipe at https://docs.python.org/3.6/library/itertools.html
    """
    # _chunker('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n_chunks
    return zip_longest(*args, fillvalue=fillvalue)


def _read_lob(rows):
    """
    Replace Oracle LOB objects within rows with their string representation.
    :param rows: list of tuples of output data
    :return: list of tuples with LOB objects converted to strings
    """
    clean_rows = []
    for row in rows:
        clean_row = [x.read() if str(x.__class__) == "<class 'cx_Oracle.LOB'>"
                     else x for x in row]
        clean_rows.append(clean_row)

    return clean_rows
