"""
Functions for transferring data in and out of databases.
"""
from itertools import zip_longest, islice
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
                transform=None, read_lob=False):
    """
    Run SQL query against connection and return iterator object to loop over
    results in batches of etlhelper.etl.CHUNKSIZE (default 5000).

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
    """
    logger.info("Fetching rows")
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
            rows = cursor.fetchmany(CHUNKSIZE)

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
              transform=None, read_lob=False):
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
    """
    for chunk in iter_chunks(select_query, conn, row_factory=row_factory,
                             parameters=parameters, transform=transform,
                             read_lob=read_lob):
        for row in chunk:
            yield row


def get_rows(select_query, conn, parameters=(),
             row_factory=namedtuple_row_factory, transform=None):
    """
    Get results of query as a list.  See iter_rows for details.
    :param select_query: str, SQL query to execute
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    """
    return list(iter_rows(select_query, conn, row_factory=row_factory,
                          parameters=parameters, transform=transform))


def fetchone(select_query, conn, parameters=(),
             row_factory=namedtuple_row_factory, transform=None):
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
                                parameters=parameters, transform=transform))
    except StopIteration:
        result = None
    finally:
        # Commit to close the transaction before the iterator has been exhausted
        conn.commit()

    return result


def fetchmany(select_query, conn, size=1, parameters=(),
              row_factory=namedtuple_row_factory, transform=None):
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
    """
    try:
        result = list(
            islice(iter_rows(select_query, conn, row_factory=row_factory,
                             parameters=parameters, transform=transform), size))
    finally:
        # Commit to close the transaction before the iterator has been exhausted
        conn.commit()

    return result


def fetchall(select_query, conn, parameters=(),
             row_factory=namedtuple_row_factory, transform=None):
    """
    Get all results of query as a list.  See iter_rows for details.
    :param select_query: str, SQL query to execute
    :param conn: dbapi connection
    :param parameters: sequence or dict of bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    """
    return list(iter_rows(select_query, conn, row_factory=row_factory,
                          parameters=parameters, transform=transform))


def dump_rows(select_query, conn, output_func=print, parameters=(),
              row_factory=namedtuple_row_factory, transform=None):
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
    """
    for row in iter_rows(select_query, conn, parameters=parameters,
                         row_factory=row_factory, transform=transform):
        output_func(row)


def executemany(query, conn, rows, commit_chunks=True):
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
    :param conn: dbapi connection
    :param rows: List of tuples containing data to be inserted/updated
    :param commit_chunks: bool, commit after each chunk has been inserted/updated
    :return row_count: int, number of rows inserted/updated
    """
    logger.info(f"Executing many (chunksize={CHUNKSIZE})")
    logger.debug(f"Executing:\n\n{query}\n\nagainst\n\n{conn}")

    helper = DB_HELPER_FACTORY.from_conn(conn)
    processed = 0

    with helper.cursor(conn) as cursor:
        for chunk in _chunker(rows, CHUNKSIZE):
            # Run query
            try:
                # Chunker pads to whole chunk with None; remove these
                chunk = [row for row in chunk if row is not None]

                # Show first row as example of data
                if processed == 0:
                    logger.debug(f"First row: {chunk[0]}")

                # Execute query
                helper.executemany(cursor, query, chunk)
                processed += len(chunk)

            except helper.sql_exceptions as exc:
                # Rollback to clear the failed transaction before any others can
                # be # started.
                conn.rollback()
                msg = (f"SQL query raised an error.\n\n{query}\n\n"
                       f"Required paramstyle: {helper.paramstyle}\n\n{exc}\n")
                raise ETLHelperInsertError(msg)

            logger.info(
                f'{processed} rows processed')

            # Commit changes so far
            if commit_chunks:
                conn.commit()

    # Commit changes where not already committed
    if not commit_chunks:
        conn.commit()

    logger.info(f'{processed} rows processed in total')


def copy_rows(select_query, source_conn, insert_query, dest_conn,
              parameters=(), row_factory=namedtuple_row_factory,
              transform=None, commit_chunks=True,
              read_lob=False):
    """
    Copy rows from source_conn to dest_conn.  select_query and insert_query
    specify the data to be transferred.

    Note: ODBC driver requires separate connections for source_conn and
    dest_conn, even if they represent the same database.

    Geometry columns from Oracle spatial should be selected with:
      SDO_UTIL.TO_WKTGEOMETRY(shape_bng) AS geom_wkt
    and inserted into PostGIS with:
      ST_GeomFromText(%s, 27700)

    :param select_query: str, select rows from Oracle.
    :param source_conn: open dbapi connection
    :param insert_query:
    :param dest_conn: open dbapi connection
    :param parameters: sequence or dict of bind variables for select query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts an iterable (e.g. list) of rows and
                      returns an iterable of rows (possibly of different shape)
    :param commit_chunks: bool, commit after each chunk (see executemany)
    :param read_lob: bool, convert Oracle LOB objects to strings
    """
    rows_generator = iter_rows(select_query, source_conn,
                               parameters=parameters, row_factory=row_factory,
                               transform=transform, read_lob=read_lob)
    executemany(insert_query, dest_conn, rows_generator,
                commit_chunks=commit_chunks)


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
