"""
Functions for transferring data in and out of databases.
"""
import logging
import re
from copy import deepcopy
from itertools import (
    zip_longest,
    chain,
)
from typing import (
    Any,
    Callable,
    Collection,
    Iterable,
    Iterator,
    NamedTuple,
    Optional,
    Union,
)

from etlhelper.abort import (
    raise_for_abort,
    clear_abort_event,
)
from etlhelper.db_helper_factory import DB_HELPER_FACTORY
from etlhelper.exceptions import (
    ETLHelperBadIdentifierError,
    ETLHelperExtractError,
    ETLHelperInsertError,
    ETLHelperQueryError,
)
from etlhelper.row_factories import dict_row_factory
from etlhelper.types import (
    Connection,
    Row,
    Chunk,
)

logger = logging.getLogger('etlhelper')
CHUNKSIZE = 5000


class FailedRow(NamedTuple):
    row: Row
    exception: Exception


def iter_chunks(
        select_query: str,
        conn: Connection,
        parameters: tuple = (),
        row_factory: Callable = dict_row_factory,
        transform: Optional[Callable[[Chunk], Chunk]] = None,
        chunk_size: int = CHUNKSIZE
        ) -> Iterator[Chunk]:
    """
    Run SQL query against connection and return iterator object to loop over
    results in batches of chunksize (default 5000).

    The row_factory changes the output format of the results.  Other row
    factories e.g. dict_row_factory are available.

    The transform function is applied to chunks of data as they are extracted
    from the database.

    All data extraction functions call this function, directly or indirectly.

    :param select_query: SQL query to execute
    :param conn: dbapi connection
    :param parameters: bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts a list of rows and
                      returns an list of rows (possibly of different shape)
    :param chunk_size: size of chunks to group data by
    :return: generator returning a list of objects which each
             represent a row of data using the given row_factory
    :raises ETLHelperExtractError: if SQL raises an error
    """
    logger.info("Fetching rows (chunk_size=%s)", chunk_size)
    logger.debug(f"Fetching:\n\n{select_query}\n\nwith parameters:\n\n"
                 f"{parameters}\n\nagainst:\n\n{conn}")
    clear_abort_event()

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
            raise_for_abort("abort_etlhelper_threads() called during iter_chunks")

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

            # Apply row_factory
            rows = (create_row(row) for row in rows)

            # Apply transform
            if transform:
                rows = transform(rows)

            # Return data
            yield rows
            first_pass = False


def iter_rows(
        select_query: str,
        conn: Connection,
        parameters: tuple = (),
        row_factory: Callable = dict_row_factory,
        transform: Optional[Callable[[Chunk], Chunk]] = None,
        chunk_size: int = CHUNKSIZE
        ) -> Iterator[Row]:
    """
    Run SQL query against connection and return iterator object to loop over
    results, row-by-row.

    :param select_query: SQL query to execute
    :param conn: dbapi connection
    :param parameters: bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts a list of rows and
                      returns an list of rows (possibly of different shape)
    :param chunk_size: size of chunks to group data by
    :return: generator returning a list of objects which each
             represent a row of data using the given row_factory
    """
    for chunk in iter_chunks(select_query, conn, row_factory=row_factory,
                             parameters=parameters, transform=transform,
                             chunk_size=chunk_size):
        for row in chunk:
            yield row


def fetchone(
        select_query: str,
        conn: Connection,
        parameters: tuple = (),
        row_factory: Callable = dict_row_factory,
        transform: Optional[Callable[[Chunk], Chunk]] = None,
        chunk_size: int = 1
        ) -> Optional[Row]:
    """
    Get first result of query.  See iter_rows for details.  Note: iter_rows is
    recommended for looping over rows individually.

    :param select_query: SQL query to execute
    :param conn: dbapi connection
    :param parameters: bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts a list of rows and
                      returns an list of rows (possibly of different shape)
    :param chunk_size: size of chunks to group data by
    :return: None or a row of data using the given row_factory
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


def fetchall(
        select_query: str,
        conn: Connection,
        parameters: tuple = (),
        row_factory: Callable = dict_row_factory,
        transform: Optional[Callable[[Chunk], Chunk]] = None,
        chunk_size: int = CHUNKSIZE
        ) -> Chunk:
    """
    Get all results of query as a list.  See iter_rows for details.

    :param select_query: SQL query to execute
    :param conn: dbapi connection
    :param parameters: bind variables to insert in the query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts a list of rows and
                      returns an list of rows (possibly of different shape)
    :param chunk_size: size of chunks to group data by
    :return: a row of data using the given row_factory
    """
    return list(iter_rows(select_query, conn, row_factory=row_factory,
                          parameters=parameters, transform=transform,
                          chunk_size=chunk_size))


def executemany(
        query: str,
        conn: Connection,
        rows: Iterable[Row],
        transform: Optional[Callable[[Chunk], Chunk]] = None,
        on_error: Optional[Callable[[list[FailedRow]], Any]] = None,
        commit_chunks: bool = True,
        chunk_size: int = CHUNKSIZE,
        ) -> tuple[int, int]:
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

    :param query: SQL insert command with placeholders for data
    :param conn: dbapi connection
    :param rows: an iterable of rows containing data to be inserted/updated
    :param transform: function that accepts a list of rows and
                      returns an list of rows (possibly of different shape)
    :param on_error: function to be applied to failed rows in each chunk
    :param commit_chunks: commit after each chunk has been inserted/updated
    :param chunk_size: size of chunks to group data by
    :return: the number of rows processed and the number of rows failed
    :raises ETLHelperInsertError: if SQL raises an error
    """
    logger.info("Executing many (chunk_size=%s)", chunk_size)
    logger.debug("Executing:\n\n%s\n\nagainst:\n\n%s", query, conn)
    clear_abort_event()

    helper = DB_HELPER_FACTORY.from_conn(conn)
    processed = 0
    failed = 0

    with helper.cursor(conn) as cursor:
        for chunk_with_nones in _chunker(rows, chunk_size):
            raise_for_abort("abort_etlhelper_threads() called during executemany")

            # Chunker pads to whole chunk with None; remove these
            chunk = [row for row in chunk_with_nones if row is not None]

            # Apply transform
            if transform:
                # Materialise chunk as list as `transform` can return generator
                chunk = list(transform(chunk))

            # Show first row as example of data
            if processed == 0:
                logger.debug(f"First row: {chunk[0]}")

            # Run query
            try:
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


def _execute_by_row(
        query: str,
        conn: Connection,
        chunk: Chunk
        ) -> list[FailedRow]:
    """
    Retry execution of rows individually and return failed rows along with
    their errors. Successful inserts are committed.

    :param query: SQL query with placeholders for data
    :param conn: dbapi connection
    :param chunk: list of rows
    :return: a list failed rows
    """
    failed_rows: list[FailedRow] = []

    for row in chunk:
        try:
            # Use etlhelper execute to isolate transactions
            execute(query, conn, parameters=row)
        except ETLHelperQueryError as exc:
            failed_rows.append(FailedRow(row, exc))

    return failed_rows


def copy_rows(
        select_query: str,
        source_conn: Connection,
        insert_query: str,
        dest_conn: Connection,
        parameters: tuple = (),
        row_factory: Callable = dict_row_factory,
        transform: Optional[Callable[[Chunk], Chunk]] = None,
        on_error: Optional[Callable] = None,
        commit_chunks: bool = True,
        chunk_size: int = CHUNKSIZE,
        ) -> tuple[int, int]:
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

    :param select_query: SQL query to select data
    :param source_conn: dbapi connection
    :param insert_query: SQL query to insert data
    :param dest_conn: dbapi connection
    :param parameters: bind variables to insert in the select query
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts a list of rows and
                      returns an list of rows (possibly of different shape)
    :param on_error: function to be applied to failed rows in each chunk
    :param commit_chunks: commit after each chunk (see executemany)
    :param chunk_size: size of chunks to group data by
    :return: the number of rows processed and the number of rows failed
    """
    rows_generator = iter_rows(select_query, source_conn,
                               parameters=parameters, row_factory=row_factory,
                               transform=transform, chunk_size=chunk_size)
    processed, failed = executemany(insert_query, dest_conn,
                                    rows_generator,
                                    on_error=on_error,
                                    commit_chunks=commit_chunks,
                                    chunk_size=chunk_size)
    return processed, failed


def execute(
        query: str,
        conn: Connection,
        parameters: Collection[Any] = ()
        ) -> None:
    """
    Run SQL query against connection.

    :param query: SQL query to execute
    :param conn: dbapi connection
    :param parameters: bind variables to insert in the query
    :raises ETLHelperQueryError: if SQL raises an error
    """
    logger.info("Executing query")
    logger.debug(f"Executing:\n\n{query}\n\nwith parameters:\n\n"
                 f"{parameters}\n\nagainst:\n\n{conn}")

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


def copy_table_rows(
        table: str,
        source_conn: Connection,
        dest_conn: Connection,
        target: Optional[str] = None,
        row_factory: Callable = dict_row_factory,
        transform: Optional[Callable[[Chunk], Chunk]] = None,
        on_error: Optional[Callable] = None,
        commit_chunks: bool = True,
        chunk_size: int = CHUNKSIZE
        ) -> tuple[int, int]:
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

    :param table: name of table
    :param source_conn: dbapi connection
    :param dest_conn: dbapi connection
    :param target: name of target table, if different from source
    :param row_factory: function that accepts a cursor and returns a function
                        for parsing each row
    :param transform: function that accepts a list of rows and
                      returns an list of rows (possibly of different shape)
    :param on_error: function to be applied to failed rows in each chunk
    :param commit_chunks: commit after each chunk (see executemany)
    :param chunk_size: size of chunks to group data by
    :return: the number of rows processed and the number of rows failed
    """
    validate_identifier(table)

    select_query = f"SELECT * FROM {table}"
    if not target:
        target = table

    rows_generator = iter_rows(select_query, source_conn, row_factory=row_factory,
                               transform=transform, chunk_size=chunk_size)
    processed, failed = load(target, dest_conn, rows_generator, on_error=on_error,
                             commit_chunks=commit_chunks, chunk_size=chunk_size)
    return processed, failed


def load(
        table: str,
        conn: Connection,
        rows: Iterable[Row],
        transform: Optional[Callable[[Chunk], Chunk]] = None,
        on_error: Optional[Callable] = None,
        commit_chunks: bool = True,
        chunk_size: int = CHUNKSIZE,
        ) -> tuple[int, int]:
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
    :param conn: dbapi connection
    :param rows: iterable of named tuples or dictionaries of data
    :param transform: function that accepts a list of rows and
                      returns an list of rows (possibly of different shape)
    :param on_error: function to be applied to failed rows in each chunk
    :param commit_chunks: commit after each chunk (see executemany)
    :param chunk_size: size of chunks to group data by
    :return: the number of rows processed and the number of rows failed
    """
    # Return early if rows is empty
    if not rows:
        return 0, 0

    # Get first row without losing it from row iteration, returning early if
    # the generator was empty.
    try:
        rows = iter(rows)
        first_row = next(rows)  # This line throws an exception if empty
        rows = chain([first_row], rows)

        if transform:
            # next(iter(var)) is equivalent to var[0] but also works for generators,
            # which may be returned by the transform.
            # We need a deepcopy to stop the first row being transformed twice.
            first_row_transformed = next(iter(transform([deepcopy(first_row)])))
        else:
            first_row_transformed = first_row

    except StopIteration:
        return 0, 0

    # Generate insert query
    query = generate_insert_sql(table, first_row_transformed, conn)

    # Insert data
    processed, failed = executemany(
        query,
        conn,
        rows,
        transform=transform,
        on_error=on_error,
        commit_chunks=commit_chunks,
        chunk_size=chunk_size,
    )
    return processed, failed


def generate_insert_sql(
        table: str,
        row: Row,
        conn: Connection
        ) -> str:
    """Generate insert SQL for table, getting column names from row and the
    Generate insert SQL for table, getting column names from row and the
    placeholder style from the connection.  `row` is either a namedtuple or
    a dictionary.

    :param table: name of table
    :param row: a single row as a namedtuple or dict
    :param conn: dbapi connection
    :return: SQL statement to insert data into the given table
    :raises ETLHelperInsertError: if 'row' is not a namedtuple or a dict,
                                  or if the database connection encounters a
                                  parameter error
    """
    helper = DB_HELPER_FACTORY.from_conn(conn)
    paramstyles = {
        "qmark": "?",
        "numeric": ":{number}",
        "named": ":{name}",
        "format": "%s",
        "pyformat": "%({name})s"
    }

    # Namedtuples use a query with positional placeholders
    if hasattr(row, '_asdict'):
        paramstyle = helper.positional_paramstyle

        # Convert namedtuple to dictionary to easily access keys
        row_dict = row._asdict()
        columns = row_dict.keys()
        if paramstyle == "numeric":
            placeholders = [paramstyles[paramstyle].format(number=i + 1)
                            for i in range(len(columns))]
        else:
            placeholders = [paramstyles[paramstyle]] * len(columns)

    # Dictionaries use a query with named placeholders
    elif hasattr(row, 'keys'):
        paramstyle = helper.named_paramstyle
        if not paramstyle:
            msg = (f"Database connection ({str(conn.__class__)}) doesn't support named parameters.  "
                   "Pass data as namedtuples instead.")
            raise ETLHelperInsertError(msg)

        columns = row.keys()
        placeholders = [paramstyles[paramstyle].format(name=c) for c in columns]

    else:
        msg = f"Row is not a dictionary or namedtuple ({type(row)})"
        raise ETLHelperInsertError(msg)

    # Validate identifiers to prevent malicious code injection
    for identifier in (table, *columns):
        validate_identifier(identifier)

    # Generate insert statement
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

    return sql


def validate_identifier(identifier: str) -> None:
    """
    Validate characters used in identifier e.g. table or column name.
    Identifiers must comprise alpha-numeric characters, plus `_` or `$` and
    cannot start with `$`, or numbers.

    :param identifier: a database identifier
    :raises ETLHelperBadIdentifierError: if the 'identifier' contains invalid
                                         characters
    """
    # Identifier rules are based on PostgreSQL specifications, defined here:
    # https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

    # `\w` represents all alphanumeric characters (including unicode) plus `_`
    # `(?![0-9])` is a "negative-lookahead assertion" to remove numbers from
    # the match for the first character.
    # The regex comprises two very similar groups.  The first is optional and
    # ends with a dot.  This represents the schema name.
    regex = re.compile(r"((?![0-9])[\w][\w$]*\.?)?((?![0-9])[\w][\w$]*)$")

    if not regex.match(identifier):
        msg = f"'{identifier}' contains invalid characters."
        raise ETLHelperBadIdentifierError(msg)


def _chunker(
        iterable: Iterable[Row],
        n_chunks: int,
        ) -> Iterator[tuple[Union[Row, None], ...]]:
    """Collect data into fixed-length chunks or blocks.
    Code from recipe at https://docs.python.org/3.6/library/itertools.html

    :param iterable: an iterable object
    :param n_chunks: the number of values in each chunk
    :return: generator returning tuples of rows, of length n_chunks,
             where empty values are filled using None
    """
    # _chunker((A,B,C,D,E,F,G), 3) --> (A,B,C) (D,E,F) (G,None,None)
    args = [iter(iterable)] * n_chunks
    return zip_longest(*args, fillvalue=None)
