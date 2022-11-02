"""
Tests for implementation of the abort() function in both extracting and
loading data.
"""
from concurrent.futures import ThreadPoolExecutor
import logging
import sqlite3
from time import sleep

import pytest

from etlhelper import execute, executemany, fetchall, abort
from etlhelper.exceptions import ETLHelperAbort

logger = logging.getLogger('abort_test')
logger.setLevel(logging.INFO)


def test_abort_on_fetchall(tmpdir, caplog):
    """
    Fetch one row at a time from temporary database within a thread pool,
    call abort() then assert that exception was raised and not all
    rows were returned.
    """
    # Arrange
    # Create and populate a temporary SQLite database
    db = tmpdir / 'test.db'

    with sqlite3.connect(db) as conn:
        execute("CREATE TABLE test (id INTEGER)", conn)
        rows = ((row,) for row in range(10))
        executemany("INSERT INTO test VALUES (?)", conn, rows=rows)

    def transform(chunk):
        logger.info("Fetching row")
        # Add a delay to result fetching to give time for abort call
        sleep(0.1)
        return chunk

    def do_etl():
        # Function to perform fetchall
        with sqlite3.connect(db) as conn:
            result = fetchall("SELECT * FROM test", conn,
                              transform=transform, chunk_size=1)
        return result

    # Act
    # Do the ETL within a thread confirm it was aborted
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(do_etl)

        # Call abort after short delay
        sleep(0.2)
        abort()

        # Exception raised when result is retrieved
        with pytest.raises(ETLHelperAbort, match="iter_chunks"):
            _ = future.result()

    # The number of records is a proxy for the rows returned
    assert len(caplog.records) < 10

    # Redo the ETL without abort to confirm that abort event was cleared
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(do_etl)
        result = future.result()

    assert len(result) == 10
