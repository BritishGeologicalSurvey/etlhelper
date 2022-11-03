"""
Tests for implementation of the abort() function in both extracting and
loading data.
"""
from concurrent.futures import ThreadPoolExecutor
import logging
import sqlite3
from time import sleep

import pytest

from etlhelper import (
    abort_etlhelper_threads,
    execute,
    executemany,
    fetchall,
)
from etlhelper.exceptions import ETLHelperAbort

logger = logging.getLogger('abort_test')
logger.setLevel(logging.INFO)


def do_fetchall_etl(db):
    # Example of a fetchall ETL job

    def transform(chunk):
        logger.info("Processing chunk")
        # Add a delay to result fetching to give time for abort call
        sleep(0.1)
        return chunk

    with sqlite3.connect(db) as conn:
        result = fetchall("SELECT * FROM test", conn,
                          transform=transform, chunk_size=1)

    return result


@pytest.mark.parametrize("do_etl", [do_fetchall_etl])
def test_abort_etlhelper_threads(do_etl, tmpdir, caplog):
    """
    Transfer one row at a time to/from temporary database within a thread pool,
    call abort() then assert that exception was raised and not all rows were
    processed.
    """
    # Arrange
    # Create and populate a temporary SQLite database
    db = tmpdir / 'test.db'

    with sqlite3.connect(db) as conn:
        execute("CREATE TABLE test (id INTEGER)", conn)
        rows = ((row,) for row in range(10))
        executemany("INSERT INTO test VALUES (?)", conn, rows=rows)

    # Act and assert
    # Do the ETL within a thread confirm it was aborted
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(do_etl, db)

        # Call abort after short delay
        sleep(0.2)
        abort_etlhelper_threads()

        # Exceptions from threads are raised when result is retrieved
        with pytest.raises(ETLHelperAbort, match="iter_chunks"):
            _ = future.result()

    # The number of log records is a proxy for the rows handled
    assert len(caplog.records) < 10

    # Redo the ETL without abort to confirm that abort event was cleared
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(do_etl)
        result = future.result()

    assert len(result) == 10
