"""
Tests for implementation of the abort() function in both extracting and
loading data.
"""
import queue
import sqlite3
import threading
from time import sleep

import pytest

from etlhelper import execute, executemany, load, fetchall, abort


def test_abort_on_fetchall(tmpdir):
    # Arrange
    db = tmpdir / 'test.db'

    with sqlite3.connect(db) as conn:
        execute("CREATE TABLE test (id INTEGER)", conn)
        rows = ((row,) for row in range(10))
        executemany("INSERT INTO test VALUES (?)", conn, rows=rows)

    def transform(chunk):
        # Add a delay to result fetching to give time for abort call
        sleep(0.01)
        return chunk

    def do_fetchall(result_queue):
        # Function to perform fetchall
        with sqlite3.connect(db) as conn:
            result = fetchall("SELECT * FROM test", conn,
                              transform=transform, chunk_size=1)
            result_queue.put(result)

    # Act
    result_queue = queue.SimpleQueue()  # Thread-safe store for result
    etl_thread = threading.Thread(target=do_fetchall, args=(result_queue,))
    etl_thread.start()

    # Assert
    assert len(result_queue.get()) == 10
