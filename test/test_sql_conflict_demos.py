"""Check that the SQL conflict examples produce the documented SQLite rows."""
import runpy
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest


DEMOS_DIR = Path(__file__).resolve().parents[1] / "docs" / "code_demos" / "error_handling"


@pytest.mark.parametrize("filename,grain_size", [
    ("demo_sql_conflict_error.py", "fine"),
    ("demo_sql_conflict_update.py", "fine-grained"),
])
def test_sql_conflict_demo(filename, grain_size, tmp_path, monkeypatch, capsys):
    """Run the actual demos in isolation and check both their data and output."""
    monkeypatch.chdir(tmp_path)

    namespace = runpy.run_path(str(DEMOS_DIR / filename), run_name="__main__")
    namespace["conn"].close()

    with closing(sqlite3.connect(tmp_path / "igneous_rocks.db")) as conn:
        assert conn.execute("SELECT * FROM igneous_rock").fetchall() == [
            (1, "basalt", grain_size)
        ]
    assert capsys.readouterr().out == (
        f"{{'id': 1, 'name': 'basalt', 'grain_size': '{grain_size}'}}\n"
    )
