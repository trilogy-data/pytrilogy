"""pytest plugin: append the sorted rows of every `execute_text` result to
$KS_ROWDUMP as JSONL, so a plan change can be checked against its rows."""

import json
import os

from trilogy.executor import Executor

_PATH = os.environ.get("KS_ROWDUMP")
_original = Executor.execute_text


class _Rows:
    """A fetched cursor: the rows were read to be logged, so replay them."""

    def __init__(self, rows, cursor):
        self._rows = rows
        self._cursor = cursor

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def __getattr__(self, name):
        return getattr(self._cursor, name)


def _logged(cursor, test: str):
    try:
        rows = cursor.fetchall()
    except Exception:
        return cursor
    with open(_PATH, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"test": test, "rows": sorted(map(repr, rows))}) + "\n")
    return _Rows(rows, cursor)


def _dumping(self, *args, **kwargs):
    test = os.environ.get("PYTEST_CURRENT_TEST", "").split(" ")[0]
    return [_logged(cursor, test) for cursor in _original(self, *args, **kwargs)]


if _PATH:
    Executor.execute_text = _dumping
