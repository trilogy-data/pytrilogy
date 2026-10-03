"""pytest plugin: record every top-level compiled SQL statement per test.

    SQLCAP_OUT=<file.jsonl> PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p sqlcap tests ...

One JSON line per test: {nodeid, outcome, sql: [...]}. See README.md.
"""

from __future__ import annotations

import json
import os
from typing import IO, Any

import pytest

_current: dict[str, Any] = {"nodeid": None, "sql": [], "depth": 0, "outcome": "passed"}
_out: IO[str] | None = None


def _wrap(cls: type, name: str) -> None:
    orig = cls.__dict__[name]

    def wrapper(self, *args, **kwargs):
        _current["depth"] += 1
        try:
            result = orig(self, *args, **kwargs)
        finally:
            _current["depth"] -= 1
        # a statement compiled while compiling another (a chart's layers) is
        # part of the outer one
        if _current["depth"] == 0 and isinstance(result, str):
            _current["sql"].append(result)
        return result

    setattr(cls, name, wrapper)


def _all_subclasses(cls: type) -> list[type]:
    seen: list[type] = []
    stack = [cls]
    while stack:
        for sub in stack.pop().__subclasses__():
            if sub not in seen:
                seen.append(sub)
                stack.append(sub)
    return seen


def pytest_configure(config) -> None:
    global _out
    path = os.environ.get("SQLCAP_OUT")
    if not path:
        return
    # held for the session, closed in pytest_unconfigure
    _out = open(path, "w", encoding="utf-8", newline="\n")  # noqa: SIM115
    import trilogy.dialect.bigquery
    import trilogy.dialect.duckdb
    import trilogy.dialect.postgres
    import trilogy.dialect.presto
    import trilogy.dialect.snowflake
    import trilogy.dialect.sql_server  # noqa: F401
    from trilogy.dialect.base import BaseDialect

    for cls in [BaseDialect, *_all_subclasses(BaseDialect)]:
        if "compile_statement" in cls.__dict__:
            _wrap(cls, "compile_statement")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    _current["nodeid"] = item.nodeid
    _current["sql"] = []
    _current["outcome"] = "passed"
    yield
    if _out is not None:
        _out.write(
            json.dumps(
                {
                    "nodeid": item.nodeid,
                    "outcome": _current["outcome"],
                    "sql": _current["sql"],
                }
            )
            + "\n"
        )
        _out.flush()


def pytest_runtest_logreport(report) -> None:
    if report.failed:
        _current["outcome"] = "failed"
    elif report.skipped and _current["outcome"] != "failed":
        _current["outcome"] = "skipped"


def pytest_unconfigure(config) -> None:
    if _out is not None:
        _out.close()
