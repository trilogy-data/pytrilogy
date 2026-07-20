"""In-process chdb adapter exposing the trilogy EngineConnection / ExecutionEngine
protocols. Used for unit-testing the ClickHouse dialect without a server.

Server-mode ClickHouse goes through SQLAlchemy via clickhouse-sqlalchemy instead.
"""

from __future__ import annotations

import json
from collections import namedtuple
from datetime import date, datetime
from typing import Any, Callable, Generator, List, Optional

from trilogy.core.models.environment import Environment
from trilogy.engine import (
    EngineConnection,
    ExecutionEngine,
    ResultProtocol,
    unescape_literal_colons,
)


def _strip_modifiers(ch_type: str) -> str:
    """Peel Nullable(...) / LowCardinality(...) wrappers and parametric args."""
    t = ch_type.strip()
    while True:
        for wrapper in ("Nullable(", "LowCardinality("):
            if t.startswith(wrapper):
                t = t[len(wrapper) : -1].strip()
                break
        else:
            break
    paren = t.find("(")
    return t[:paren] if paren != -1 else t


def _parse_datetime(s: str) -> datetime:
    # CH DateTime64 emits "YYYY-MM-DD HH:MM:SS[.fff]"; fromisoformat needs a 'T'.
    return datetime.fromisoformat(s.replace(" ", "T"))


_COERCERS: dict[str, Callable[[Any], Any]] = {
    "Date": lambda v: date.fromisoformat(v) if isinstance(v, str) else v,
    "Date32": lambda v: date.fromisoformat(v) if isinstance(v, str) else v,
    "DateTime": lambda v: _parse_datetime(v) if isinstance(v, str) else v,
    "DateTime64": lambda v: _parse_datetime(v) if isinstance(v, str) else v,
}


def _coerce_value(ch_type: str, value: Any) -> Any:
    if value is None:
        return None
    coercer = _COERCERS.get(_strip_modifiers(ch_type))
    return coercer(value) if coercer else value


def _row_class_for(columns: tuple[str, ...]) -> type:
    # rename=True replaces invalid identifiers with _0, _1, ... so column names
    # like "local.x" don't blow up. Index access still works regardless.
    return namedtuple("ChdbRow", columns, rename=True)


class ChdbResult(ResultProtocol):
    def __init__(self, columns: List[str], rows: List[tuple]):
        self._columns = columns
        if columns:
            row_cls = _row_class_for(tuple(columns))
            self._rows = [row_cls(*r) for r in rows]
        else:
            self._rows = list(rows)
        self._cursor = 0

    def fetchall(self) -> List[Any]:
        remaining = self._rows[self._cursor :]
        self._cursor = len(self._rows)
        return remaining

    def fetchone(self) -> Optional[Any]:
        if self._cursor >= len(self._rows):
            return None
        row = self._rows[self._cursor]
        self._cursor += 1
        return row

    def fetchmany(self, size: int) -> List[Any]:
        end = min(self._cursor + size, len(self._rows))
        chunk = self._rows[self._cursor : end]
        self._cursor = end
        return chunk

    def keys(self) -> List[str]:
        return list(self._columns)

    def __iter__(self) -> Generator[Any, None, None]:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row


def _statement_to_sql(statement: Any, parameters: Any | None) -> str:
    """Resolve a SQLAlchemy TextClause or raw string to a final SQL string.

    Parameters are inlined via SQLAlchemy's literal_binds compiler since chdb
    does not support bound parameters in its Python API.
    """
    text_value = getattr(statement, "text", None)
    if text_value is None:
        return str(statement)
    if not parameters:
        # A TextClause's raw .text still carries the executor's literal-colon
        # escapes; SQLAlchemy unescapes them at compile time, which this
        # branch skips.
        return unescape_literal_colons(str(text_value))
    from sqlalchemy import bindparam
    from sqlalchemy import text as sa_text

    bound = sa_text(text_value)
    bound = bound.bindparams(*[bindparam(k, v) for k, v in parameters.items()])
    compiled = bound.compile(compile_kwargs={"literal_binds": True})
    return str(compiled)


class ChdbConnection(EngineConnection):
    def __init__(self, path: str | None):
        self._path = path
        self._session: Any | None = None

    def _get_session(self) -> Any:
        if self._session is None:
            try:
                from chdb.session import Session
            except ImportError as exc:
                raise ImportError(
                    "chdb is not installed. Install with: python -m pip install chdb"
                ) from exc
            self._session = Session(self._path) if self._path else Session()
        return self._session

    def execute(self, statement: Any, parameters: Any | None = None) -> ResultProtocol:
        sql = _statement_to_sql(statement, parameters)
        # Use JSON format so we get both column metadata and row data.
        raw = self._get_session().query(sql, "JSON")
        text_out = str(raw).strip()
        if not text_out:
            return ChdbResult([], [])
        payload = json.loads(text_out)
        meta = payload.get("meta") or []
        columns = [m["name"] for m in meta]
        types = [m.get("type", "") for m in meta]
        data = payload.get("data") or []
        rows = [
            tuple(_coerce_value(t, row.get(c)) for c, t in zip(columns, types))
            for row in data
        ]
        return ChdbResult(columns, rows)

    def commit(self) -> None:
        # chdb is a single in-process session; no transactional semantics.
        return None

    def begin(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class ChdbEngine(ExecutionEngine):
    def __init__(self, path: str | None = None):
        self._path = path
        self._connection: ChdbConnection | None = None

    def connect(self) -> EngineConnection:
        if self._connection is None:
            self._connection = ChdbConnection(self._path)
        return self._connection

    def setup(self, env: Environment, connection: Any) -> None:
        return None

    def dispose(self, close: bool = True) -> None:
        if close and self._connection is not None:
            self._connection.close()
            self._connection = None
