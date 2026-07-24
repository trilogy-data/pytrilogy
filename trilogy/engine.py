from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import TextClause

from trilogy.core.models.environment import Environment

# How a literal `:` inside a SQL string literal is escaped so SQLAlchemy's
# text() does not read it as a bind parameter (e.g. the `:s` in 'http(?:s)?').
# This is a CONTRACT between the executor and engine adapters: text() unescapes
# it back to `:` at compile time, so any adapter that executes a TextClause's
# raw .text WITHOUT compiling must call `unescape_literal_colons` itself.
LITERAL_COLON_ESCAPE = "\\:"


def escape_literal_colons(sql: str) -> str:
    """Escape `:` as LITERAL_COLON_ESCAPE inside single-quoted string literals.

    Colons outside literals are left alone and still bind as parameters."""
    out: list[str] = []
    in_string = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if not in_string:
            if ch == "'":
                in_string = True
            out.append(ch)
        elif ch == "'" and sql[i + 1 : i + 2] == "'":
            out.append("''")
            i += 1
        elif ch == "'":
            in_string = False
            out.append(ch)
        elif ch == "\\" and i + 1 < len(sql):
            # escape-char dialects (BigQuery/Snowflake) emit \' and \\ pairs
            out.append(sql[i : i + 2])
            i += 1
        elif ch == ":":
            out.append(LITERAL_COLON_ESCAPE)
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def unescape_literal_colons(sql: str) -> str:
    """Undo `escape_literal_colons`, mirroring SQLAlchemy's compile-time
    unescaping — for paths that execute SQL text without compiling it."""
    return sql.replace(LITERAL_COLON_ESCAPE, ":")


class ResultProtocol(Protocol):

    def fetchall(self) -> list[Any]: ...

    def keys(self) -> list[str]: ...

    def fetchone(self) -> Any | None: ...

    def fetchmany(self, size: int) -> list[Any]: ...

    def __iter__(self) -> Iterator[Any]: ...


class EngineConnection(Protocol):

    def execute(
        self, statement: str | TextClause, parameters: Any | None = None
    ) -> ResultProtocol:
        pass

    def commit(self):
        raise NotImplementedError()

    def begin(self):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def close(self) -> None:
        return


class ExecutionEngine(Protocol):

    def connect(self) -> EngineConnection:
        pass

    def setup(self, env: Environment, connection):
        pass

    def dispose(self, close: bool = True):
        pass
