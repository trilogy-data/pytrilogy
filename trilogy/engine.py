from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import TextClause

    from trilogy.core.statements.execute import ProcessedQueryPersist
    from trilogy.executor import Executor

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


def statement_to_sql(statement: Any, parameters: Any | None = None) -> str:
    """Resolve a SQLAlchemy TextClause or raw string to a final SQL string.

    For engine adapters that talk to a driver directly rather than through
    SQLAlchemy. Parameters are inlined via the literal_binds compiler.
    """
    from sqlalchemy import bindparam
    from sqlalchemy import text as sa_text
    from sqlalchemy.sql.elements import TextClause

    if not isinstance(statement, TextClause):
        return str(statement)
    if not parameters:
        # A TextClause's raw .text still carries the executor's literal-colon
        # escapes; SQLAlchemy unescapes them at compile time, which this
        # branch skips.
        return unescape_literal_colons(statement.text)
    bound = sa_text(statement.text).bindparams(
        *[bindparam(k, v) for k, v in parameters.items()]
    )
    return str(bound.compile(compile_kwargs={"literal_binds": True}))


class ResultProtocol(Protocol):

    @property
    def returns_rows(self) -> bool:
        """Whether the statement produced a result set. Mirrors SQLAlchemy's
        CursorResult attribute; adapters that always return rows report True."""
        return True

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

    def in_transaction(self) -> bool:
        """Whether a transaction is currently open. Engines with no
        transactional semantics report False and never need a commit."""
        return False

    def get_transaction(self) -> Any:
        """The open transaction, if the engine exposes one as an object."""
        return None

    def close(self) -> None:
        return


class NonTransactionalConnection(EngineConnection):
    """Base for engines with no transaction semantics to expose.

    BigQuery has none outside a script; chdb is a single in-process session.
    Because ``in_transaction`` never becomes True, the executor never adopts an
    implicit transaction and never tries to commit one.
    """

    def commit(self) -> None:
        return None

    def begin(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def in_transaction(self) -> bool:
        return False

    def get_transaction(self) -> Any:
        return None

    def close(self) -> None:
        return None


class ExecutionEngine(Protocol):

    def connect(self) -> EngineConnection:
        pass

    def setup(self, env: Environment, connection):
        pass

    def dispose(self, close: bool = True):
        pass


@runtime_checkable
class SupportsNativePersist(Protocol):
    """An engine that can perform some writes through its own API rather than
    by running the dialect's SQL.

    The seam is deliberately the *processed statement*, not a new plan type:
    ``ProcessedQueryPersist`` already carries the target, the persist mode, the
    partition columns and their types, so a plan object would only re-encode it
    — and would then be a second description of the write that could drift from
    the SQL. The dialect stays the single renderer; an engine that wants the
    rows staged somewhere of its own asks it for them
    (``BaseDialect.render_insert_into``).

    Returning ``None`` means "I do not handle this one", and the executor runs
    the SQL instead. Implementations must be conservative: recognize the exact
    shapes they optimize and decline everything else, because the fallback is
    always correct and a wrong native write is not.
    """

    def execute_persist(
        self, query: ProcessedQueryPersist, executor: Executor
    ) -> ResultProtocol | None: ...
