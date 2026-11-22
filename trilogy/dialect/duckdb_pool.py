"""Recycled in-memory DuckDB instances.

Opening an in-memory DuckDB instance registers its whole built-in catalog
(~3k functions) and closing one frees it again: ~12ms per executor, which is
most of what a short-lived executor costs. Built-ins live in the instance's
system catalog, so an instance can serve many executors in turn if each gets
a database of its own.

Each executor gets an instance to itself, with a freshly attached `:memory:`
database named `memory`, so `memory.main.t` addresses resolve exactly as on a
fresh instance. On close the database is detached and the instance is kept
only if its global state (settings, loaded extensions, secrets, variables,
attached databases, filesystems) matches what it was opened with and no
instance-level Python API (UDFs, filesystems, extensions) was touched;
anything else retires it. An executor never shares an instance with another
live one.
"""

import os
import re
import threading
import weakref
from typing import Any

import duckdb
from duckdb_engine import ConnectionWrapper, CursorWrapper

_HOST = "trilogy_host"
# ~3.6MB each. Executors dropped without close() come back in batches when the
# cycle collector runs, and a batch larger than this is closed instead.
_MAX_IDLE = 8

# Instance-level state an executor could leave behind. LOCAL settings belong
# to the connection and close with it. Any statement can load an extension
# (a function or a path autoloads one), so those are read on every release.
_EXTENSIONS_SQL = """
SELECT hash(list(extension_name ORDER BY extension_name))
FROM duckdb_extensions() WHERE loaded
"""
# The rest is most of the check's cost and only moves when a statement moves
# it, so it is read only for a connection that ran one that could. A loaded
# extension can add settings too; the extension list above sees that.
_MOVED_SQL = """
SELECT
    (SELECT hash(list((name, value) ORDER BY name))
     FROM duckdb_settings() WHERE scope = 'GLOBAL'),
    (SELECT hash(list(name ORDER BY name)) FROM duckdb_secrets()),
    (SELECT hash(list((name, value::VARCHAR) ORDER BY name)) FROM duckdb_variables()),
    (SELECT hash(list(database_name ORDER BY database_name)) FROM duckdb_databases())
"""
_MOVES_STATE = re.compile(
    r"\b(set|reset|pragma|call|install|load|import|force|attach|detach|use|secret)\b",
    re.IGNORECASE,
)
# The words above inside a string literal (an error message) move nothing.
_LITERAL = re.compile(r"'(?:[^']|'')*'")
# `USE` sets the instance's default database, so the host is selected again
# before the executor's database can be detached. The file cache is flushed
# because a fresh instance would not have one.
_RESET_SQL = f"""
USE {_HOST};
DETACH memory;
SET enable_external_file_cache = false;
SET enable_external_file_cache = true;
"""

# Python APIs whose effect outlives the connection. A UDF is the sharp one:
# its callable dies with the connection while its catalog entry stays, and
# invoking it afterwards crashes the process.
_INSTANCE_APIS = frozenset(
    {
        "create_function",
        "remove_function",
        "register_filesystem",
        "unregister_filesystem",
        "install_extension",
        "load_extension",
    }
)

# Python APIs that run a statement, which is then read for what it could move.
_STATEMENT_APIS = frozenset({"execute", "executemany", "sql", "query"})
# Python APIs that run nothing. Any other is taken to have moved state.
_READ_APIS = frozenset(
    {
        "fetchone",
        "fetchall",
        "fetchmany",
        "fetchdf",
        "fetchnumpy",
        "fetch_arrow_table",
        "fetch_df",
        "df",
        "pl",
        "arrow",
        "description",
        "rowcount",
        "commit",
        "rollback",
        "begin",
        "register",
        "unregister",
        "interrupt",
    }
)

_lock = threading.Lock()
_idle: list["_Instance"] = []
# Instances inherited across a fork are never touched again: their threads
# did not survive it, so neither using nor closing them is safe.
_abandoned: list["_Instance"] = []
# What every fresh instance reports, read from the first one opened.
_baseline: tuple | None = None


def _user_agent() -> str:
    """The agent duckdb_engine would have set on a connection it opened."""
    from duckdb_engine import __version__
    from sqlalchemy import __version__ as sqlalchemy_version

    return f"duckdb_engine/{__version__}(sqlalchemy/{sqlalchemy_version})"


class _Instance:
    def __init__(self) -> None:
        self.db = duckdb.connect(
            ":memory:", config={"custom_user_agent": _user_agent()}
        )
        self.db.execute(f"ATTACH ':memory:' AS {_HOST}; USE {_HOST}; DETACH memory")
        global _baseline
        if _baseline is None:
            _baseline = self._state()

    def _state(self, moved: bool = True) -> tuple:
        cursor = self.db.cursor()
        try:
            cursor.execute(f"USE {_HOST}")
            loaded = cursor.execute(_EXTENSIONS_SQL).fetchone()
            rest = cursor.execute(_MOVED_SQL).fetchone() if moved else None
        finally:
            cursor.close()
        return (rest, loaded, tuple(sorted(self.db.list_filesystems())))

    def open(self) -> duckdb.DuckDBPyConnection:
        cursor = self.db.cursor()
        try:
            cursor.execute(f"USE {_HOST}; ATTACH ':memory:' AS memory; USE memory")
        except Exception:
            cursor.close()
            raise
        return cursor

    def reset(self, moved: bool = True) -> bool:
        """Detach the executor's database; whether the instance is pristine.
        `moved` is whether the connection ran anything that could have moved
        a setting, secret, variable or attached database."""
        cursor = self.db.cursor()
        try:
            cursor.execute(_RESET_SQL)
        except Exception:
            return False
        finally:
            cursor.close()
        assert _baseline is not None
        state = self._state(moved)
        return state[1:] == _baseline[1:] and (not moved or state == _baseline)


class _PooledCursor(CursorWrapper):
    """duckdb_engine's cursor, reporting what it runs to its connection."""

    def __init__(
        self, cursor: duckdb.DuckDBPyConnection, wrapper: "PooledConnection"
    ) -> None:
        super().__init__(cursor, wrapper)
        self._pooled = wrapper

    def execute(
        self, statement: str, parameters: Any = None, context: Any = None
    ) -> None:
        self._pooled.note(statement)
        super().execute(statement, parameters, context)

    def executemany(
        self, statement: str, parameters: Any = None, context: Any = None
    ) -> None:
        self._pooled.note(statement)
        super().executemany(statement, parameters, context)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pooled, name)


class PooledConnection(ConnectionWrapper):
    """The wrapper duckdb_engine would make, returning its instance on close.

    Released on `close()` or, for an executor dropped without closing, when
    the wrapper is collected."""

    def __init__(self, instance: _Instance) -> None:
        # [instance API touched, a statement could have moved instance state]
        self._tainted = [False, False]
        cursor = instance.open()
        self._cursor = cursor
        super().__init__(cursor)
        self._release = weakref.finalize(
            self, _release, instance, cursor, self._tainted
        )
        self._release.atexit = False

    def note(self, statement: object) -> None:
        if not isinstance(statement, str) or _MOVES_STATE.search(
            _LITERAL.sub("''", statement)
        ):
            self._tainted[1] = True

    def cursor(self) -> _PooledCursor:
        return _PooledCursor(self._cursor, self)

    def _noting(self, method: Any) -> Any:
        def run(statement: object, *args: Any, **kwargs: Any) -> Any:
            self.note(statement)
            return method(statement, *args, **kwargs)

        return run

    def __getattr__(self, name: str) -> Any:
        if name in _INSTANCE_APIS:
            self._tainted[0] = True
        found = super().__getattr__(name)
        if name in _STATEMENT_APIS:
            return self._noting(found)
        if name not in _READ_APIS:
            self._tainted[1] = True
        return found

    def close(self) -> None:
        super().close()
        self._release()


def _release(
    instance: _Instance, cursor: duckdb.DuckDBPyConnection, tainted: list[bool]
) -> None:
    try:
        cursor.close()
        # Only an instance with room to be kept is worth the state check.
        keep = (
            not tainted[0]
            and len(_idle) < _MAX_IDLE
            and instance.reset(moved=tainted[1])
        )
    except Exception:
        keep = False
    with _lock:
        if keep and len(_idle) < _MAX_IDLE:
            _idle.append(instance)
            return
    instance.db.close()


def connect() -> PooledConnection:
    with _lock:
        instance = _idle.pop() if _idle else None
    return PooledConnection(instance or _Instance())


def _abandon_idle() -> None:
    _abandoned.extend(_idle)
    _idle.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_abandon_idle)
