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
import threading
import weakref
from typing import Any

import duckdb
from duckdb_engine import ConnectionWrapper

_HOST = "trilogy_host"
_MAX_IDLE = 4

# Instance-level state an executor could leave behind. LOCAL settings belong
# to the connection and close with it.
_STATE_SQL = """
SELECT
    (SELECT hash(list((name, value) ORDER BY name))
     FROM duckdb_settings() WHERE scope = 'GLOBAL'),
    (SELECT hash(list(extension_name ORDER BY extension_name))
     FROM duckdb_extensions() WHERE loaded),
    (SELECT hash(list(name ORDER BY name)) FROM duckdb_secrets()),
    (SELECT hash(list((name, value::VARCHAR) ORDER BY name)) FROM duckdb_variables()),
    (SELECT hash(list(database_name ORDER BY database_name)) FROM duckdb_databases())
"""
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

_lock = threading.Lock()
_idle: list["_Instance"] = []
# Instances inherited across a fork are never touched again: their threads
# did not survive it, so neither using nor closing them is safe.
_abandoned: list["_Instance"] = []


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
        self.baseline = self._state()

    def _state(self) -> tuple:
        cursor = self.db.cursor()
        try:
            cursor.execute(f"USE {_HOST}")
            values = cursor.execute(_STATE_SQL).fetchone()
        finally:
            cursor.close()
        return (values, tuple(sorted(self.db.list_filesystems())))

    def open(self) -> duckdb.DuckDBPyConnection:
        cursor = self.db.cursor()
        try:
            cursor.execute(f"USE {_HOST}; ATTACH ':memory:' AS memory; USE memory")
        except Exception:
            cursor.close()
            raise
        return cursor

    def reset(self) -> bool:
        """Detach the executor's database; whether the instance is pristine."""
        cursor = self.db.cursor()
        try:
            cursor.execute(_RESET_SQL)
        except Exception:
            return False
        finally:
            cursor.close()
        return self._state() == self.baseline


class PooledConnection(ConnectionWrapper):
    """The wrapper duckdb_engine would make, returning its instance on close.

    Released on `close()` or, for an executor dropped without closing, when
    the wrapper is collected."""

    def __init__(self, instance: _Instance) -> None:
        self._tainted = [False]
        cursor = instance.open()
        super().__init__(cursor)
        self._release = weakref.finalize(
            self, _release, instance, cursor, self._tainted
        )
        self._release.atexit = False

    def __getattr__(self, name: str) -> Any:
        if name in _INSTANCE_APIS:
            self._tainted[0] = True
        return super().__getattr__(name)

    def close(self) -> None:
        super().close()
        self._release()


def _release(
    instance: _Instance, cursor: duckdb.DuckDBPyConnection, tainted: list[bool]
) -> None:
    try:
        cursor.close()
        keep = not tainted[0] and instance.reset()
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
