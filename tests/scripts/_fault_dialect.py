"""Fault-injectable BaseDialect subclass for tests.

Lets us exercise the catch-branches in code that calls
``executor.generator.list_tables`` / ``get_table_schema`` / ``execute_raw_sql``
without resorting to monkeypatching: instantiate with the call you want to
fail and the exception it should raise, then swap onto a real Executor.

    exec = Dialects.DUCK_DB.default_executor()
    exec.generator = FaultDialect(list_tables=RuntimeError("boom"))
    # later: exec.generator.list_tables(exec) raises RuntimeError
"""

from __future__ import annotations

from typing import Any

from trilogy.dialect.base import BaseDialect


class FaultDialect(BaseDialect):
    """Forwards to BaseDialect except where a fault is injected."""

    def __init__(
        self,
        *,
        list_tables: Exception | None = None,
        get_table_schema: Exception | None = None,
        execute_raw_sql: Exception | None = None,
    ) -> None:
        super().__init__()
        self._list_tables_error = list_tables
        self._get_table_schema_error = get_table_schema
        self._execute_raw_sql_error = execute_raw_sql

    def list_tables(
        self, executor: Any, schema: str | None = None
    ) -> list[tuple[str, str]]:
        if self._list_tables_error is not None:
            raise self._list_tables_error
        return super().list_tables(executor, schema)

    def get_table_schema(
        self, executor: Any, table_name: str, schema: str | None = None
    ) -> list[tuple]:
        if self._get_table_schema_error is not None:
            raise self._get_table_schema_error
        return super().get_table_schema(executor, table_name, schema)


class FailingExecutor:
    """Minimal Executor-shaped object whose ``execute_raw_sql`` always raises.

    Useful for exercising sniff-failure paths without needing a real warehouse
    that happens to fail; pair with a real ``BaseDialect`` as ``generator``.
    """

    def __init__(self, error: Exception, dialect: BaseDialect | None = None) -> None:
        self._error = error
        self.generator = dialect or BaseDialect()

        class _Conn:
            rolled_back = False

            def rollback(_self) -> None:
                _self.rolled_back = True

        self.connection = _Conn()

    def execute_raw_sql(self, sql: str, params: Any = None) -> Any:
        raise self._error
