from typing import Any

from trilogy import Dialects
from trilogy.dialect.config import DuckDBConfig
from trilogy.executor import Executor


class DummyConnection:
    def __init__(self) -> None:
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1


class DummyEngine:
    def __init__(self) -> None:
        self.connection = DummyConnection()

    def connect(self) -> DummyConnection:
        return self.connection

    def setup(self, env: Any, connection: DummyConnection) -> None:
        return None

    def dispose(self, close: bool = True) -> None:
        return None


def test_duckdb_spatial_setup_enabled_executes_install_and_load(monkeypatch) -> None:
    executed_sql: list[str] = []

    def fake_execute_raw_sql(
        self: Executor,
        command: str,
        variables: dict[str, Any] | None = None,
        local_concepts: dict[str, Any] | None = None,
    ) -> None:
        executed_sql.append(command)
        return None

    monkeypatch.setattr(Executor, "execute_raw_sql", fake_execute_raw_sql)
    engine = DummyEngine()

    Executor(
        dialect=Dialects.DUCK_DB,
        engine=engine,
        config=DuckDBConfig(enable_spatial=True),
    )

    assert "INSTALL spatial;" in executed_sql
    assert "LOAD spatial;" in executed_sql
    assert executed_sql.index("INSTALL spatial;") < executed_sql.index("LOAD spatial;")
    assert engine.connection.commits == 2
