from typing import Any, Protocol

from sqlalchemy.engine import Connection, CursorResult, Engine

from trilogy.core.models.environment import Environment


class EngineResult(Protocol):
    pass

    def fetchall(self) -> list[tuple]:
        pass


class EngineConnection(Protocol):
    pass

    def execute(self, statement: str, parameters: Any | None = None) -> EngineResult:
        pass


class ExecutionEngine(Protocol):
    pass

    def connect(self) -> EngineConnection:
        pass

    def setup(self, env: Environment, connection):
        pass


### Begin default SQLAlchemy implementation
class SqlAlchemyResult(EngineResult):
    def __init__(self, result: CursorResult):
        self.result = result

    def fetchall(self):
        return self.result.fetchall()


class SqlAlchemyConnection(EngineConnection):
    def __init__(self, connection: Connection):
        self.connection = connection

    def execute(
        self, statement: str, parameters: Any | None = None
    ) -> SqlAlchemyResult:
        return SqlAlchemyResult(self.connection.execute(statement, parameters))


class SqlAlchemyEngine(ExecutionEngine):
    def __init__(self, engine: Engine):
        self.engine = engine

    def connect(self) -> SqlAlchemyConnection:
        return SqlAlchemyConnection(self.engine.connect())
