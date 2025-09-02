from typing import Any, Generator, List, Optional, Protocol

from sqlalchemy.engine import Connection, CursorResult, Engine

from trilogy.core.models.environment import Environment


class ResultProtocol(Protocol):

    def fetchall(self) -> List[Any]: ...

    def keys(self) -> List[str]: ...

    def fetchone(self) -> Optional[Any]: ...

    def fetchmany(self, size: int) -> List[Any]: ...

    def __iter__(self) -> Generator[Any, None, None]: ...


class EngineConnection(Protocol):
    pass

    def execute(self, statement: str, parameters: Any | None = None) -> ResultProtocol:
        pass

    def commit(self):
        raise NotImplementedError()

    def begin(self):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()


class ExecutionEngine(Protocol):
    pass

    def connect(self) -> EngineConnection:
        pass

    def setup(self, env: Environment, connection):
        pass


### Begin default SQLAlchemy implementation
class SqlAlchemyResult:
    def __init__(self, result: CursorResult):
        self.result = result

    def fetchall(self):
        return self.result.fetchall()

    def keys(self):
        return self.result.keys()

    def fetchone(self):
        return self.result.fetchone()

    def fetchmany(self, size: int):
        return self.result.fetchmany(size)

    def __iter__(self):
        return iter(self.result)


class SqlAlchemyConnection(EngineConnection):
    def __init__(self, connection: Connection):
        from sqlalchemy.future import Connection

        self.connection: Connection = connection

    def execute(
        self, statement: str, parameters: Any | None = None
    ) -> SqlAlchemyResult:
        return SqlAlchemyResult(self.connection.execute(statement, parameters))

    def commit(self):
        self.connection.commit()

    def begin(self):
        self.connection.begin()

    def rollback(self):
        self.connection.rollback()


class SqlAlchemyEngine(ExecutionEngine):
    def __init__(self, engine: Engine):
        self.engine = engine

    def connect(self) -> SqlAlchemyConnection:
        return SqlAlchemyConnection(self.engine.connect())
