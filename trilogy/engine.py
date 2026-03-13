from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generator, List, Optional, Protocol

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import TextClause

from trilogy.core.models.environment import Environment


class ResultProtocol(Protocol):

    def fetchall(self) -> List[Any]: ...

    def keys(self) -> List[str]: ...

    def fetchone(self) -> Optional[Any]: ...

    def fetchmany(self, size: int) -> List[Any]: ...

    def __iter__(self) -> Generator[Any, None, None]: ...


class EngineConnection(Protocol):
    pass

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
    pass

    def connect(self) -> EngineConnection:
        pass

    def setup(self, env: Environment, connection):
        pass

    def dispose(self, close: bool = True):
        pass
