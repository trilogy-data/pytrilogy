from collections.abc import Iterable

from trilogy import Dialects
from trilogy.executor import Executor


def executor_for(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(model)
    return executor


def row_key(row: tuple) -> tuple:
    return tuple((v is None, str(v)) for v in row)


def sort_rows(rows: Iterable[tuple]) -> list[tuple]:
    return sorted(rows, key=row_key)


def fetch_rows(executor: Executor, query: str) -> list[tuple]:
    statement = query if query.rstrip().endswith(";") else query + ";"
    return [tuple(r) for r in executor.execute_text(statement)[-1].fetchall()]


def sorted_rows(executor: Executor, query: str) -> list[tuple]:
    """The query's rows in a None-safe order."""
    return sort_rows(fetch_rows(executor, query))


def twin_rows(derived: Executor, materialized: Executor, query: str) -> list[tuple]:
    """The query's rows, asserted identical on both twins of a model."""
    rows = sorted_rows(derived, query)
    assert rows == sorted_rows(materialized, query)
    return rows
