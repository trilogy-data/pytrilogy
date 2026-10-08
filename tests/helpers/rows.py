from collections.abc import Iterable

from trilogy import Dialects
from trilogy.executor import Executor


def executor_for(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(model)
    return executor


def twins(derived: str, materialized: str) -> tuple[Executor, Executor]:
    return executor_for(derived), executor_for(materialized)


def customer_twins(extra: str = "") -> tuple[Executor, Executor]:
    """The customers materialization twin (`tests.helpers.models`) with
    `extra` on both sides. Fresh executors: a rowset statement redefines the
    environment it runs in, so share one only within a module."""
    from tests.helpers.models import CUSTOMERS_DERIVED, CUSTOMERS_MATERIALIZED

    return twins(CUSTOMERS_DERIVED + extra, CUSTOMERS_MATERIALIZED + extra)


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
