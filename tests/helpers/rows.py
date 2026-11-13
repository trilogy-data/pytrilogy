import re
from collections.abc import Iterable

from trilogy import Dialects
from trilogy.executor import Executor


def executor_for(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(model)
    return executor


class ExecutorCache:
    """`functools.cache` for an executor factory, with a way to let go.

    A cached Executor holds a live DuckDB database (and its worker threads)
    until the cache dies, and a module-level `functools.cache` dies with the
    process -- so every module that memoized its twins kept them for the rest
    of the suite. Call `close()` from a module-scoped fixture instead."""

    def __init__(self, factory):
        self._factory = factory
        self._made: dict[tuple, Executor | tuple[Executor, ...]] = {}

    def __call__(self, *args):
        if args not in self._made:
            self._made[args] = self._factory(*args)
        return self._made[args]

    def close(self) -> None:
        for made in self._made.values():
            for executor in made if isinstance(made, tuple) else (made,):
                executor.close()
        self._made.clear()


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


CTE_NAME = re.compile(r"^(\w+) as (?:MATERIALIZED )?\(", re.MULTILINE)


def cte_rows(executor: Executor, sql: str, name: str) -> int:
    head = sql[: sql.rindex("\nSELECT")]
    return executor.execute_raw_sql(f"{head}\nSELECT count(*) FROM {name}").fetchone()[
        0
    ]


def widest_cte(executor: Executor, sql: str) -> tuple[int, str]:
    """The row count and name of the plan's widest CTE: a fan-out bound."""
    return max(
        ((cte_rows(executor, sql, name), name) for name in CTE_NAME.findall(sql)),
        default=(0, ""),
    )


def as_float(v):
    return float(v) if v is not None and not isinstance(v, (str, bool)) else v


def normalize_numbers(rows: Iterable[tuple]) -> list[tuple]:
    """Rows sorted with numbers on one spelling (`2.0` is `2`), for an oracle."""

    def fix(v):
        if v is None or isinstance(v, (str, bool)):
            return v
        number = float(v)
        return int(number) if number.is_integer() else round(number, 6)

    return sorted((tuple(fix(v) for v in r) for r in rows), key=repr)
