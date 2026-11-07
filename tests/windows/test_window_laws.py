"""`formal/Trilogy/Windows.lean`: a window is evaluated over the rows of its
own grain. A numbering window ranks the distinct tuples of its arguments,
partition and order columns (`Windows.rank_reads_values`), and its value is
read back onto each row holding the tuple once (`Windows.lookup_keeps_rows`).
Ranking the rows of a finer key the select also asks for counts each repeat
and fans the read-back out (`Windows.row_stream_rank_counts_repeats`,
`Windows.stream_join_fans_out`). An aggregate window with no order is the
aggregate `by` its partition, and an empty `over ()` is the total
(`Windows.empty_over_is_total`).

A plain ORDER BY direction sorts NULLs last on every engine, `nulls first`
first, and `nulls auto` as the engine does (`Windows.render_places`). SQLite
sorts an unplaced NULL first (`Windows.bare_direction_misplaces`), so every
case runs on DuckDB, SQLite and ClickHouse (chdb, where it installs) and must
agree with the reference. ClickHouse also pads `lag`/`lead` with NULL past a
partition's edge (`Padding.lean`)."""

import importlib.util
import random
from typing import Any

import pytest

from trilogy import Dialects, Environment, parse
from trilogy.dialect.config import ClickhouseConfig
from trilogy.executor import Executor

SEEDS = range(4)
COLS = ["id", "g", "x"]
# chdb has no Windows wheel
CHDB = [Dialects.CLICKHOUSE] if importlib.util.find_spec("chdb") else []
DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
# (column, descending, nulls first)
Order = tuple[str, bool, bool]
SPELL = {
    (False, False): "asc",
    (True, False): "desc",
    (False, True): "asc nulls first",
    (True, True): "desc nulls first",
}
_EXECUTORS: dict[tuple[Dialects, int], Executor] = {}


def make_rows(seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    return [
        dict(
            zip(
                COLS,
                (i, rng.choice(["a", "b", "c", None]), rng.choice([1, 2, 2, 3, None])),
            )
        )
        for i in range(1, rng.randint(4, 12))
    ]


def literal(value: Any, cast: str) -> str:
    """A typed NULL every engine reads, ClickHouse included: its `cast(null as
    T)` raises, as T is NOT NULL there."""
    if value is None:
        zero = "''" if cast == "varchar" else "0"
        return f"nullif({zero}, {zero})"
    return repr(value)


def fixture(rows: list[dict[str, Any]]) -> str:
    body = " union all\n".join(
        f"select {r['id']} as id, {literal(r['g'], 'varchar')} as g, "
        f"{literal(r['x'], 'int')} as x"
        for r in rows
    )
    return f"""
key id int;
property id.g string?;
property id.x int?;
datasource t (id: id, g: g, x: x) grain (id) query '''
{body}
''';
"""


def seeds_for(dialect: Dialects, seeds: range) -> range:
    """ClickHouse holds the same laws on fewer seeds, to keep CI time down."""
    return seeds[:1] if dialect == Dialects.CLICKHOUSE else seeds


def new_executor(dialect: Dialects) -> Executor:
    if dialect == Dialects.CLICKHOUSE:
        return dialect.default_executor(
            environment=Environment(), conf=ClickhouseConfig(mode="chdb")
        )
    return dialect.default_executor(environment=Environment())


def executor(dialect: Dialects, seed: int) -> Executor:
    if (dialect, seed) not in _EXECUTORS:
        ex = new_executor(dialect)
        ex.execute_text(fixture(make_rows(seed)))
        _EXECUTORS[(dialect, seed)] = ex
    return _EXECUTORS[(dialect, seed)]


def sort_key(row: dict[str, Any], order: list[Order]) -> tuple:
    out = []
    for column, descending, nulls_first in order:
        value = row[column]
        if value is None:
            out.append((0 if nulls_first else 2, ()))
            continue
        parts = tuple(ord(c) for c in value) if isinstance(value, str) else (value,)
        out.append((1, tuple(-p for p in parts) if descending else parts))
    return tuple(out)


def partitions(
    rows: list[dict[str, Any]], partition: tuple[str, ...]
) -> list[list[dict[str, Any]]]:
    groups: dict[tuple, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row[c] for c in partition), []).append(row)
    return list(groups.values())


def rank_tuples(
    rows: list[dict[str, Any]],
    grain: list[str],
    partition: tuple[str, ...],
    order: list[Order],
    dense: bool,
) -> dict[tuple, int]:
    """`Windows.rankD`: rank the distinct grain tuples, not the rows."""
    distinct = list({tuple(r[c] for c in grain): r for r in rows}.values())
    ranks: dict[tuple, int] = {}
    for members in partitions(distinct, partition):
        members.sort(key=lambda r: sort_key(r, order))
        keys = [sort_key(r, order) for r in members]
        rank = dense_rank = 0
        for i, row in enumerate(members):
            if i == 0 or keys[i] != keys[i - 1]:
                rank, dense_rank = i + 1, dense_rank + 1
            ranks[tuple(row[c] for c in grain)] = dense_rank if dense else rank
    return ranks


def frame_value(
    fn: str, members: list[dict[str, Any]], i: int, keys: list | None, operand: str
) -> Any:
    if fn in ("lag", "lead"):
        j = i - 1 if fn == "lag" else i + 1
        return members[j][operand] if 0 <= j < len(members) else None
    # with an order, the frame runs to the current row's last peer
    frame = (
        members if keys is None else [m for m, k in zip(members, keys) if k <= keys[i]]
    )
    values = [m[operand] for m in frame if m[operand] is not None]
    if fn == "count":
        return len(values)
    if not values:
        return None
    if fn == "sum":
        return sum(values)
    if fn == "max":
        return max(values)
    return round(sum(values) / len(values), 6)


def projected(
    rows: list[dict[str, Any]], query: str, selected: tuple[str, ...]
) -> list:
    """The selected tuples, one per distinct tuple of the statement's grain."""
    _, statements = parse(fixture(rows) + query, environment=Environment())
    grain = {a.split(".")[-1] for a in statements[-1].grain.components}
    over = sorted(grain | set(selected))
    distinct = {tuple(r[c] for c in over): r for r in rows}.values()
    return sorted((tuple(r[c] for c in selected) for r in distinct), key=repr)


def order_clause(order: list[Order]) -> str:
    return ", ".join(f"{c} {SPELL[(d, nf)]}" for c, d, nf in order)


def over_clause(partition: tuple[str, ...], order: list[Order]) -> str:
    parts = [f"partition by {', '.join(partition)}"] if partition else []
    if order:
        parts.append(f"order by {order_clause(order)}")
    return f"over ({' '.join(parts)})"


def fetch(dialect: Dialects, seed: int, query: str) -> list:
    rows = executor(dialect, seed).execute_text(query)[-1].fetchall()
    return [
        tuple(round(v, 6) if isinstance(v, float) else v for v in row) for row in rows
    ]


NUMBERING = [
    (fn, args, partition, order, selected)
    for fn in ["rank", "dense_rank"]
    for args in [("g",), ("x",), ("g", "x"), ("id",)]
    for partition in [(), ("g",)]
    for order in [
        ("x", False, False),
        ("x", True, False),
        ("x", False, True),
        ("g", False, False),
    ]
    for selected in [("id",), ("g",), ("id", "g")]
]


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fn, args, partition, order, selected", NUMBERING, ids=repr)
def test_numbering_window_ranks_its_grain(
    dialect: Dialects,
    fn: str,
    args: tuple[str, ...],
    partition: tuple[str, ...],
    order: Order,
    selected: tuple[str, ...],
) -> None:
    window = f"{fn}({', '.join(args)}) {over_clause(partition, [order])}"
    query = f"select {', '.join(selected)}, {window} as r;"
    grain = sorted(set(args) | set(partition) | {order[0]}, key=COLS.index)
    for seed in seeds_for(dialect, SEEDS):
        rows = make_rows(seed)
        ranks = rank_tuples(rows, grain, partition, [order], fn == "dense_rank")
        for row in rows:
            row["r"] = ranks[tuple(row[c] for c in grain)]
        expected = projected(rows, query, (*selected, "r"))
        assert sorted(fetch(dialect, seed, query), key=repr) == expected, (seed, query)


ORDERED = [
    [("id", False, False)],
    [("x", False, False), ("id", True, False)],
    [("x", True, True), ("id", False, False)],
]
UNORDERED_OR_PEERS = [
    [],
    [("x", False, False)],
    [("x", True, False)],
    [("x", False, True)],
    [("g", False, False)],
]
VALUE_WINDOWS = [
    (fn, operand, partition, order, selected)
    for fn in ["lag", "lead", "sum", "count", "avg", "max"]
    for operand in ["x", "id"]
    for partition in [(), ("g",)]
    for order in (
        ORDERED if fn in ("lag", "lead") else ORDERED[:1] + UNORDERED_OR_PEERS
    )
    for selected in [("id",), ("g",), ()]
]


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize(
    "fn, operand, partition, order, selected", VALUE_WINDOWS, ids=repr
)
def test_value_window_reads_each_row(
    dialect: Dialects,
    fn: str,
    operand: str,
    partition: tuple[str, ...],
    order: list[Order],
    selected: tuple[str, ...],
) -> None:
    window = f"{fn}({operand}) {over_clause(partition, order)}"
    query = f"select {', '.join([*selected, f'{window} as r'])};"
    for seed in seeds_for(dialect, SEEDS):
        rows = make_rows(seed)
        for members in partitions(rows, partition):
            members.sort(key=lambda r: sort_key(r, order))
            keys = [sort_key(r, order) for r in members] if order else None
            for i, row in enumerate(members):
                row["r"] = frame_value(fn, members, i, keys, operand)
        # a value window's keys are its operand, not the rows it reads, so
        # the tuples are compared as a set
        expected = sorted(set(projected(rows, query, (*selected, "r"))), key=repr)
        got = sorted(set(fetch(dialect, seed, query)), key=repr)
        assert got == expected, (seed, query)


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize(
    "order",
    [[(c, d, nf)] for c in ("x", "g") for d in (False, True) for nf in (False, True)],
    ids=repr,
)
def test_order_by_places_nulls_as_asked(dialect: Dialects, order: list[Order]) -> None:
    query = f"select id, {order[0][0]} order by {order_clause(order)}, id asc;"
    for seed in seeds_for(dialect, SEEDS):
        rows = sorted(make_rows(seed), key=lambda r: (sort_key(r, order), r["id"]))
        expected = [(r["id"], r[order[0][0]]) for r in rows]
        assert fetch(dialect, seed, query) == expected, (seed, query)
