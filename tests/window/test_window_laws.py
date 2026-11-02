"""`formal/Trilogy/Window.lean`: Trilogy sorts NULLs last in both directions
unless an ordering says `nulls first`, and a bare `asc` / `desc` or `nulls auto`
means the same on every dialect. Each dialect renders a bare direction only
where its engine's default agrees, and an explicit or emulated placement
otherwise (`Window.render_exact`; `Window.bare_asc_differs` was SQLite,
`Window.bare_desc_differs` Postgres and Snowflake).

A numbering window ranks the distinct values of its argument, partition and
order fields, so the rank is a function of those values whatever else a select
projects (`Window.rank_dup_invariant`). Ranking the row stream a property lives
on gave a different rank (`Window.row_stream_rank_differs`), and joining that
row-level output back on the value fanned out (`Window.row_output_join_fans_out`).
Navigation windows walk their operand's rows (`docs/modeling_concepts.md`).

`formal/Trilogy/SetOps.lean`: `except` / `intersect` are set operators on
NULL-safe row equality, unlike `not in` (`SetOps.except_keeps_no_match`,
`SetOps.not_in_drops_null`).

Each case builds random rows with NULLs and ties and holds DuckDB and SQLite
to the reference computed here."""

import itertools
import random
from collections import Counter
from collections.abc import Callable
from functools import cache
from typing import Any

import pytest

from trilogy import Dialects, Environment, Executor
from trilogy.core.enums import Ordering
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.mysql import MySQLDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect, TrinoDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.dialect.sqlite import SQLiteDialect

ENGINES = [Dialects.DUCK_DB, Dialects.SQLITE]
SEEDS = range(3)
ORDERS = ["asc", "desc", "asc nulls first", "desc nulls first", "desc nulls last"]

Row = tuple[Any, ...]


def _lit(x: int | None) -> str:
    return "cast(null as int)" if x is None else str(x)


def _sort_rows(rows: list[Row]) -> list[Row]:
    return sorted(rows, key=lambda t: [(y is None, 0 if y is None else y) for y in t])


def _placement(order: str) -> tuple[bool, bool]:
    return order.startswith("desc"), "nulls first" in order


def _key(x: int | None, order: str) -> tuple[int, int]:
    desc, nulls_first = _placement(order)
    if x is None:
        return (0 if nulls_first else 2, 0)
    return (1, -x if desc else x)


def _ranks(values: list[int | None], order: str) -> dict:
    """`Window.rank` over the distinct values."""
    return {
        v: 1 + sum(1 for w in set(values) if _key(w, order) < _key(v, order))
        for v in values
    }


# single table: a key with two nullable, tied properties


def _table(seed: int) -> list[tuple[int, int | None, int | None]]:
    rnd = random.Random(seed)
    return [
        (i, rnd.choice([None, 1, 2]), rnd.choice([None, 1, 2, 3]))
        for i in range(1, rnd.randint(4, 10))
    ]


def _table_model(seed: int) -> str:
    body = " union all\n".join(
        f"select {i} as id, {_lit(g)} as g, {_lit(v)} as v" for i, g, v in _table(seed)
    )
    return f"""
key id int;
property id.g int?;
property id.v int?;
datasource t (id: id, g: g, v: v) grain (id) query '''
{body}
''';
auto rv <- rank v order by v asc;
auto rg <- rank g order by g desc;
auto cnt_v <- count(id) by v;
auto rc <- rank v order by cnt_v desc;
"""


# a fact and a dimension it references partially (`~sid`)


def _star(seed: int) -> tuple[list, list]:
    rnd = random.Random(seed)
    dims = [
        (s, rnd.choice([None, 1, 2, 3]), rnd.choice([None, 1, 2]))
        for s in range(1, rnd.randint(3, 6))
    ]
    facts = [(i, rnd.choice([d[0] for d in dims])) for i in range(1, rnd.randint(2, 9))]
    return dims, facts


def _star_model(seed: int) -> str:
    dims, facts = _star(seed)
    d = " union all\n".join(
        f"select {s} as sid, {_lit(n)} as nm, {_lit(g)} as grp" for s, n, g in dims
    )
    f = " union all\n".join(f"select {i} as id, {s} as sid" for i, s in facts)
    return f"""
key sid int;
property sid.nm int?;
property sid.grp int?;
key id int;
datasource dims (sid: sid, nm: nm, grp: grp) grain (sid) query '''{d}''';
datasource facts (id: id, sid: ~sid) grain (id) query '''{f}''';
"""


@cache
def _executor(engine: Dialects, model: str) -> Executor:
    executor = engine.default_executor(environment=Environment())
    executor.execute_text(model)
    return executor


def _run(engine: Dialects, model: str, query: str) -> list[Row]:
    rows = _executor(engine, model).execute_text(query)[-1].fetchall()
    return _sort_rows([tuple(r) for r in rows])


# rendering, against the engines' own defaults

# Where each engine sorts NULLs under a bare ASC / DESC, and whether it has
# NULLS FIRST / LAST syntax.
ENGINE_DEFAULTS: dict[type[BaseDialect], tuple[bool, bool, bool]] = {
    DuckDBDialect: (False, False, True),
    PrestoDialect: (False, False, True),
    TrinoDialect: (False, False, True),
    ClickhouseDialect: (False, False, True),
    SQLiteDialect: (True, False, True),
    BigqueryDialect: (True, False, True),
    PostgresDialect: (False, True, True),
    SnowflakeDialect: (False, True, True),
    MySQLDialect: (True, False, False),
    SqlServerDialect: (True, False, False),
}


def _term_key(term: str, x: int | None, defaults: tuple[bool, bool, bool]) -> tuple:
    """`Window.Term.before`: one rendered ORDER BY term over a column `x`, as an
    engine with these defaults sorts it."""
    expr, _, rest = term.partition(" ")
    desc = rest.startswith("desc")
    if expr != "x":
        flag = 1 if x is None else 0
        return (-flag if desc else flag,)
    if "nulls" in rest:
        assert defaults[2], f"{term!r}: no NULLS FIRST/LAST syntax"
        nulls_first = rest.endswith("first")
    else:
        nulls_first = defaults[1] if desc else defaults[0]
    if x is None:
        return (0 if nulls_first else 2, 0)
    return (1, -x if desc else x)


def _engine_key(
    rendered: str, x: int | None, defaults: tuple[bool, bool, bool]
) -> list[tuple]:
    """`Window.before` is the lexicographic order of these keys."""
    return [_term_key(t.strip(), x, defaults) for t in rendered.split(",")]


def _flag_spelling(rendered: str) -> str:
    return (
        rendered.replace("CASE WHEN x IS NULL THEN 1 ELSE 0 END", "flag")
        .replace("(x) IS NULL", "flag")
        .strip()
    )


@pytest.mark.parametrize("order", list(Ordering), ids=lambda o: o.name)
@pytest.mark.parametrize(
    "dialect", list(ENGINE_DEFAULTS), ids=lambda d: d.__name__.replace("Dialect", "")
)
def test_rendered_ordering_places_nulls_on_every_engine(
    dialect: type[BaseDialect], order: Ordering
):
    defaults = ENGINE_DEFAULTS[dialect]
    assert dialect.NULLS_FIRST_BY_DEFAULT == defaults[:2]
    assert dialect.NULLS_PLACEMENT_SYNTAX == defaults[2]
    rendered = _flag_spelling(dialect().render_ordering("x", order))
    assert "auto" not in rendered
    values: list[int | None] = [None, 0, 1]
    for a, b in itertools.product(values, values):
        before = _engine_key(rendered, a, defaults) < _engine_key(rendered, b, defaults)
        assert before == (_key(a, order.value) < _key(b, order.value)), (
            rendered,
            a,
            b,
        )


@pytest.mark.parametrize("order", [o.value for o in Ordering])
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.value)
def test_select_order_places_nulls(engine: Dialects, order: str):
    model = _table_model(0)
    rows = _executor(engine, model).execute_text(f"select v order by v {order};")
    got = [r[0] for r in rows[-1].fetchall()]
    expected = sorted({v for _, _, v in _table(0)}, key=lambda v: _key(v, order))
    assert got == expected


# numbering windows


NUMBERING = [
    (kind, arg, part, order_by, order)
    for kind, arg, part, order_by, order in itertools.product(
        ["rank", "dense_rank"], ["id", "v", "g"], [None, "g"], ["v", "g"], ORDERS
    )
    if part != order_by
]


def _numbering_reference(seed: int, kind, arg, part, order_by, order) -> list[Row]:
    cols = {"id": 0, "g": 1, "v": 2}
    names = [c for c in (arg, part, order_by) if c]
    rows = _table(seed)
    stream = {tuple(r[cols[c]] for c in names) for r in rows}
    ranked = {}
    for s in stream:
        own = _key(s[-1], order)
        ahead = [
            _key(p[-1], order)
            for p in stream
            if (not part or p[1] == s[1]) and _key(p[-1], order) < own
        ]
        ranked[s] = 1 + (len(set(ahead)) if kind == "dense_rank" else len(ahead))
    return _sort_rows([(r[0], ranked[tuple(r[cols[c]] for c in names)]) for r in rows])


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.value)
def test_numbering_window_ranks_argument_values(engine: Dialects, seed: int):
    model = _table_model(seed)
    for kind, arg, part, order_by, order in NUMBERING:
        over = f" over {part}" if part else ""
        query = f"select id, {kind} {arg}{over} order by {order_by} {order} -> r;"
        expected = _numbering_reference(seed, kind, arg, part, order_by, order)
        assert _run(engine, model, query) == expected, query


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.value)
def test_rank_is_projection_invariant(engine: Dialects, seed: int):
    model = _table_model(seed)
    rows = _table(seed)
    rv = _ranks([v for _, _, v in rows], "asc")
    rg = _ranks([g for _, g, _ in rows], "desc")
    cnt = Counter(v for _, _, v in rows)
    rc = {v: 1 + sum(1 for w in cnt if cnt[w] > cnt[v]) for v in cnt}
    cases = {
        "select id, v, rv;": [(i, v, rv[v]) for i, _, v in rows],
        "select v, rv;": [(v, rv[v]) for v in rv],
        "select id, rv, rg;": [(i, rv[v], rg[g]) for i, g, v in rows],
        "select id, rv where rv = 1;": [(i, 1) for i, _, v in rows if rv[v] == 1],
        "select id, rv having rv = 1;": [(i, 1) for i, _, v in rows if rv[v] == 1],
        "select rv, count(id) as n;": list(Counter(rv[v] for *_, v in rows).items()),
        "select count(id) as n where rv = 1;": [
            (sum(1 for *_, v in rows if rv[v] == 1),)
        ],
        "select g, max(rv) as m;": [
            (g, max(rv[v] for _, h, v in rows if h == g)) for g in {r[1] for r in rows}
        ],
        "select id, rc;": [(i, rc[v]) for i, _, v in rows],
        "select v, rc;": [(v, rc[v]) for v in cnt],
    }
    for query, expected in cases.items():
        assert _run(engine, model, query) == _sort_rows(expected), query


def test_rank_beside_its_row_key_does_not_fan_out():
    model = _table_model(0)
    query = "select id, v, rank v order by v asc -> r;"
    rows = _run(Dialects.DUCK_DB, model, query)
    assert len(rows) == len(_table(0))
    assert len({r[0] for r in rows}) == len(rows)


def _star_cases(seed: int, order: str) -> dict[str, list[Row]]:
    dims, facts = _star(seed)
    name = {s: n for s, n, _ in dims}
    group = {s: g for s, _, g in dims}
    unused = [s for s in name if s not in {s for _, s in facts}]
    ranked = _ranks(list(name.values()), order)
    in_group = {
        s: _ranks([name[t] for t in name if group[t] == group[s]], order)[name[s]]
        for s in name
    }
    return {
        f"select id, rank nm order by nm {order} -> r;": [
            (i, ranked[name[s]]) for i, s in facts
        ]
        + list({(None, ranked[name[s]]) for s in unused}),
        f"select sid, rank nm order by nm {order} -> r;": [
            (s, ranked[n]) for s, n in name.items()
        ],
        f"select id, rank nm over grp order by nm {order} -> r;": [
            (i, in_group[s]) for i, s in facts
        ]
        + list({(None, in_group[s]) for s in unused}),
        f"select nm, count(id) as n, rank nm order by nm {order} -> r;": [
            (n, sum(1 for _, s in facts if name[s] == n), ranked[n])
            for n in set(name.values())
        ],
    }


@pytest.mark.parametrize("seed", range(4))
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.value)
def test_window_over_a_joined_dimension(engine: Dialects, seed: int):
    """A rank of `nm` ranks every dimension row's name; a select of the fact
    key pads the dimension rows no fact references (`~sid`)."""
    model = _star_model(seed)
    for order in ORDERS:
        for query, expected in _star_cases(seed, order).items():
            assert _run(engine, model, query) == _sort_rows(expected), query


# navigation windows


def _navigation_reference(seed, kind, offset, col, part, order) -> list[Row]:
    cols = {"id": 0, "g": 1, "v": 2}
    groups: dict = {}
    for r in _table(seed):
        groups.setdefault(r[1] if part else 0, []).append(r)
    out = []
    for rows in groups.values():
        rows.sort(key=lambda r: (_key(r[2], order), r[0]))
        for i, r in enumerate(rows):
            j = i - offset if kind == "lag" else i + offset
            out.append((r[0], rows[j][cols[col]] if 0 <= j < len(rows) else None))
    return _sort_rows(out)


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.value)
def test_navigation_window_walks_operand_rows(engine: Dialects, seed: int):
    model = _table_model(seed)
    for kind, offset, col, part, order in itertools.product(
        ["lag", "lead"], [1, 2], ["v", "g"], [None, "g"], ORDERS[:4]
    ):
        over = f" over {part}" if part else ""
        query = (
            f"select id, {kind} {offset} {col}{over} "
            f"order by v {order}, id asc -> r;"
        )
        expected = _navigation_reference(seed, kind, offset, col, part, order)
        assert _run(engine, model, query) == expected, query


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.value)
def test_navigation_over_distinct_values(engine: Dialects, seed: int):
    model = _table_model(seed)
    rows = _table(seed)
    for kind, order in itertools.product(["lag", "lead"], ORDERS[:4]):
        values = sorted({v for *_, v in rows}, key=lambda v: _key(v, order))
        step = -1 if kind == "lag" else 1
        moved = {
            v: values[i + step] if 0 <= i + step < len(values) else None
            for i, v in enumerate(values)
        }
        query = (
            f"select id, {kind}(distinct(v), 1) over "
            f"(order by distinct(v) {order}) -> r;"
        )
        expected = _sort_rows([(i, moved[v]) for i, _, v in rows])
        assert _run(engine, model, query) == expected, query


# set operators

CONDITIONS: dict[str, Callable[[tuple], bool]] = {
    "g = 1": lambda r: r[1] == 1,
    "g = 2": lambda r: r[1] == 2,
    "g is null": lambda r: r[1] is None,
    "v > 1": lambda r: r[2] is not None and r[2] > 1,
    "v is null": lambda r: r[2] is None,
    "id > 0": lambda r: True,
}


def _set_op(op: str, arms: list[set]) -> set:
    out = arms[0]
    for arm in arms[1:]:
        if op == "except":
            out = out - arm
        elif op == "intersect":
            out = out & arm
        else:
            out = out | arm
    return out


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.value)
def test_set_operators_are_null_safe_sets(engine: Dialects, seed: int):
    model = _table_model(seed)
    rows = _table(seed)
    rnd = random.Random(seed)
    for op, arity in itertools.product(["except", "intersect", "union"], [2, 3]):
        for _ in range(3):
            conditions = [rnd.choice(list(CONDITIONS)) for _ in range(arity)]
            col = rnd.choice(["g", "v"])
            index = 1 if col == "g" else 2
            arms = ", ".join(f"(where {c} select {col} -> k)" for c in conditions)
            values = [{r[index] for r in rows if CONDITIONS[c](r)} for c in conditions]
            expected = _set_op(op, values)
            head = f"with c as {op}({arms}) -> (k); "
            assert _run(engine, model, head + "select c.k;") == _sort_rows(
                [(x,) for x in expected]
            ), (head, values)
            counted = [(len(expected - {None}),)] if expected else []
            assert _run(engine, model, head + "select count(c.k) as n;") == counted, (
                head,
                values,
            )
