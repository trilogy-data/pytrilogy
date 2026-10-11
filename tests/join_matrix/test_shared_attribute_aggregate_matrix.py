"""Two facts aggregated to a shared dimension ATTRIBUTE (issue #715).

The reference shape: two unrelated facts, one date dimension, `select year,
count(store), count(web)`. Each aggregate reads its own fact through the
dimension, and the per-year aggregates FULL JOIN on year.

Every other form reaches `year` through a SEPARATE date dimension per fact and
relates the two years (`union join`, `subset join`, a global `merge`). The
relation only makes the two years one mergeable value, so each form must plan
like the reference: never join one fact's rows to the other fact's dimension
on year (an m:n join, one row per date pair).

Contract per cell: the oracle rows, and a peak DuckDB operator cardinality the
reference plan stays under and a cross-dimension year join cannot.
"""

import json
from pathlib import Path

import pytest

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Environment

# (date_id, year): ten dates a year; 2022 has no sales (dimension padding)
DATES = [(i, 2020 + (i - 1) // 10) for i in range(1, 31)]
# (ticket, item, date_id, paid): sales land on four dates, two a year, so a
# per-date fact x fact join shows; ticket t spans two items; undated rows
STORE = [(i // 2, i % 2, None if i % 9 == 0 else i % 4 * 5 + 1, i) for i in range(40)]
# (order, item, date_id, paid)
WEB = [
    (i // 3, i % 3, None if i == 7 else i * 7 % 4 * 5 + 1, 100 + i) for i in range(30)
]
# returns cover part of each fact's lines: (ticket/order, item, amount)
STORE_RETURNS = [(t, i, p) for t, i, _, p in STORE[::3]]
WEB_RETURNS = [(o, i, p) for o, i, _, p in WEB[::4]]

YEARS = dict(DATES)
PEAK_BOUND = 2 * (len(DATES) + len(STORE) + len(WEB))


def _values(rows: list[tuple], names: tuple[str, ...]) -> str:
    def lit(v):
        return "cast(null as int)" if v is None else str(v)

    return " union all ".join(
        "select " + ", ".join(f"{lit(v)} as {n}" for v, n in zip(row, names))
        for row in rows
    )


def _dim(prefix: str) -> str:
    return f"""
key {prefix}date_id int;
property {prefix}date_id.{prefix}year int;
datasource {prefix}dates (d: {prefix}date_id, y: {prefix}year) grain ({prefix}date_id)
query '''{_values(DATES, ("d", "y"))}''';
"""


def _facts(store_date: str, web_date: str, returns: bool) -> str:
    model = f"""
key ticket int;
key s_item int;
property <ticket, s_item>.s_paid int;
datasource store_sales (t: ticket, i: s_item, d: ~?{store_date}, p: s_paid)
grain (ticket, s_item)
query '''{_values(STORE, ("t", "i", "d", "p"))}''';

key order_number int;
key w_item int;
property <order_number, w_item>.w_paid int;
datasource web_sales (o: order_number, i: w_item, d: ~?{web_date}, p: w_paid)
grain (order_number, w_item)
query '''{_values(WEB, ("o", "i", "d", "p"))}''';
"""
    if returns:
        model += f"""
property <ticket, s_item>.s_returned int?;
datasource store_returns (t: ~ticket, i: ~s_item, a: s_returned)
grain (ticket, s_item)
query '''{_values(STORE_RETURNS, ("t", "i", "a"))}''';

property <order_number, w_item>.w_returned int?;
datasource web_returns (o: ~order_number, i: ~w_item, a: w_returned)
grain (order_number, w_item)
query '''{_values(WEB_RETURNS, ("o", "i", "a"))}''';
"""
    return model


# form -> (model, relation clause, store year, web year)
FORMS = {
    "shared_dimension": (_dim("") + "{facts}", "", "year", "year"),
    "union_join": (
        _dim("s_") + _dim("w_") + "{facts}",
        "union join w_year = s_year\n",
        "s_year",
        "w_year",
    ),
    "subset_join": (
        _dim("s_") + _dim("w_") + "{facts}",
        "subset join w_year = s_year\n",
        "s_year",
        "w_year",
    ),
    "merge": (
        _dim("s_") + _dim("w_") + "{facts}\nmerge w_year into s_year;\n",
        "",
        "s_year",
        "w_year",
    ),
}
DATE_KEYS = {
    "shared_dimension": ("date_id", "date_id"),
    "union_join": ("s_date_id", "w_date_id"),
    "subset_join": ("s_date_id", "w_date_id"),
    "merge": ("s_date_id", "w_date_id"),
}


def _by_year(rows: list[tuple], measure) -> dict:
    """Per year (every dimension year, plus NULL for undated rows) the
    measure over the rows dated in it, None where none are."""
    groups: dict = {year: [] for year in YEARS.values()}
    for row in rows:
        groups.setdefault(YEARS.get(row[2]), []).append(row)
    return {year: measure(members) for year, members in groups.items()}


def _count(members: list[tuple]) -> int:
    return len({m[0] for m in members})


def _sum(members: list[tuple]) -> int | None:
    return sum(m[3] for m in members) if members else None


def _expected(store: dict | None, web: dict | None) -> list[tuple]:
    sides = [side for side in (store, web) if side is not None]
    years = set().union(*sides)
    return sort_rows([(y, *(side.get(y) for side in sides)) for y in years])


def _shifted(rows: list[tuple]) -> list[tuple]:
    return sort_rows([(None if y is None else y + 1, *rest) for y, *rest in rows])


STORE_COUNT = _by_year(STORE, _count)
WEB_COUNT = _by_year(WEB, _count)

# name -> (select body given store/web year spellings, expected rows)
QUERIES = {
    "both_counts": (
        "select {s}, count(ticket) as s, count(order_number) as w;",
        _expected(STORE_COUNT, WEB_COUNT),
    ),
    "both_sums": (
        "select {s}, sum(s_paid) as s, sum(w_paid) as w;",
        _expected(_by_year(STORE, _sum), _by_year(WEB, _sum)),
    ),
    "store_only": (
        "select {s}, count(ticket) as s;",
        _expected(STORE_COUNT, None),
    ),
    "web_only": (
        "select {w}, count(order_number) as w;",
        _expected(None, WEB_COUNT),
    ),
    "derived_year": (
        "select {s} + 1 as y, count(ticket) as s, count(order_number) as w;",
        _shifted(_expected(STORE_COUNT, WEB_COUNT)),
    ),
    "coalesced": (
        "select coalesce({s}, {w}) as y, count(ticket) as s, count(order_number) as w;",
        _expected(STORE_COUNT, WEB_COUNT),
    ),
    "coalesced_having": (
        (
            "select coalesce({s}, {w}) as y, count(ticket) as s,"
            " count(order_number) as w having s > 0 and w > 0;"
        ),
        [row for row in _expected(STORE_COUNT, WEB_COUNT) if row[1] and row[2]],
    ),
}


def _peak_rows(engine, sql: str, profile: Path) -> int:
    connection = engine.connection.connection.driver_connection
    connection.execute("PRAGMA enable_profiling='json'")
    connection.execute(f"PRAGMA profiling_output='{profile.as_posix()}'")
    try:
        connection.execute(sql).fetchall()
    finally:
        connection.execute("PRAGMA disable_profiling")
    peak = 0
    stack = [json.loads(profile.read_text())]
    while stack:
        node = stack.pop()
        peak = max(peak, node.get("operator_cardinality") or 0)
        stack.extend(node.get("children", []))
    return peak


@pytest.mark.parametrize("returns", [False, True], ids=["plain", "with_returns"])
@pytest.mark.parametrize("query", list(QUERIES))
@pytest.mark.parametrize("form", list(FORMS))
def test_shared_attribute_aggregates(form, query, returns, tmp_path):
    model, relation, store_year, web_year = FORMS[form]
    store_date, web_date = DATE_KEYS[form]
    env = Environment()
    env.parse(model.replace("{facts}", _facts(store_date, web_date, returns)))
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    body, expected = QUERIES[query]
    text = relation + body.format(s=store_year, w=web_year)
    sql = engine.generate_sql(text)[-1]
    rows = sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])
    assert rows == expected, sql
    peak = _peak_rows(engine, sql, tmp_path / "profile.json")
    assert peak <= PEAK_BOUND, f"peak {peak} rows > {PEAK_BOUND}\n{sql}"
