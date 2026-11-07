"""`formal/Trilogy/Padding.lean` on ClickHouse (chdb). Its types are NOT NULL
unless wrapped, and several of its defaults read a NULL as the type's default
value; the language means NULL on every engine:

- an outer join pads a row it could not match with NULL
  (`Padding.null_pad_count_eq_matches`, `Padding.null_pad_sum_eq_matches`),
  not `0` (`Padding.default_pad_counts_unmatched`,
  `Padding.default_pad_max_raises`);
- a cast keeps NULL (`Padding.cast_keeps_null`);
- a typed constant keeps its type, so a date function reads it as a date.

Every case runs on DuckDB beside ClickHouse and both must agree with the
reference."""

import random
from collections import defaultdict
from datetime import date, datetime
from typing import Any

import pytest

from tests.windows.test_window_laws import new_executor
from trilogy import Dialects
from trilogy.core.enums import FunctionType
from trilogy.core.models.core import ArrayType, DataType
from trilogy.dialect.clickhouse import ClickhouseDialect

pytest.importorskip("chdb")

DIALECTS = [Dialects.DUCK_DB, Dialects.CLICKHOUSE]
SEEDS = range(4)
Rows = list[dict[str, Any]]


def make(seed: int) -> tuple[Rows, Rows]:
    rng = random.Random(seed)
    customers = [
        {"id": i, "name": rng.choice(["a", "b", "c"])}
        for i in range(1, rng.randint(3, 8))
    ]
    orders = [
        {"oid": 100 + i, "cust": rng.choice(customers)["id"], "amt": rng.randint(-5, 5)}
        for i in range(rng.randint(0, 6))
    ]
    return customers, orders


def union(rows: Rows, cols: list[str], empty: str) -> str:
    if not rows:
        return empty
    return " union all\n".join(
        "select " + ", ".join(f"{r[c]!r} as {c}" for c in cols) for r in rows
    )


def model(customers: Rows, orders: Rows) -> str:
    no_orders = "select 0 as oid, 0 as cust, 0 as amt where 1 = 0"
    return f"""
key id int;
property id.name string;
key oid int;
property oid.amt int;
datasource customers (id: id, name: name) grain (id) query '''
{union(customers, ["id", "name"], "")}
''';
datasource orders (oid: oid, cust: ~id, amt: amt) grain (oid) query '''
{union(orders, ["oid", "cust", "amt"], no_orders)}
''';
"""


def normalized(rows: list) -> list[tuple]:
    return sorted((tuple(r) for r in rows), key=repr)


def run(dialect: Dialects, text: str) -> list[tuple]:
    return normalized(new_executor(dialect).execute_text(text)[-1].fetchall())


def present_max(values: list[int]) -> int | None:
    return max(values) if values else None


def padded_reference(customers: Rows, orders: Rows, query: str) -> list[tuple]:
    """A customer with no order is padded with NULL: counted 0, summed and
    maxed as NULL."""
    by_id: dict[int, list[int]] = defaultdict(list)
    for o in orders:
        by_id[o["cust"]].append(o["amt"])
    if query == "per_customer":
        return normalized(
            (
                c["id"],
                len(by_id[c["id"]]),
                sum(by_id[c["id"]]) if by_id[c["id"]] else None,
                present_max(by_id[c["id"]]),
            )
            for c in customers
        )
    if query == "pairs":
        ids = {o["cust"] for o in orders}
        return normalized(
            [(o["cust"], o["oid"]) for o in orders]
            + [(c["id"], None) for c in customers if c["id"] not in ids]
        )
    by_name: dict[str, list[int]] = defaultdict(list)
    for c in customers:
        by_name[c["name"]].extend(by_id[c["id"]])
    return normalized((n, len(v), present_max(v)) for n, v in by_name.items())


PADDED = {
    "per_customer": "select id, count(oid) as c, sum(amt) as s, max(amt) as m;",
    "pairs": "select id, oid;",
    "per_name": "select name, count(oid) as c, max(amt) as m;",
}


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("query", PADDED)
def test_outer_join_pads_null(dialect: Dialects, query: str) -> None:
    for seed in SEEDS:
        customers, orders = make(seed)
        text = model(customers, orders) + PADDED[query]
        expected = padded_reference(customers, orders, query)
        assert run(dialect, text) == expected, (seed, PADDED[query])


CASTS = {
    "y::int": [3, None],
    "y::float": [3.0, None],
    "y::string": ["3", None],
    "d::date": [date(2020, 1, 2), None],
    "d::datetime": [datetime(2020, 1, 2), None],
}
NULLABLE = """
key id int;
property id.y string?;
property id.d string?;
datasource t (id: id, y: y, d: d) grain (id) query '''
select 1 as id, '3' as y, '2020-01-02' as d union all
select 2 as id, nullif('', '') as y, nullif('', '') as d
''';
"""


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("cast", CASTS)
def test_cast_keeps_null(dialect: Dialects, cast: str) -> None:
    text = NULLABLE + f"select id, {cast} as x order by id asc;"
    got = new_executor(dialect).execute_text(text)[-1].fetchall()
    assert [r[1] for r in got] == CASTS[cast]


CONSTANTS = {
    "const t <- '2020-05-17 03:04:05'::datetime; select date_trunc(t, month) as x;": date(
        2020, 5, 1
    ),
    "const t <- '2020-05-17'::date; select date_add(t, month, 1) as x;": date(
        2020, 6, 17
    ),
    "const t <- '2020-05-17'::date; select date_trunc(t, year) as x;": date(2020, 1, 1),
    "const t <- '2020-05-17 03:04:05'::datetime; select date_add(t, day, 1) as x;": datetime(
        2020, 5, 18, 3, 4, 5
    ),
    "const t <- '2020-05-17'::date; select month(t) as x;": 5,
}


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("text", CONSTANTS)
def test_typed_constant_keeps_its_type(dialect: Dialects, text: str) -> None:
    assert run(dialect, text) == [(CONSTANTS[text],)]


def test_settings_only_where_a_join_pads() -> None:
    customers, orders = make(0)
    executor = new_executor(Dialects.CLICKHOUSE)
    executor.parse_text(model(customers, orders))
    padded = executor.generate_sql("select id, oid;")[-1]
    plain = executor.generate_sql("select id, name;")[-1]
    assert padded.rstrip().endswith("SETTINGS join_use_nulls = 1")
    assert "SETTINGS" not in plain


def test_cast_renders_nullable_scalars() -> None:
    cast = ClickhouseDialect.FUNCTION_MAP
    assert (
        cast[FunctionType.CAST](["x", "Int64"], [DataType.STRING, DataType.INTEGER])
        == "CAST(x AS Nullable(Int64))"
    )
    array = ArrayType(type=DataType.INTEGER)
    assert (
        cast[FunctionType.CAST](["x", "Array(Int64)"], [DataType.STRING, array])
        == "CAST(x AS Array(Int64))"
    )
