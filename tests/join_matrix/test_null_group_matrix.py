"""Two facts reading ONE key through their own bindings, no declared relation.

A complete binding (`?k`) says a side carries every value of the key's domain,
and a `~?k` binding a subset of them. Neither says the side holds a NULL group:
that is data. So no NULL placement may lose a row, whichever side holds it
(`docs/query_gen_simplification_audit_2026_10.md`, seam 2's fifth slice). The
declared-relation matrix (`test_two_source_matrix.py`) puts a NULL row on every
side, so it cannot see this.

Axes: each side's binding, which sides hold NULL-key rows, the key kind (the key
itself, an expression over it, an attribute of a dimension keyed by it, and an
expression over that), and whether a dimension holding the whole domain is in
the model. The oracle is a null-safe FULL over per-side aggregates, plus every
member the dimension holds.
"""

import pytest

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Environment

DOMAIN = [1, 2, 3]
SUBSET = [1, 2]
YEARS = {1: 2020, 2: 2021, 3: 2022}

# kind -> (select expression, key function over the bound key, needs a dimension)
KINDS = {
    "root": ("k", lambda k: k, False),
    "derived": ("k + 1", lambda k: k + 1, False),
    "attribute": ("year", lambda k: YEARS[k], True),
    "derived_attribute": ("year + 1", lambda k: YEARS[k] + 1, True),
}
BINDINGS = {"complete": "?k", "subset": "~?k"}
NULLS = ["left", "right", "both", "neither"]


def _rows(binding: str, holds_null: bool, base: int) -> list[tuple[int, int | None]]:
    keys: list[int | None] = list(DOMAIN if binding == "complete" else SUBSET)
    if holds_null:
        keys.append(None)
    # two rows on the first key, so a fan-out or a dropped row moves a sum
    keys = [keys[0], *keys]
    return [(key, base * 2**i) for i, key in enumerate(keys)]


def _source(name: str, rows: list[tuple[int | None, int]]) -> str:
    return " union all ".join(
        f"select {i} i, {'cast(null as int)' if k is None else k} k, {v} v"
        for i, (k, v) in enumerate(rows)
    )


def _model(left: list, right: list, lb: str, rb: str, dimension: bool) -> str:
    model = f"""
key k int;
key l_id int;
property l_id.l_val int;
datasource lf (i: l_id, k: {BINDINGS[lb]}, v: l_val) grain (l_id)
query '''{_source("lf", left)}''';
key r_id int;
property r_id.r_val int;
datasource rf (i: r_id, k: {BINDINGS[rb]}, v: r_val) grain (r_id)
query '''{_source("rf", right)}''';
"""
    if dimension:
        domain = " union all ".join(f"select {k} k, {YEARS[k]} y" for k in DOMAIN)
        model += f"""
property k.year int;
datasource dim (k: k, y: year) grain (k)
query '''{domain}''';
"""
    return model


def _aggregate(rows: list[tuple[int | None, int]], key_fn) -> dict:
    out: dict = {}
    for k, v in rows:
        kk = None if k is None else key_fn(k)
        out[kk] = out.get(kk, 0) + v
    return out


def _expected(left: list, right: list, key_fn, dimension: bool) -> list[tuple]:
    lsum, rsum = _aggregate(left, key_fn), _aggregate(right, key_fn)
    keys = set(lsum) | set(rsum)
    if dimension:
        keys |= {key_fn(k) for k in DOMAIN}
    return sort_rows([(k, lsum.get(k), rsum.get(k)) for k in keys])


CELLS = [
    (kind, lb, rb, nulls, dimension)
    for kind, (_, _, needs_dimension) in KINDS.items()
    for lb in BINDINGS
    for rb in BINDINGS
    for nulls in NULLS
    for dimension in ([True] if needs_dimension else [False, True])
]


@pytest.mark.parametrize("kind,lb,rb,nulls,dimension", CELLS)
def test_null_groups_survive(kind: str, lb: str, rb: str, nulls: str, dimension: bool):
    select, key_fn, _ = KINDS[kind]
    left = _rows(lb, nulls in ("left", "both"), 1)
    right = _rows(rb, nulls in ("right", "both"), 100)
    env = Environment()
    env.parse(_model(left, right, lb, rb, dimension))
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = f"select {select} as key, sum(l_val) as l, sum(r_val) as r;"
    sql = executor.generate_sql(query)[-1]
    rows = sort_rows([tuple(r) for r in executor.execute_raw_sql(sql).fetchall()])
    assert rows == _expected(left, right, key_fn, dimension), sql
