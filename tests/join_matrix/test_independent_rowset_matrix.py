"""Union join between two INDEPENDENT `where ... select` rowsets over one base.

The q59 shape: two filtered aggregations of the same fact (this-year vs
next-year), related only by an authored `union join`. Regression cells for the
c0d82fb1e collapse, where discovery satisfied one side's join key through its
group-mate pseudonym and never built the other rowset (single key: the entire
side dropped; composite: the derived or plain co-key silently left the join).

Sweeps key shape (single / composite plain / composite derived) x projection
(keys only / keys plus measures). Expected rows are computed from the fact rows
by a python oracle implementing the ruled semantics: null-safe FULL join over
every authored key pair, and a projected member of a coalescing key group
renders as the row-by-row coalesce of the whole group.
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

# (rid, store, week, year, sales)
FACT_ROWS: list[tuple[int, int, int, int, int]] = [
    (1, 10, 1, 2001, 5),
    (2, 10, 1, 2001, 7),
    (3, 10, 2, 2001, 11),
    (4, 20, 1, 2001, 13),
    (5, 10, 53, 2002, 17),
    (6, 20, 53, 2002, 19),
    (7, 20, 54, 2002, 23),
    (8, 30, 53, 2002, 29),
]

MODEL_HEAD = """
key rid int;
property rid.store int;
property rid.week int;
property rid.year int;
property rid.sales int;
datasource fact (r: rid, s: store, w: week, y: year, v: sales) grain (rid)
query '''{source}''';
auto total <- sum(sales) by store, week, year;
with this_year as
where year = 2001
select store as code, week as wk, total;
with next_year as
where year = 2002
select store as code, week as wk, total;
"""


def _source_sql() -> str:
    return " union all ".join(
        f"select {r} r, {s} s, {w} w, {y} y, {v} v" for r, s, w, y, v in FACT_ROWS
    )


def _aggregate(year: int) -> dict[tuple[int, int], int]:
    out: dict[tuple[int, int], int] = {}
    for _, store, week, row_year, sales in FACT_ROWS:
        if row_year == year:
            out[(store, week)] = out.get((store, week), 0) + sales
    return out


def _full_join_rows(
    right_key_fn,
) -> list[tuple[tuple[int, int] | None, tuple[int, int] | None]]:
    """Null-safe FULL join of the two aggregates: this-year keys against
    `right_key_fn`-mapped next-year keys. Returns (ty key, ny key) per row."""
    ty, ny = _aggregate(2001), _aggregate(2002)
    rows: list[tuple[tuple[int, int] | None, tuple[int, int] | None]] = []
    matched_ny = set()
    for ty_key in ty:
        hits = [ny_key for ny_key in ny if right_key_fn(ny_key) == ty_key]
        matched_ny.update(hits)
        rows.extend((ty_key, ny_key) for ny_key in hits)
        if not hits:
            rows.append((ty_key, None))
    rows.extend((None, ny_key) for ny_key in ny if ny_key not in matched_ny)
    return rows


def _run(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(MODEL_HEAD.format(source=_source_sql()) + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


def _single_key_rows() -> list[tuple[tuple[int, int] | None, tuple[int, int] | None]]:
    """Fan-out join on store code alone: (ty key, ny key) per output row."""
    ty, ny = _aggregate(2001), _aggregate(2002)
    matched = [(t, n) for t in ty for n in ny if t[0] == n[0]]
    matched_ty = {t for t, _ in matched}
    matched_ny = {n for _, n in matched}
    return (
        matched
        + [(t, None) for t in ty if t not in matched_ty]
        + [(None, n) for n in ny if n not in matched_ny]
    )


def test_union_join_single_key_keys_only(tmp_path: Path):
    # The q59 smoking gun: only the coalesced keys projected. The collapse
    # built ONE rowset and aliased its key as both columns (3 rows, no fanout).
    rows = _run(
        tmp_path,
        "select this_year.code, next_year.code "
        "union join this_year.code = next_year.code;",
    )
    want = []
    for ty_key, ny_key in _single_key_rows():
        code = ty_key[0] if ty_key else ny_key[0]
        want.append((code, code))
    assert rows == sort_rows(want)


def test_union_join_single_key_with_measures(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select this_year.code, this_year.wk, next_year.wk,"
        " this_year.total, next_year.total "
        "union join this_year.code = next_year.code;",
    )
    ty, ny = _aggregate(2001), _aggregate(2002)
    want = []
    for ty_key, ny_key in _single_key_rows():
        code = ty_key[0] if ty_key else ny_key[0]
        want.append(
            (
                code,
                ty_key[1] if ty_key else None,
                ny_key[1] if ny_key else None,
                ty.get(ty_key) if ty_key else None,
                ny.get(ny_key) if ny_key else None,
            )
        )
    assert rows == sort_rows(want)


def test_union_join_composite_plain_keys(tmp_path: Path):
    # Weeks are disjoint across years, so a correct composite join matches
    # nothing; dropping either co-key would fabricate matches.
    rows = _run(
        tmp_path,
        "select this_year.code, this_year.wk, this_year.total, next_year.total "
        "union join this_year.code = next_year.code"
        " and this_year.wk = next_year.wk;",
    )
    ty, ny = _aggregate(2001), _aggregate(2002)
    assert not set(ty) & set(ny)
    want = [(store, week, total, None) for (store, week), total in ty.items()] + [
        (store, week, None, total) for (store, week), total in ny.items()
    ]
    assert rows == sort_rows(want)


def test_union_join_composite_derived_key(tmp_path: Path):
    # `this_year.wk = next_year.wk - 52`: both the plain code co-key and the
    # derived week key must survive into the join. The collapse dropped one or
    # the other, fanning out on whichever remained.
    rows = _run(
        tmp_path,
        "select this_year.code, this_year.wk, this_year.total, next_year.total "
        "union join this_year.code = next_year.code"
        " and this_year.wk = next_year.wk - 52;",
    )
    ty, ny = _aggregate(2001), _aggregate(2002)
    want = []
    for ty_key, ny_key in _full_join_rows(lambda k: (k[0], k[1] - 52)):
        code = ty_key[0] if ty_key else ny_key[0]
        # wk is a coalescing group member: it renders as the coalesce of the
        # authored side and the derived expression from the other side.
        week = ty_key[1] if ty_key else ny_key[1] - 52
        want.append(
            (
                code,
                week,
                ty.get(ty_key) if ty_key else None,
                ny.get(ny_key) if ny_key else None,
            )
        )
    assert rows == sort_rows(want)
