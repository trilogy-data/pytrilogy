"""Presence tests across coalescing (`full`/`union`) joins between two
INDEPENDENT rowsets over separate facts (the TPC-DS q97 shape): each side's
NULL marker must survive the join.

Two framework defects pinned here (evals/tpcds_agent/
bug_q97_coalescing_join_presence_and_derived_key_recursion.md):

- Bug 1: a derived-expression join key (cast/concat) with BOTH endpoints
  derived fell onto the build-time substitution path, destroying one side's
  compute expression; the surviving key then re-sourced through its own
  rowset -> RecursionError ("query could not be planned; this is a bug").
- Bug 2: a null test on a coalescing key-group member read the group's
  mandatory coalesce -- never NULL on a half-matched row -- so presence
  counts silently collapsed to (0, 0, N). Null tests now bind to a per-side
  presence probe materialized before the merge. PROJECTING a member is
  unchanged: it stays the coalesced group axis
  (test_independent_rowset_matrix.py).

Mirrors the fuzzer family `coalescing_presence` (local_scripts/fuzzer).
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

# (cust, item) pairs per channel; grain of each rowset
STORE_PAIRS: list[tuple[int, int]] = [(1, 10), (1, 20), (2, 10)]
CATALOG_PAIRS: list[tuple[int, int]] = [(1, 10), (3, 30)]

MODEL = """
key srid int;
property srid.s_cust int;
property srid.s_item int;
datasource store_fact (r: srid, c: s_cust, i: s_item) grain (srid)
query '''{store}''';

key crid int;
property crid.c_cust int;
property crid.c_item int;
datasource catalog_fact (r: crid, c: c_cust, i: c_item) grain (crid)
query '''{catalog}''';

with store_set as
select s_cust as cust, s_item as item;

with catalog_set as
select c_cust as cust, c_item as item;
"""

PRESENCE = """
select
  sum(case when store_set.cust is not null and catalog_set.cust is null then 1 else 0 end) as store_only,
  sum(case when store_set.cust is null and catalog_set.cust is not null then 1 else 0 end) as catalog_only,
  sum(case when store_set.cust is not null and catalog_set.cust is not null then 1 else 0 end) as both_channels
{join};
"""


def _rows_sql(pairs: list[tuple[int, int]]) -> str:
    return " union all ".join(
        f"select {i + 1} r, {c} c, {it} i" for i, (c, it) in enumerate(pairs)
    )


def _run(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    text = MODEL.format(store=_rows_sql(STORE_PAIRS), catalog=_rows_sql(CATALOG_PAIRS))
    statements = engine.parse_text(text + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


def _full_join(key_s, key_c) -> list[tuple[tuple | None, tuple | None]]:
    """Null-safe FULL join of the two pair sets on the given key functions."""
    rows: list[tuple[tuple | None, tuple | None]] = []
    matched_c: set[tuple[int, int]] = set()
    for s in STORE_PAIRS:
        hits = [c for c in CATALOG_PAIRS if key_c(c) == key_s(s)]
        matched_c.update(hits)
        rows.extend((s, c) for c in hits)
        if not hits:
            rows.append((s, None))
    rows.extend((None, c) for c in CATALOG_PAIRS if c not in matched_c)
    return rows


def _presence_counts(key_s, key_c) -> tuple[int, int, int]:
    rows = _full_join(key_s, key_c)
    return (
        sum(1 for s, c in rows if s is not None and c is None),
        sum(1 for s, c in rows if s is None and c is not None),
        sum(1 for s, c in rows if s is not None and c is not None),
    )


SINGLE_KEY = (lambda p: p[0], lambda p: p[0])
COMPOSITE_KEY = (lambda p: p, lambda p: p)
CONCAT_KEY = (lambda p: f"{p[0]}{p[1]}", lambda p: f"{p[0]}{p[1]}")


def test_presence_union_plain_single(tmp_path: Path):
    rows = _run(
        tmp_path,
        PRESENCE.format(join="union join store_set.cust = catalog_set.cust"),
    )
    assert rows == [_presence_counts(*SINGLE_KEY)]


def test_presence_full_plain_single(tmp_path: Path):
    rows = _run(
        tmp_path,
        PRESENCE.format(join="full join store_set.cust = catalog_set.cust"),
    )
    assert rows == [_presence_counts(*SINGLE_KEY)]


def test_presence_union_plain_composite(tmp_path: Path):
    rows = _run(
        tmp_path,
        PRESENCE.format(
            join="union join store_set.cust = catalog_set.cust"
            " and store_set.item = catalog_set.item"
        ),
    )
    assert rows == [_presence_counts(*COMPOSITE_KEY)]


def test_presence_full_plain_composite(tmp_path: Path):
    rows = _run(
        tmp_path,
        PRESENCE.format(
            join="full join store_set.cust = catalog_set.cust"
            " and store_set.item = catalog_set.item"
        ),
    )
    assert rows == [_presence_counts(*COMPOSITE_KEY)]


def test_presence_union_cast_single(tmp_path: Path):
    # Bug 1 minimal trigger: a single cast key between two independent
    # rowsets recursed at plan time.
    rows = _run(
        tmp_path,
        PRESENCE.format(
            join="union join store_set.cust::string = catalog_set.cust::string"
        ),
    )
    assert rows == [_presence_counts(*SINGLE_KEY)]


def test_presence_full_cast_single(tmp_path: Path):
    rows = _run(
        tmp_path,
        PRESENCE.format(
            join="full join store_set.cust::string = catalog_set.cust::string"
        ),
    )
    assert rows == [_presence_counts(*SINGLE_KEY)]


def test_presence_union_concat_composite(tmp_path: Path):
    rows = _run(
        tmp_path,
        PRESENCE.format(
            join="union join concat(store_set.cust::string, store_set.item::string)"
            " = concat(catalog_set.cust::string, catalog_set.item::string)"
        ),
    )
    assert rows == [_presence_counts(*CONCAT_KEY)]


def test_presence_where_filter(tmp_path: Path):
    # WHERE-level presence: `store_set.cust is null` keeps only catalog-only
    # pairs. Pre-fix this silently read the coalesced key (empty result) or
    # dropped the filter entirely (probe unrecognized by the cross-rowset
    # WHERE gate).
    rows = _run(
        tmp_path,
        "select catalog_set.cust, catalog_set.item "
        "union join store_set.cust = catalog_set.cust"
        " and store_set.item = catalog_set.item "
        "where store_set.cust is null;",
    )
    want = [c for s, c in _full_join(*COMPOSITE_KEY) if s is None]
    assert rows == sort_rows(want)


def test_presence_projection_stays_coalesced(tmp_path: Path):
    # Projecting a key-group member still renders the coalesced group axis;
    # only the null test binds per-side. Both semantics in one statement.
    rows = _run(
        tmp_path,
        "select store_set.cust, catalog_set.cust,"
        " case when store_set.cust is not null and catalog_set.cust is null"
        " then 1 else 0 end as store_only_flag,"
        " case when store_set.cust is null and catalog_set.cust is not null"
        " then 1 else 0 end as catalog_only_flag "
        "union join store_set.cust = catalog_set.cust;",
    )
    want = []
    for s, c in _full_join(*SINGLE_KEY):
        axis = s[0] if s is not None else c[0]
        want.append((axis, axis, 1 if c is None else 0, 1 if s is None else 0))
    assert rows == sort_rows(want)
