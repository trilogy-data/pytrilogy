"""Side-qualified null test on a SUBSET join key expresses intersection.

A `subset join a.k = b.k` (a ⊆ b) renders row-preserving with b as the anchor
and a NULL-padded on anchor-only rows. `a.k is not null` therefore asks the
per-ROW question "did the subset side match?" — the intersection idiom — but
pre-fix read the coalesced group key and silently no-op'd (the anchor key is
always present). The presence probe (`Factory._coalescing_presence_probe`),
already wired for `full`/`union` members, now also fires for subset SOURCES.

The superset/anchor side (`b.k`) is preserved, so its key is never null: a
null test on it is a genuine no-op and stays one. Projecting either member is
untouched — it remains the coalesced group axis.
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

# store 10 -> 2001 only (a), 20 & 30 -> both, 40 -> 2002 only (b).
MODEL = """
key store_id int;
property store_id.sname string;
datasource stores (i: store_id, n: sname) grain (store_id)
query '''
select 10 i, 'S10' n union all select 20 i, 'S20' n
union all select 30 i, 'S30' n union all select 40 i, 'S40' n
''';

key sid int;
property sid.yr int;
property sid.st int;
property sid.amt int;
datasource sales (i: sid, y: yr, s: st, a: amt) grain (sid)
query '''
select 1 i, 2001 y, 10 s, 100 a union all select 2 i, 2001 y, 20 s, 200 a
union all select 3 i, 2001 y, 30 s, 300 a union all select 4 i, 2002 y, 20 s, 250 a
union all select 5 i, 2002 y, 30 s, 350 a union all select 6 i, 2002 y, 40 s, 400 a
''';
merge st into store_id;

rowset a <- where yr = 2001 select st as store, sname as nm, sum(amt) as amt;
rowset b <- where yr = 2002 select st as store, sname as nm, sum(amt) as amt;
"""


def _run(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(MODEL + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


# subset join a.st = b.st: a ⊆ b, b is the anchor (rows kept, a NULL-padded).
SUBSET = "select a.store, a.amt, b.amt subset join a.store = b.store "
UNION = "select a.store, a.amt, b.amt union join a.store = b.store "

DIRECTIONAL = [(20, 200, 250), (30, 300, 350), (40, None, 400)]
INTERSECTION = [(20, 200, 250), (30, 300, 350)]


def test_subset_no_condition_row_preserving(tmp_path: Path):
    assert _run(tmp_path, SUBSET + ";") == sort_rows(DIRECTIONAL)


def test_subset_side_key_null_test_intersects(tmp_path: Path):
    # a.store is the SUBSET side: NULL where the subset side is absent (store
    # 40 is anchor-only). Pre-fix this no-op'd on the coalesced key.
    assert _run(tmp_path, SUBSET + "where a.store is not null;") == sort_rows(
        INTERSECTION
    )


def test_subset_side_property_null_test_intersects(tmp_path: Path):
    # a per-side property (a's own materialized column) already reads NULL on
    # anchor-only rows — same intersection, guarding it stays correct.
    assert _run(tmp_path, SUBSET + "where a.nm is not null;") == sort_rows(INTERSECTION)


def test_subset_anchor_key_null_test_is_noop(tmp_path: Path):
    # b.store is the superset/anchor: preserved, never null -> genuine no-op.
    assert _run(tmp_path, SUBSET + "where b.store is not null;") == sort_rows(
        DIRECTIONAL
    )


def test_union_both_sides_symmetric(tmp_path: Path):
    both = [(10, 100, None), (20, 200, 250), (30, 300, 350), (40, None, 400)]
    assert _run(tmp_path, UNION + ";") == sort_rows(both)
    assert _run(tmp_path, UNION + "where a.store is not null;") == sort_rows(
        [(10, 100, None), (20, 200, 250), (30, 300, 350)]
    )
    assert _run(tmp_path, UNION + "where b.store is not null;") == sort_rows(
        DIRECTIONAL
    )
