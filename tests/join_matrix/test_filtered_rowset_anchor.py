"""q54 REFUTATION guard: `subset join` onto a FILTERED rowset superset anchor
renders ROW-PRESERVING BY DESIGN — it is NOT a framework bug.

The 20260706 sink handoff claimed a `subset join a = rs.k` whose superset
anchor `rs.k` is a filtered rowset "leaks" non-members (renders FULL instead of
a directional LEFT) and blamed rowset-vs-plain anchor. That is the documented
semantics, not a defect: a subset/left/partial-merge relation narrows to a
directional (member-dropping) join ONLY when the superset side is provably
value-complete. A FILTERED superset cannot prove containment, so the preserving
render stands and unmatched subset-side rows survive NULL-padded — identical to
the `partial_merge_left_anchor` and `test_rowset_outer_join_shared_base_no_fanout`
cells, which pin the same rule for filtered rowsets.

The handoff misidentified the flipping variable: an UNFILTERED rowset anchor
narrows exactly like a plain datasource (see `test_unfiltered_*` below), so the
real variable is filtered-vs-unfiltered superset, not rowset-vs-plain.

Membership (q54's actual intent) is spelled with `in` (semijoin) or the explicit
`is not null` idiom, both pinned here.
"""

from pathlib import Path

from tests.join_matrix.harness import make_engine

MODEL = """
key sid int;
property sid.cust int;
property sid.amount int;
datasource sales (i: sid, c: cust, a: amount)
grain (sid)
query '''
select 1 i, 1 c, 10 a
union all select 2 i, 2 c, 20 a
union all select 3 i, 3 c, 30 a
union all select 4 i, 9 c, 999 a
''';

key mid_raw int;
property mid_raw.flag int;
datasource cohort_src (m: mid_raw, f: flag)
grain (mid_raw)
query '''
select 1 m, 1 f
union all select 2 m, 1 f
union all select 3 m, 1 f
union all select 7 m, 1 f
''';
"""

FILTERED = "rowset members <- select mid_raw as mid where flag = 1;\n"
UNFILTERED = "rowset members <- select mid_raw as mid;\n"


def _rows(tmp_path: Path, query: str) -> list[tuple]:
    (tmp_path / "base.preql").write_text(MODEL)
    engine = make_engine(tmp_path)
    engine.parse_text("import base;")
    statements = engine.parse_text(query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sorted(
        (tuple(r) for r in engine.execute_raw_sql(sql).fetchall()),
        key=lambda r: tuple((x is None, x) for x in r),
    )


def test_filtered_rowset_anchor_subset_join_is_preserving(tmp_path: Path):
    query = (
        FILTERED + "select cust, sum(amount) as total subset join cust = members.mid;"
    )
    # non-member 9 survives (padded superset) and member 7 (no sale) survives:
    # the filtered superset can't prove containment, so preserving render stands.
    assert _rows(tmp_path, query) == [(1, 10), (2, 20), (3, 30), (7, None), (9, 999)]


def test_unfiltered_rowset_anchor_subset_join_narrows(tmp_path: Path):
    query = (
        UNFILTERED + "select cust, sum(amount) as total subset join cust = members.mid;"
    )
    # an UNFILTERED superset proves containment → directional LEFT drops the
    # non-member (9), keeps the unmatched member (7): identical to a plain
    # datasource anchor. Proves the flip is filtered-vs-unfiltered, not rowset.
    assert _rows(tmp_path, query) == [(1, 10), (2, 20), (3, 30), (7, None)]


def test_membership_via_in_is_semijoin(tmp_path: Path):
    query = FILTERED + "where cust in members.mid select cust, sum(amount) as total;"
    # q54's actual intent — only members with sales, no padded rows.
    assert _rows(tmp_path, query) == [(1, 10), (2, 20), (3, 30)]


def test_explicit_is_not_null_restricts_subset_join(tmp_path: Path):
    query = (
        FILTERED + "where members.mid is not null "
        "select cust, sum(amount) as total subset join cust = members.mid;"
    )
    # documented idiom to drop non-anchor rows from the preserving render:
    # non-member 9 gone, unmatched member 7 kept.
    assert _rows(tmp_path, query) == [(1, 10), (2, 20), (3, 30), (7, None)]
