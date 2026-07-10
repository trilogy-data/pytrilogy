"""`subset join` onto a rowset superset anchor narrows DIRECTIONALLY (q54 family).

A rowset boundary is OPAQUE: its output is a freshly-named concept whose value
set is whatever the body produces, so the body WHERE/HAVING *defines* that
domain rather than restricting a pre-existing one. At the rename boundary the
anchor is therefore complete BY CONSTRUCTION, and `subset join a = rs.k`
(declaring a ⊆ rs.k) narrows to the directional superset-anchored LEFT exactly
like a plain datasource or an unfiltered rowset: non-members of the declared
superset drop, unmatched anchor rows survive NULL-padded.

The flipping variable is NOT rowset-vs-plain — filtered, unfiltered, and plain
datasource anchors all narrow identically (pinned below). "Keep both sides"
(neither domain contains the other) is the explicit `union join` / FULL form.
Row membership (q54's actual intent) is `in` (semijoin) or the `is not null`
idiom.
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

# member 7 has no sale (unmatched anchor row → NULL-padded); non-member 9 has a
# sale but is outside the declared superset (dropped by directional narrowing).
DIRECTIONAL = [(1, 10), (2, 20), (3, 30), (7, None)]


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


def test_filtered_rowset_anchor_subset_join_narrows(tmp_path: Path):
    query = (
        FILTERED + "select cust, sum(amount) as total subset join cust = members.mid;"
    )
    # filtered rowset anchor is complete at its opaque boundary → non-member 9
    # drops, unmatched member 7 kept.
    assert _rows(tmp_path, query) == DIRECTIONAL


def test_unfiltered_rowset_anchor_subset_join_narrows(tmp_path: Path):
    query = (
        UNFILTERED + "select cust, sum(amount) as total subset join cust = members.mid;"
    )
    assert _rows(tmp_path, query) == DIRECTIONAL


def test_plain_datasource_anchor_subset_join_narrows(tmp_path: Path):
    query = "select cust, sum(amount) as total subset join cust = mid_raw;"
    # identical to both rowset forms — proves the flip is completeness, not
    # rowset-vs-plain.
    assert _rows(tmp_path, query) == DIRECTIONAL


def test_union_join_keeps_both_sides(tmp_path: Path):
    query = (
        FILTERED + "select cust, sum(amount) as total union join cust = members.mid;"
    )
    # ∦ declaration: neither domain contains the other, so both the unmatched
    # member (7) and the non-member sale (9) survive.
    assert _rows(tmp_path, query) == [(1, 10), (2, 20), (3, 30), (7, None), (9, 999)]


def test_membership_via_in_is_semijoin(tmp_path: Path):
    query = FILTERED + "where cust in members.mid select cust, sum(amount) as total;"
    # q54's actual intent — only members with sales, no padded rows.
    assert _rows(tmp_path, query) == [(1, 10), (2, 20), (3, 30)]


def test_explicit_is_not_null_matches_directional(tmp_path: Path):
    query = (
        FILTERED + "where members.mid is not null "
        "select cust, sum(amount) as total subset join cust = members.mid;"
    )
    # the `is not null` idiom composes with (and here coincides with) the
    # directional narrowing.
    assert _rows(tmp_path, query) == DIRECTIONAL
