"""Tier 2: composite join keys over two re-aggregated rowsets.

Sweeps the composite-key kind (both-plain / plain-equality + derived /
derived + derived) x join type (SUBSET / UNION). This is the TPC-DS q59/q78
family: the plain co-key must survive alongside the derived key in ONE join —
dropping it cross-products (q59 fan-out), splitting it into a second join
level widens LEFT to FULL (comp_mixed leak).

Distinct-address variant only (two independent models). The SHARED-CANONICAL
variant (both sides of the plain co-key passing through one parent rowset) is
pinned live-failing in
tests/test_scoped_derived_rowset_join_matrix.py::test_q59_shared_canonical_composite_left_join_no_fanout
per evals/tpcds_agent/bug_q59_composite_derived_left_join_drops_equality_key_fanout.md.

Data: store 3 exists only on the left, store 4 only on the right — a LEFT that
widens to FULL leaks a store-4 row; any dropped co-key fans stores across each
other (duplicate (store, week) output keys).
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import run_cell, sort_rows

# (store, week, val); right-side weeks sit at +52 so the derived-key variants
# relate `right.week = left.week + 52`, mirroring q59's year-offset join.
LEFT_ROWS = [(1, 1, 10), (1, 2, 20), (2, 1, 30), (3, 1, 40)]
RIGHT_ROWS = [(1, 53, 100), (1, 54, 200), (2, 53, 300), (4, 53, 400)]


def _datasource_sql(rows: list[tuple[int, int, int]]) -> str:
    return " union all ".join(
        f"select {i} i, {s} s, {w} w, {v} v"
        for i, (s, w, v) in enumerate(rows, start=1)
    )


def _model(side: str, rows: list[tuple[int, int, int]]) -> str:
    return (
        f"key {side}_id int;\n"
        f"property {side}_id.{side}_store int;\n"
        f"property {side}_id.{side}_week int;\n"
        f"property {side}_id.{side}_val int;\n"
        f"datasource {side}src (i: {side}_id, s: {side}_store, w: {side}_week,"
        f" v: {side}_val) grain ({side}_id)\n"
        f"query '''{_datasource_sql(rows)}''';\n"
    )


def write_models(tmp_path: Path) -> Path:
    (tmp_path / "this_year.preql").write_text(_model("t", LEFT_ROWS))
    (tmp_path / "next_year.preql").write_text(_model("n", RIGHT_ROWS))
    return tmp_path


_TA = "rowset ta <- select t.t_store as s, t.t_week as w, sum(t.t_val) as tot;\n"
HEAD = (
    "import this_year as t;\n"
    "import next_year as n;\n"
    + _TA
    + "rowset nb <- select n.n_store as s, n.n_week as w, sum(n.n_val) as tot;\n"
)
# both-plain control: bake the +52 offset into the rowset body so both join
# operands are plain rowset outputs
HEAD_SHIFTED = (
    "import this_year as t;\n"
    "import next_year as n;\n"
    + _TA
    + "rowset nb <- select n.n_store as s, n.n_week - 52 as w,"
    " sum(n.n_val) as tot;\n"
)
SELECT = "select ta.s, ta.w, ta.tot, nb.tot order by ta.s asc, ta.w asc;"

# (kind, join_type) -> (head, join key clause). Operand order carries meaning
# for SUBSET: `subset join <subset> = <superset>` anchors its RIGHT (superset)
# operand, so every SUBSET pair trails with ta's key (consistent ta anchor; a
# MIXED-anchor composite legitimately composes to FULL — see
# test_mixed_anchor_composite_composes_to_full). UNION is symmetric; both operand
# orders must behave identically — the derived-as-left-operand order is pinned
# separately in test_union_derived_key_as_left_operand_direction.
CLAUSES = {
    ("both_plain", "subset"): (HEAD_SHIFTED, "nb.s = ta.s and nb.w = ta.w"),
    ("both_plain", "union"): (HEAD_SHIFTED, "ta.s = nb.s and ta.w = nb.w"),
    ("plain_derived", "subset"): (HEAD, "nb.s = ta.s and nb.w = ta.w + 52"),
    ("plain_derived", "union"): (HEAD, "ta.s = nb.s and nb.w = ta.w + 52"),
    ("derived_derived", "subset"): (HEAD, "nb.s = ta.s + 0 and nb.w = ta.w + 52"),
    ("derived_derived", "union"): (HEAD, "nb.s = ta.s + 0 and nb.w = ta.w + 52"),
}


def _matched(left_key: tuple[int, int]) -> int | None:
    s, w = left_key
    match = [v for (rs, rw, v) in RIGHT_ROWS if rs == s and rw == w + 52]
    return sum(match) if match else None


def _expected_left() -> list[tuple]:
    return sort_rows([(s, w, v, _matched((s, w))) for (s, w, v) in LEFT_ROWS])


def _expected_full(kind: str) -> list[tuple]:
    # right-only rows: a key output coalesces across a PLAIN pair (both
    # physical columns exist); a DERIVED pair's ta-side key has no row to
    # compute from, so it is NULL. both_plain coalesces s and w; plain_derived
    # only s; derived_derived neither.
    rows: list[tuple] = [(s, w, v, _matched((s, w))) for (s, w, v) in LEFT_ROWS]
    for rs, rw, rv in RIGHT_ROWS:
        if not any(s == rs and w + 52 == rw for (s, w, _) in LEFT_ROWS):
            store = rs if kind in ("both_plain", "plain_derived") else None
            week = rw - 52 if kind == "both_plain" else None
            rows.append((store, week, None, rv))
    return sort_rows(rows)


@pytest.mark.parametrize(
    "kind,join_type",
    [
        pytest.param("both_plain", "subset"),
        pytest.param("both_plain", "union"),
        pytest.param("plain_derived", "subset"),
        pytest.param("plain_derived", "union"),
        pytest.param("derived_derived", "subset"),
        pytest.param("derived_derived", "union"),
    ],
)
def test_composite_key_join(tmp_path: Path, kind: str, join_type: str):
    head, clause = CLAUSES[(kind, join_type)]
    query = head + f"{join_type} join {clause}\n" + SELECT
    rows = run_cell(write_models(tmp_path), query)
    want = _expected_left() if join_type == "subset" else _expected_full(kind)
    assert rows == want, f"{kind}/{join_type}:\n{query}\ngot {rows}\nwant {want}"
    # any dropped co-key fans stores across each other: output keys must be
    # unique regardless of the row values
    keys = [(r[0], r[1]) for r in rows]
    assert len(keys) == len(set(keys)), f"fan-out: {rows}"


def test_union_derived_key_as_left_operand_direction(tmp_path: Path):
    # UNION is symmetric: the derived expr on the LEFT of `=` (rowset key as the
    # group canonical) must behave identically to the derived-on-the-right
    # form. This direction used to fall through to the substitution path and
    # drop the derived key (intra-store fan-out) or disconnect.
    query = HEAD + "union join ta.s = nb.s and ta.w + 52 = nb.w\n" + SELECT
    rows = run_cell(write_models(tmp_path), query)
    keys = [(r[0], r[1]) for r in rows]
    assert len(keys) == len(set(keys)), f"fan-out: {rows}"
    assert rows == _expected_full("plain_derived"), f"got {rows}"


def test_mixed_anchor_composite_composes_to_full(tmp_path: Path):
    # q59's authored form: `subset join nb.s = ta.s and ta.w + 52 = nb.w` — pair 1
    # anchors ta, pair 2 anchors nb. Independent relations compose: preserve ta
    # rows AND preserve nb rows. The nb-only store-4 row survives; no fan-out.
    query = HEAD + "subset join nb.s = ta.s and ta.w + 52 = nb.w\n" + SELECT
    rows = run_cell(write_models(tmp_path), query)
    keys = [(r[0], r[1]) for r in rows]
    assert len(keys) == len(set(keys)), f"fan-out: {rows}"
    matched = [r for r in rows if r[1] is not None]
    assert matched == _expected_left(), f"anchored rows wrong: {rows}"
    extras = [r for r in rows if r[1] is None]
    assert all(r[2] is None for r in extras), f"unmatched ta measure bled: {rows}"
