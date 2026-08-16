"""Tier 3: three-way relations on a derived key, join form vs merge form.

The join-clause forms (pairwise + chained `a = b = c`) are pinned in
tests/test_join_merge_parity.py; this tier pins that a STACK OF MERGES relating
the same three keys resolves to the same rows as the join clauses, against the
python oracle.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import (
    LEFT_ROWS,
    RIGHT_ROWS,
    aggregate,
    run_cell,
    sort_rows,
    write_models,
)

MID_ROWS: list[tuple[int, int | None, int]] = [(1, 1, 7), (2, 2, 77), (3, 5, 777)]

MID_MODEL = (
    "key m_id int;\n"
    "property m_id.m_key int;\n"
    "property m_id.m_val int;\n"
    "datasource msrc (i: m_id, k: m_key, v: m_val) grain (m_id)\n"
    "query '''"
    + " union all ".join(f"select {i} i, {k} k, {v} v" for i, k, v in MID_ROWS)
    + "''';\n"
)

HEAD = (
    "import left_fact as a;\nimport right_fact as b;\nimport mid_fact as c;\n"
    "auto ka <- a.l_key + 1;\n"
    "auto kb <- b.r_key + 1;\n"
    "auto kc <- c.m_key + 1;\n"
)
SELECT = "select ka, sum(a.l_val) as lv, sum(b.r_val) as rv, sum(c.m_val) as cv;"

RELATIONS = {
    # `subset join kb = ka; subset join kc = ka` anchors ka; the merge form marks
    # each optional side partial against the same anchor.
    ("subset", "join"): "subset join kb = ka\nsubset join kc = ka",
    ("subset", "merge"): "merge kb into ~ka;\nmerge kc into ~ka;",
    ("union", "join"): "union join ka = kb = kc",
    ("union", "merge"): "merge ka into kb;\nmerge kb into kc;",
}


def _write(tmp_path: Path) -> Path:
    write_models(tmp_path)
    (tmp_path / "mid_fact.preql").write_text(MID_MODEL)
    return tmp_path


def _oracle(join_type: str, form: str) -> list[tuple]:
    shift = lambda k: k + 1
    a = aggregate(LEFT_ROWS, shift)
    b = aggregate(RIGHT_ROWS, shift)
    c = aggregate(MID_ROWS, shift)
    if join_type == "subset":
        keys = set(a)
    elif form == "merge":
        # the merge stack is a chain of EQUAL declarations, narrowed to the
        # three-way intersection by default (lying declaration = author
        # error); the query-scoped `union join` chain keeps the union.
        keys = set(a) & set(b) & set(c)
    else:
        keys = set(a) | set(b) | set(c)
    return sort_rows([(k, a.get(k), b.get(k), c.get(k)) for k in keys])


@pytest.mark.parametrize("form", ["join", "merge"])
@pytest.mark.parametrize("join_type", ["subset", "union"])
def test_three_way_derived_key(tmp_path: Path, join_type: str, form: str):
    relation = RELATIONS[(join_type, form)]
    query = HEAD + relation + "\n" + SELECT
    rows = run_cell(_write(tmp_path), query)
    want = _oracle(join_type, form)
    assert rows == want, f"{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"


# Three-way chain over ROWSET sides — the q83 shape, pinning two defects the
# derived-key cells above cannot reach (there the merge CTE never has to read
# the merged key from all three parents):
#
# - HAVING cells: pseudonyms for a chained group were registered as a STAR
#   around the union-find canonical, so two non-canonical members related only
#   THROUGH the canonical — absent from the side being resolved, leaving the key
#   unreachable and rendered as an INVALID_ALIAS sentinel / a wrong column name.
# - no-HAVING cells: rule-B ∦ narrowing read a sibling out of the sup side's
#   pseudonym closure and "proved" the sub side a subset of its own alias,
#   narrowing FULL to RIGHT OUTER and silently dropping side-exclusive rows.
ROWSET_HEAD = (
    "import left_fact as a;\nimport right_fact as b;\nimport mid_fact as c;\n"
    "rowset ra <- select a.l_key as k, sum(a.l_val) as v, count(a.l_id) as n;\n"
    "rowset rb <- select b.r_key as k, sum(b.r_val) as v, count(b.r_id) as n;\n"
    "rowset rc <- select c.m_key as k, sum(c.m_val) as v, count(c.m_id) as n;\n"
)
ROWSET_CHAIN = "union join ra.k = rb.k = rc.k\n"

ROWSET_CELLS = {
    "measures_only": "select ra.k as k, ra.v as av, rb.v as bv, rc.v as cv,\n",
    "counts_only": "select ra.k as k, ra.n as an, rb.n as bn, rc.n as cn,\n",
    "both": (
        "select ra.k as k, ra.v as av, rb.v as bv, rc.v as cv,"
        " ra.n as an, rb.n as bn, rc.n as cn,\n"
    ),
}


def _counts(rows: list[tuple[int, int | None, int]]) -> dict:
    out: dict = {}
    for _, k, _v in rows:
        out[k] = out.get(k, 0) + 1
    return out


def _rowset_oracle(cell: str, having: bool) -> list[tuple]:
    vals = [aggregate(r) for r in (LEFT_ROWS, RIGHT_ROWS, MID_ROWS)]
    cnts = [_counts(r) for r in (LEFT_ROWS, RIGHT_ROWS, MID_ROWS)]
    keys = set(vals[0]) & set(vals[1]) & set(vals[2]) if having else set().union(*vals)
    picked = {"measures_only": vals, "counts_only": cnts, "both": vals + cnts}[cell]
    return sort_rows([(k,) + tuple(m.get(k) for m in picked) for k in keys])


@pytest.mark.parametrize("having", [False, True])
@pytest.mark.parametrize("cell", sorted(ROWSET_CELLS))
def test_three_way_chain_over_rowsets(tmp_path: Path, cell: str, having: bool):
    query = ROWSET_HEAD + ROWSET_CELLS[cell] + ROWSET_CHAIN
    query += "having ra.n > 0 and rb.n > 0 and rc.n > 0;" if having else ";"
    rows = run_cell(_write(tmp_path), query)
    want = _rowset_oracle(cell, having)
    assert rows == want, f"{cell}/having={having}:\n{query}\ngot {rows}\nwant {want}"
