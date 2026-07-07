"""Subset relations anchored on a FILTERED rowset key (q54 family).

A rowset body's WHERE *defines* the rowset output's domain rather than
restricting it, so `subset join a = rs.k` must still narrow to the directional
superset-anchored LEFT: non-members of the declared superset are excluded and
unmatched anchor rows survive. Before the fix the anchor side could never
prove value-completeness (its chain looked "filtered"), the preserving FULL
survived narrowing, and non-member rows silently leaked — the q54 sink.

Adversarial pair: `union join` onto the same filtered rowset declares ∦ and
must keep every row of both sides.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import (
    RIGHT_ROWS,
    aggregate,
    matrix_rows,
    run_cell,
    sort_rows,
    write_models,
)

# Anchor domain: right keys whose row survives the body filter (drops key 4's
# 800 row only when the bound tightens; at < 900 keys {1, 2, 4} all survive
# and left-exclusive key 3 is the non-member that must not leak).
HEAD = "import left_fact as a;\nimport right_fact as b;\n"
ROWSET = "rowset members <- select b.r_key as k where b.r_val < 900;\n"
SELECT = "select a.l_key, sum(a.l_val) as lv;"

FORMS = {
    "subset_join": "subset join a.l_key = members.k",
    "left_join": "left join members.k = a.l_key",
    "merge": "merge a.l_key into ~members.k;",
}


def _anchor_keys() -> set[int]:
    return {k for _, k, v in RIGHT_ROWS if v < 900}


def _expected_subset() -> list[tuple]:
    left = aggregate(matrix_rows(False)[0])
    return sort_rows([(k, left.get(k)) for k in _anchor_keys()])


def _expected_union() -> list[tuple]:
    left = aggregate(matrix_rows(False)[0])
    return sort_rows([(k, left.get(k)) for k in _anchor_keys() | set(left)])


@pytest.mark.parametrize("form", list(FORMS))
def test_subset_onto_filtered_rowset_anchor_is_directional(tmp_path: Path, form: str):
    relation = FORMS[form]
    query = HEAD + ROWSET + relation + "\n" + SELECT
    rows = run_cell(write_models(tmp_path), query)
    want = _expected_subset()
    assert rows == want, f"{form}:\n{query}\ngot {rows}\nwant {want}"


def test_union_onto_filtered_rowset_anchor_keeps_full(tmp_path: Path):
    query = HEAD + ROWSET + "union join a.l_key = members.k\n" + SELECT
    rows = run_cell(write_models(tmp_path), query)
    want = _expected_union()
    assert rows == want, f"{query}\ngot {rows}\nwant {want}"
