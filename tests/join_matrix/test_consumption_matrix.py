"""Tier 4: consuming a joined relation, join form vs merge form.

The relation itself is the tier-1 rowset cell; these cells pin what happens
AROUND it: a post-join WHERE on the optional side's measure, and a WHERE that
narrows an outer relation to intersection via `is not null` (the documented
idiom replacing scoped INNER).
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import (
    LEFT_ROWS,
    RIGHT_ROWS,
    aggregate,
    expected_full,
    expected_left,
    run_cell,
    sort_rows,
    write_models,
)

HEAD = (
    "import left_fact as a;\nimport right_fact as b;\n"
    "rowset ra <- select a.l_key as k, sum(a.l_val) as tot;\n"
    "rowset rb <- select b.r_key as k, sum(b.r_val) as tot;\n"
)
SELECT = "select ra.k, ra.tot, rb.tot;"

RELATIONS = {
    ("full", "join"): "full join ra.k = rb.k\n",
    ("full", "merge"): "merge ra.k into rb.k;\n",
    ("left", "join"): "left join ra.k = rb.k\n",
    ("left", "merge"): "merge rb.k into ~ra.k;\n",
}


def _base(join_type: str) -> list[tuple]:
    left = aggregate(LEFT_ROWS)
    right = aggregate(RIGHT_ROWS)
    if join_type == "left":
        return expected_left(left, right)
    return expected_full(left, right)


def _query(join_type: str, form: str, where: str) -> str:
    # a merge is a standalone statement and precedes the query; a join clause
    # sits inside the query after its WHERE
    relation = RELATIONS[(join_type, form)]
    if form == "merge":
        return HEAD + relation + where + SELECT
    return HEAD + where + relation + SELECT


@pytest.mark.parametrize("form", ["join", "merge"])
@pytest.mark.parametrize("join_type", ["left", "full"])
def test_post_join_where_on_optional_measure(tmp_path: Path, join_type: str, form: str):
    # WHERE over the joined relation: NULL measures (unmatched rows) fail the
    # predicate and drop — this must filter the JOINED rows, not pre-filter one
    # side and change the join population.
    query = _query(join_type, form, "where rb.tot > 150\n")
    rows = run_cell(write_models(tmp_path), query)
    want = sort_rows([r for r in _base(join_type) if r[2] is not None and r[2] > 150])
    assert rows == want, f"{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"


@pytest.mark.parametrize("form", ["join", "merge"])
@pytest.mark.parametrize("join_type", ["left", "full"])
def test_intersection_via_not_null(tmp_path: Path, join_type: str, form: str):
    # the documented replacement for scoped INNER: outer join + `is not null`
    # on a non-key attribute of the optional side
    query = _query(join_type, form, "where rb.tot is not null\n")
    rows = run_cell(write_models(tmp_path), query)
    want = sort_rows([r for r in _base(join_type) if r[2] is not None])
    assert rows == want, f"{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"
