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
    # `left join ka = kb; left join ka = kc` anchors ka; the merge form marks
    # each optional side partial against the same anchor.
    ("left", "join"): "left join ka = kb\nleft join ka = kc",
    ("left", "merge"): "merge kb into ~ka;\nmerge kc into ~ka;",
    ("full", "join"): "full join ka = kb = kc",
    ("full", "merge"): "merge ka into kb;\nmerge kb into kc;",
}


def _write(tmp_path: Path) -> Path:
    write_models(tmp_path)
    (tmp_path / "mid_fact.preql").write_text(MID_MODEL)
    return tmp_path


def _oracle(join_type: str) -> list[tuple]:
    shift = lambda k: k + 1  # noqa: E731
    a = aggregate(LEFT_ROWS, shift)
    b = aggregate(RIGHT_ROWS, shift)
    c = aggregate(MID_ROWS, shift)
    keys = set(a) if join_type == "left" else set(a) | set(b) | set(c)
    return sort_rows([(k, a.get(k), b.get(k), c.get(k)) for k in keys])


@pytest.mark.parametrize("form", ["join", "merge"])
@pytest.mark.parametrize("join_type", ["left", "full"])
def test_three_way_derived_key(tmp_path: Path, join_type: str, form: str):
    relation = RELATIONS[(join_type, form)]
    query = HEAD + relation + "\n" + SELECT
    rows = run_cell(_write(tmp_path), query)
    want = _oracle(join_type)
    assert rows == want, f"{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"
