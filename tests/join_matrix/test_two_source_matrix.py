"""Tier 1: two sources, single join key.

Sweeps key kind (root column / derived expression / rowset output) x join type
(FULL / LEFT) x declaration form (query `join` / global `merge`) against the
python oracle. The merge and join forms of a cell share one expected result —
parity is asserted by construction.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import (
    aggregate,
    expected_full,
    expected_left,
    matrix_rows,
    run_cell,
    write_models,
)

HEAD = "import left_fact as a;\nimport right_fact as b;\n"

ROWSET_HEAD = HEAD + (
    "rowset ra <- select a.l_key as k, sum(a.l_val) as tot;\n"
    "rowset rb <- select b.r_key as k, sum(b.r_val) as tot;\n"
)
DERIVED_HEAD = HEAD + "auto ka <- a.l_key + 1;\nauto kb <- b.r_key + 1;\n"

# per key kind: (head, left operand, right operand, select clause)
KINDS = {
    "rowset": (
        ROWSET_HEAD,
        "ra.k",
        "rb.k",
        "select ra.k, ra.tot, rb.tot;",
    ),
    "derived": (
        DERIVED_HEAD,
        "ka",
        "kb",
        "select ka, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "root": (
        HEAD,
        "a.l_key",
        "b.r_key",
        "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
}

# per join type: (join clause template, merge statement template, oracle)
# `left join L = R` anchors L; its merge equivalent is `merge R into ~L`.
TYPES = {
    "full": ("full join {l} = {r}", "merge {l} into {r};", expected_full),
    "left": ("left join {l} = {r}", "merge {r} into ~{l};", expected_left),
}


def _oracle(kind: str, join_type: str, nullable: bool = False) -> list[tuple]:
    key_fn = (lambda k: k + 1) if kind == "derived" else None
    left_rows, right_rows = matrix_rows(nullable)
    left = aggregate(left_rows, key_fn)
    right = aggregate(right_rows, key_fn)
    return TYPES[join_type][2](left, right)


def _query(kind: str, join_type: str, form: str) -> str:
    head, left_op, right_op, select = KINDS[kind]
    join_tpl, merge_tpl, _ = TYPES[join_type]
    tpl = join_tpl if form == "join" else merge_tpl
    return head + tpl.format(l=left_op, r=right_op) + "\n" + select


@pytest.mark.parametrize("form", ["join", "merge"])
@pytest.mark.parametrize("join_type", list(TYPES))
@pytest.mark.parametrize("kind", list(KINDS))
def test_two_source_single_key(tmp_path: Path, kind: str, join_type: str, form: str):
    query = _query(kind, join_type, form)
    rows = run_cell(write_models(tmp_path), query)
    want = _oracle(kind, join_type)
    assert rows == want, f"{kind}/{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"


# Nullable axis: the same cells with the key columns DECLARED nullable (`?`)
# and a NULL-key row on each side. NULL keys are valid members: they must match
# null-safely (one (None, 16, 1600) row, never two half-rows or a drop), and an
# authored LEFT must stay LEFT (no nullable-driven widening to FULL).
_XFAIL_ROWSET_NULL = pytest.mark.xfail(
    strict=True,
    reason="nullability is not propagated from a `?` column through ROWSET "
    "lineage: get_modifiers sees neither side as nullable, renders plain `=`, "
    "and each side's NULL key group survives as a separate half-row instead of "
    "matching null-safely (root-keyed cells correctly render "
    "IS NOT DISTINCT FROM)",
)
_XFAIL_DERIVED_NULL = pytest.mark.xfail(
    strict=True,
    reason="a NULL derived join key (`k + 1` over a `?` nullable column) drops "
    "the row entirely — including the LEFT anchor's own NULL-key row (row "
    "LOSS, worse than the rowset half-row split)",
)
_NULLABLE_MARKS = {"rowset": (_XFAIL_ROWSET_NULL,), "derived": (_XFAIL_DERIVED_NULL,)}


@pytest.mark.parametrize(
    "kind,join_type,form",
    [
        pytest.param(k, t, f, marks=_NULLABLE_MARKS.get(k, ()))
        for k in KINDS
        for t in TYPES
        for f in ("join", "merge")
    ],
)
def test_two_source_single_key_nullable(
    tmp_path: Path, kind: str, join_type: str, form: str
):
    query = _query(kind, join_type, form)
    rows = run_cell(write_models(tmp_path, nullable=True), query)
    want = _oracle(kind, join_type, nullable=True)
    assert rows == want, f"{kind}/{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"
