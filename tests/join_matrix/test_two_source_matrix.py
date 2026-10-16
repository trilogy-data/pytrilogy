"""Tier 1: two sources, single join key.

Sweeps key kind (root column / derived expression / rowset output) x join type
(subset / union / equal) x declaration form (query `join` / global `merge`) against the
python oracle. The merge and join forms of a cell share one expected result —
parity is asserted by construction.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import (
    aggregate,
    expected_equal,
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

# per relation: (oracle, {form: declaration template}). Every form of a
# relation shares one oracle, so merge/join parity is asserted by construction
# (docs/subset_union_join_design.md). `subset join R = L` declares R ⊆ L (the
# superset L anchors, row parity with a preserving LEFT); `union join L = R`
# declares neither contains the other (never narrows, keeps FULL rows);
# `equal join L = R` declares one domain, and with `narrow_equal_domain_joins`
# defaulted on it narrows to INNER, which on these deliberately
# declaration-violating rows DROPS the side-exclusive rows (lying declaration
# = author error). `into` is the legacy merge spelling, which has no union.
TYPES = {
    "subset": (
        expected_left,
        {
            "join": "subset join {r} = {l}",
            "merge": "merge subset {r} = {l};",
            "into": "merge {r} into ~{l};",
        },
    ),
    "union": (
        expected_full,
        {
            "join": "union join {l} = {r}",
            "merge": "merge union {l} = {r};",
        },
    ),
    "equal": (
        expected_equal,
        {
            "join": "equal join {l} = {r}",
            "merge": "merge equal {l} = {r};",
            "into": "merge {l} into {r};",
        },
    ),
}

CELLS = [
    (kind, join_type, form)
    for kind in KINDS
    for join_type, (_, forms) in TYPES.items()
    for form in forms
]


def _oracle(kind: str, join_type: str, nullable: bool = False) -> list[tuple]:
    key_fn = (lambda k: k + 1) if kind == "derived" else None
    left_rows, right_rows = matrix_rows(nullable)
    left = aggregate(left_rows, key_fn)
    right = aggregate(right_rows, key_fn)
    return TYPES[join_type][0](left, right)


def _query(kind: str, join_type: str, form: str) -> str:
    head, left_op, right_op, select = KINDS[kind]
    tpl = TYPES[join_type][1][form]
    return head + tpl.format(l=left_op, r=right_op) + "\n" + select


@pytest.mark.parametrize("kind,join_type,form", CELLS)
def test_two_source_single_key(tmp_path: Path, kind: str, join_type: str, form: str):
    query = _query(kind, join_type, form)
    rows = run_cell(write_models(tmp_path), query)
    want = _oracle(kind, join_type)
    assert rows == want, f"{kind}/{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"


# Nullable axis: the same cells with the key columns DECLARED nullable (`?`)
# and a NULL-key row on each side. NULL keys are valid members: they must match
# null-safely (one (None, 16, 1600) row, never two half-rows or a drop), and an
# authored LEFT must stay LEFT (no nullable-driven widening to FULL).
# The former derived-LEFT zip xfails dissolved with the always-preserving flip:
# the scan-level nullability stamp for computed BASIC keys keeps the zip
# null-safe, so narrowing to INNER stays row-identical.
@pytest.mark.parametrize("kind,join_type,form", CELLS)
def test_two_source_single_key_nullable(
    tmp_path: Path, kind: str, join_type: str, form: str
):
    query = _query(kind, join_type, form)
    rows = run_cell(write_models(tmp_path, nullable=True), query)
    want = _oracle(kind, join_type, nullable=True)
    assert rows == want, f"{kind}/{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"
