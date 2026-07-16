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

# per join type: (join clause template, merge statement template,
# join-form oracle, merge-form oracle).
# SUBSET/UNION are the two scoped-relation declarations
# (docs/subset_union_join_design.md): `subset join R = L` declares R ⊆ L (the
# superset L anchors, row parity with a preserving LEFT), with merge equivalent
# `merge R into ~L`; `union join L = R` declares neither contains the other
# (never narrows, keeps FULL rows), with merge equivalent `merge L into R`. The
# non-partial merge is the EQUAL declaration: with `narrow_equal_domain_joins`
# defaulted on it narrows to INNER, and on these deliberately declaration-
# violating rows that DROPS the side-exclusive rows (lying declaration =
# author error) — so the merge-form union cell rules intersection while
# the query-scoped join forms keep the preserving FULL rows.
TYPES = {
    "subset": (
        "subset join {r} = {l}",
        "merge {r} into ~{l};",
        expected_left,
        expected_left,
    ),
    "union": (
        "union join {l} = {r}",
        "merge {l} into {r};",
        expected_full,
        expected_equal,
    ),
}


def _oracle(
    kind: str, join_type: str, form: str, nullable: bool = False
) -> list[tuple]:
    key_fn = (lambda k: k + 1) if kind == "derived" else None
    left_rows, right_rows = matrix_rows(nullable)
    left = aggregate(left_rows, key_fn)
    right = aggregate(right_rows, key_fn)
    oracle = TYPES[join_type][2] if form == "join" else TYPES[join_type][3]
    return oracle(left, right)


def _query(kind: str, join_type: str, form: str) -> str:
    head, left_op, right_op, select = KINDS[kind]
    join_tpl, merge_tpl = TYPES[join_type][0], TYPES[join_type][1]
    tpl = join_tpl if form == "join" else merge_tpl
    return head + tpl.format(l=left_op, r=right_op) + "\n" + select


@pytest.mark.parametrize("form", ["join", "merge"])
@pytest.mark.parametrize("join_type", list(TYPES))
@pytest.mark.parametrize("kind", list(KINDS))
def test_two_source_single_key(tmp_path: Path, kind: str, join_type: str, form: str):
    query = _query(kind, join_type, form)
    rows = run_cell(write_models(tmp_path), query)
    want = _oracle(kind, join_type, form)
    assert rows == want, f"{kind}/{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"


# Nullable axis: the same cells with the key columns DECLARED nullable (`?`)
# and a NULL-key row on each side. NULL keys are valid members: they must match
# null-safely (one (None, 16, 1600) row, never two half-rows or a drop), and an
# authored LEFT must stay LEFT (no nullable-driven widening to FULL).
# The former derived-LEFT zip xfails dissolved with the always-preserving flip:
# the scan-level nullability stamp for computed BASIC keys keeps the zip
# null-safe, so narrowing to INNER stays row-identical.
@pytest.mark.parametrize(
    "kind,join_type,form",
    [pytest.param(k, t, f) for k in KINDS for t in TYPES for f in ("join", "merge")],
)
def test_two_source_single_key_nullable(
    tmp_path: Path, kind: str, join_type: str, form: str
):
    query = _query(kind, join_type, form)
    rows = run_cell(write_models(tmp_path, nullable=True), query)
    want = _oracle(kind, join_type, form, nullable=True)
    assert rows == want, f"{kind}/{join_type}/{form}:\n{query}\ngot {rows}\nwant {want}"
