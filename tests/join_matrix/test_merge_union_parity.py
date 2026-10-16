"""`merge union a = b` is the environment-scoped `union join a = b`.

Every shape below runs twice, once with the query-scoped join clause and once
with the global merge statement, and the two must return identical rows. The
join forms are oracle-checked across the rest of the matrix; the shapes with a
cheap python oracle assert it here too, so "both wrong" cannot pass as parity.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import (
    aggregate,
    expected_full,
    matrix_rows,
    run_cell,
    sort_rows,
    write_models,
)

HEAD = "import left_fact as a;\nimport right_fact as b;\n"
DERIVED_HEAD = HEAD + "auto ka <- a.l_key + 1;\nauto kb <- b.r_key + 1;\n"
ROWSET_HEAD = HEAD + (
    "rowset ra <- select a.l_key as k, sum(a.l_val) as tot, count(a.l_id) as n;\n"
    "rowset rb <- select b.r_key as k, sum(b.r_val) as tot, count(b.r_id) as n;\n"
)
FILTERED_ROWSET_HEAD = HEAD + (
    "rowset ra <- where a.l_val < 8 select a.l_key as k, sum(a.l_val) as tot;\n"
    "rowset rb <- where b.r_val > 100 select b.r_key as k, sum(b.r_val) as tot;\n"
)

Keys = tuple[str, list[tuple[str, str]]]

ROOT: Keys = (HEAD, [("a.l_key", "b.r_key")])
DERIVED: Keys = (DERIVED_HEAD, [("ka", "kb")])
ROWSET: Keys = (ROWSET_HEAD, [("ra.k", "rb.k")])
# ids pair everywhere but keys only on ids 1 and 3: `l_id -> l_key` holds on
# each side, yet nothing makes the two sides' keys agree
COMPOSITE: Keys = (HEAD, [("a.l_id", "b.r_id"), ("a.l_key", "b.r_key")])

# name: ((head, key pairs), WHERE prefix, select statement)
SHAPES: dict[str, tuple[Keys, str, str]] = {
    "root_sums": (ROOT, "", "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv;"),
    "root_counts": (
        ROOT,
        "",
        "select a.l_key, count(a.l_id) as ln, count(b.r_id) as rn;",
    ),
    "root_other_endpoint": (
        ROOT,
        "",
        "select b.r_key, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "root_rows": (ROOT, "", "select a.l_key, a.l_id, b.r_id;"),
    "root_where_side_attr": (
        ROOT,
        "where b.r_val > 150\n",
        "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "root_where_key": (
        ROOT,
        "where a.l_key > 1\n",
        "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "root_where_both_sides": (
        ROOT,
        "where a.l_val < 8 and b.r_val < 800\n",
        "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "root_intersection_idiom": (
        ROOT,
        "where a.l_id is not null and b.r_id is not null\n",
        "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "root_membership": (
        ROOT,
        "where a.l_key in (1, 4)\n",
        "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "root_having": (
        ROOT,
        "",
        "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv having rv > 150;",
    ),
    "root_filtered_aggregate": (
        ROOT,
        "",
        "select a.l_key, sum(b.r_val ? b.r_val > 150) as big;",
    ),
    "root_global_aggregate": (
        ROOT,
        "",
        "select count(a.l_id) as ln, count(b.r_id) as rn, count(a.l_key) as kn;",
    ),
    "root_order_limit": (
        ROOT,
        "",
        "select a.l_key, sum(b.r_val) as rv order by rv desc nulls last limit 2;",
    ),
    "derived_sums": (
        DERIVED,
        "",
        "select ka, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "derived_where_side_attr": (
        DERIVED,
        "where a.l_val > 1\n",
        "select ka, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
    "rowset_measures": (ROWSET, "", "select ra.k, ra.tot, rb.tot;"),
    "rowset_counts": (ROWSET, "", "select ra.k, ra.n, rb.n;"),
    "rowset_where_optional_side": (
        ROWSET,
        "where rb.tot > 150\n",
        "select ra.k, ra.tot, rb.tot;",
    ),
    "rowset_having": (ROWSET, "", "select ra.k, ra.tot, rb.tot having ra.n > 0;"),
    "rowset_filtered_bodies": (
        (FILTERED_ROWSET_HEAD, [("ra.k", "rb.k")]),
        "",
        "select ra.k, ra.tot, rb.tot;",
    ),
    "root_rows_where_side": (
        ROOT,
        "where a.l_val > 1\n",
        "select a.l_key, a.l_id, b.r_id;",
    ),
    "root_rows_where_key": (
        ROOT,
        "where b.r_key > 1\n",
        "select a.l_key, a.l_id, b.r_id;",
    ),
    "derived_rows_where_side": (
        DERIVED,
        "where b.r_val < 800\n",
        "select ka, a.l_id, b.r_id;",
    ),
    "rowset_rows_where_side": (
        ROWSET,
        "where ra.tot > 3\n",
        "select ra.k, rb.tot;",
    ),
    "composite_rows": (
        COMPOSITE,
        "",
        "select a.l_id, a.l_key, b.r_key, a.l_val, b.r_val;",
    ),
    "composite_sums": (
        COMPOSITE,
        "",
        "select a.l_id, sum(a.l_val) as lv, sum(b.r_val) as rv;",
    ),
}


def _join_form(shape: str) -> str:
    (head, pairs), where, select = SHAPES[shape]
    keys = " and ".join(f"{left} = {right}" for left, right in pairs)
    return f"{head}{where}union join {keys}\n{select}"


def _merge_form(shape: str) -> str:
    (head, pairs), where, select = SHAPES[shape]
    merges = "".join(f"merge union {left} = {right};\n" for left, right in pairs)
    return f"{head}{merges}{where}{select}"


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_merge_union_matches_union_join(tmp_path: Path, shape: str, nullable: bool):
    workdir = write_models(tmp_path, nullable=nullable)
    joined = run_cell(workdir, _join_form(shape))
    merged = run_cell(workdir, _merge_form(shape))
    assert (
        merged == joined
    ), f"{shape}:\n{_merge_form(shape)}\ngot {merged}\nwant {joined}"


def _sides(nullable: bool) -> tuple[dict, dict]:
    left_rows, right_rows = matrix_rows(nullable)
    return aggregate(left_rows), aggregate(right_rows)


def _counts(rows: list[tuple]) -> dict:
    out: dict = {}
    for _, k, _v in rows:
        out[k] = out.get(k, 0) + 1
    return out


@pytest.mark.parametrize("nullable", [False, True])
def test_merge_union_sums_oracle(tmp_path: Path, nullable: bool):
    rows = run_cell(write_models(tmp_path, nullable), _merge_form("root_sums"))
    assert rows == expected_full(*_sides(nullable))


@pytest.mark.parametrize("nullable", [False, True])
def test_merge_union_counts_oracle(tmp_path: Path, nullable: bool):
    left_rows, right_rows = matrix_rows(nullable)
    left, right = _counts(left_rows), _counts(right_rows)
    rows = run_cell(write_models(tmp_path, nullable), _merge_form("rowset_counts"))
    assert rows == sort_rows(
        [(k, left.get(k), right.get(k)) for k in set(left) | set(right)]
    )


def test_merge_union_having_oracle(tmp_path: Path):
    rows = run_cell(write_models(tmp_path), _merge_form("root_having"))
    assert rows == [r for r in expected_full(*_sides(False)) if (r[2] or 0) > 150]


def test_merge_union_global_aggregate_oracle(tmp_path: Path):
    left_rows, right_rows = matrix_rows(False)
    keys = {k for _, k, _ in left_rows} | {k for _, k, _ in right_rows}
    rows = run_cell(write_models(tmp_path), _merge_form("root_global_aggregate"))
    assert rows == [(len(left_rows), len(right_rows), len(keys))]


@pytest.mark.parametrize("form", ["join", "merge"])
@pytest.mark.parametrize("nullable", [False, True])
def test_having_on_unprojected_side_keeps_exclusive_rows(
    tmp_path: Path, nullable: bool, form: str
):
    query = (_join_form if form == "join" else _merge_form)("rowset_having")
    left_rows, _ = matrix_rows(nullable)
    left_keys = {k for _, k, _ in left_rows}
    rows = run_cell(write_models(tmp_path, nullable), query)
    assert rows == [r for r in expected_full(*_sides(nullable)) if r[0] in left_keys]
