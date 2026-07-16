"""Cells distilled from the v4 parity ports (run under both planners via
conftest): row-level reads across a `merge into` pair, the bare coalescing-axis
projection, and a presence-probe filter that pulls only the subset side's
measure. Row expectations follow the harness oracle convention (computed from
LEFT_ROWS/RIGHT_ROWS by hand where the shape is a join the oracle helpers
don't model)."""

from pathlib import Path

from tests.join_matrix.harness import run_cell, write_models

IMPORTS = "import left_fact;\nimport right_fact;\n"


def test_merged_key_row_level_read(tmp_path: Path):
    # merge declares r_key ⊆ l_key (partial): row-level read of both measures
    # LEFT-joins the fact pair on the merged key, preserving left-only key 3.
    write_models(tmp_path)
    rows = run_cell(
        tmp_path,
        IMPORTS + "merge r_key into ~l_key;\nselect l_key, l_val, r_val;",
    )
    assert rows == [(1, 1, 100), (1, 2, 100), (2, 4, 200), (2, 4, 400), (3, 8, None)]


def test_bare_coalescing_axis_projection(tmp_path: Path):
    # A union-join axis is the union of member domains; no single scan may
    # satisfy the bare projection (left {1,2,3} ∪ right {1,2,4}).
    write_models(tmp_path)
    rows = run_cell(tmp_path, IMPORTS + "select l_key union join r_key = l_key;")
    assert rows == [(1,), (2,), (3,), (4,)]


def test_probe_filter_with_subset_measure_only(tmp_path: Path):
    # The member key is neither selected nor named outside the probe: the
    # subset boundary must still materialize it (hidden) so the probe computes
    # pre-merge and the anchor narrows to the intersection {1,2}.
    write_models(tmp_path)
    rows = run_cell(
        tmp_path,
        IMPORTS + "with store_cust as where l_val > 0 select l_key as cust_sk;\n"
        "with web_cust as where r_val > 0 select r_key as cust_sk, sum(r_val) as tot;\n"
        "select store_cust.cust_sk as c_sk, web_cust.tot as w_tot "
        "subset join web_cust.cust_sk = store_cust.cust_sk "
        "where web_cust.cust_sk is not null;",
    )
    assert rows == [(1, 100), (2, 600)]
