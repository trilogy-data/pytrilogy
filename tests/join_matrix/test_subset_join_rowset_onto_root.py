"""`subset join <rowset key> = <root key>` — rowset subset side onto a
datasource-bound anchor (the TPC-DS q35 full-population shape,
evals/tpcds_agent/bug_q35_subset_join_rowset_drops_filter.md).

The rowset member must keep its own physical identity: substituting it onto
the ROOT canonical (the pre-fix behavior) made every authored reference — a
null-test, a projection — read the anchor's full population, and let the
rowset body (with its WHERE restriction) prune out of the plan entirely: a
silent wrong result over the unrestricted anchor.

Semantics mirror the rowset<->rowset cells (test_subset_join_between_rowsets):
the declaration alone never restricts (domain knowledge, not row intent); row
restriction is the authored `is not null` / membership predicate.

Data: anchor l_key values {1,2,3}; rowset keeps r_key rows below 800 = {1,2},
an honest subset of the anchor (r_key 4 would make the declaration lie —
author error, planner-divergent by design).
"""

from pathlib import Path

from tests.join_matrix.harness import run_cell, write_models

BASE = """
import left_fact;
import right_fact;

with web_cust as
where r_val < 800
select r_key as cust_sk;
"""


def _rows(tmp_path: Path, query: str) -> list[tuple]:
    return run_cell(write_models(tmp_path), BASE + query)


def test_where_member_null_test_is_intersection(tmp_path: Path):
    query = (
        "select l_key "
        "subset join web_cust.cust_sk = l_key "
        "where web_cust.cust_sk is not null;"
    )
    assert _rows(tmp_path, query) == [(1,), (2,)]


def test_projecting_member_key_is_group_axis(tmp_path: Path):
    query = (
        "select l_key, web_cust.cust_sk as w_sk "
        "subset join web_cust.cust_sk = l_key;"
    )
    assert _rows(tmp_path, query) == [(1, 1), (2, 2), (3, 3)]


def test_no_member_reference_unchanged(tmp_path: Path):
    query = "select l_key subset join web_cust.cust_sk = l_key;"
    assert _rows(tmp_path, query) == [(1,), (2,), (3,)]


def test_aggregate_restricted_by_member_null_test(tmp_path: Path):
    query = (
        "where web_cust.cust_sk is not null "
        "select sum(l_val) as total "
        "subset join web_cust.cust_sk = l_key;"
    )
    assert _rows(tmp_path, query) == [(7,)]


def test_membership_idiom_parity(tmp_path: Path):
    query = "where l_key in web_cust.cust_sk select sum(l_val) as total;"
    assert _rows(tmp_path, query) == [(7,)]
