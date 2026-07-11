"""`subset join` BETWEEN two rowsets must wire the subset member's source
(q35 family).

`subset join a = b` between rowsets declares a ⊆ b with `b` the preserving
anchor. Both sides keep their own physical identity (a rowset output has no
datasource binding), so referencing the subset member's key — in the WHERE or
the SELECT — requires the member's own CTE joined in. The planner used to
satisfy the member through the anchor's group pseudonym, dropping the member's
source entirely and stranding every reference as an INVALID_REFERENCE_BUG
render sentinel (evals/tpcds_agent/bug_q35_subset_join_rowset_render_sentinel.md).

Data (harness): store_cust keys {1,2,3}, web_cust keys {1,2,4}.
"""

from pathlib import Path

from tests.join_matrix.harness import make_engine, sort_rows, write_models

BASE = """
import left_fact;
import right_fact;

with store_cust as
where l_val > 0
select l_key as cust_sk;

with web_cust as
where r_val > 0
select r_key as cust_sk;
"""


def _rows(tmp_path: Path, query: str) -> list[tuple]:
    write_models(tmp_path)
    engine = make_engine(tmp_path)
    statements = engine.parse_text(BASE + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


def test_where_member_null_test_is_intersection(tmp_path: Path):
    query = (
        "select store_cust.cust_sk as c_sk "
        "subset join web_cust.cust_sk = store_cust.cust_sk "
        "where web_cust.cust_sk is not null;"
    )
    # anchor {1,2,3} narrowed to keys present on the subset side {1,2,4}
    assert _rows(tmp_path, query) == [(1,), (2,)]


def test_projecting_member_key_is_group_axis(tmp_path: Path):
    query = (
        "select store_cust.cust_sk as c_sk, web_cust.cust_sk as w_sk "
        "subset join web_cust.cust_sk = store_cust.cust_sk;"
    )
    # projecting a join-key-group member yields the coalesced group axis over
    # the anchor's preserved rows (matching union-join member projection);
    # per-side absence is the `is not null` presence-probe idiom, not the key
    assert _rows(tmp_path, query) == [(1, 1), (2, 2), (3, 3)]


def test_no_member_reference_unchanged(tmp_path: Path):
    query = (
        "select store_cust.cust_sk as c_sk "
        "subset join web_cust.cust_sk = store_cust.cust_sk;"
    )
    assert _rows(tmp_path, query) == [(1,), (2,), (3,)]


def test_wrapped_in_rowset_where_member(tmp_path: Path):
    # the original q35 shape: the subset join lives inside another rowset body
    query = (
        "with target_cust as "
        "select store_cust.cust_sk as c_sk "
        "subset join web_cust.cust_sk = store_cust.cust_sk "
        "where web_cust.cust_sk is not null; "
        "select target_cust.c_sk;"
    )
    assert _rows(tmp_path, query) == [(1,), (2,)]


def test_two_subset_joins_or_of_members(tmp_path: Path):
    # q35's full shape: three rowsets, two subset joins on the same anchor key,
    # OR of the two members' null tests. cat_cust keys = {2,3} (l_val > 2).
    query = (
        "with cat_cust as where l_val > 2 select l_key as k_sk; "
        "select store_cust.cust_sk as c_sk "
        "subset join web_cust.cust_sk = store_cust.cust_sk "
        "subset join cat_cust.k_sk = store_cust.cust_sk "
        "where web_cust.cust_sk is not null or cat_cust.k_sk is not null;"
    )
    # web contributes {1,2}, cat contributes {2,3} -> {1,2,3}
    assert _rows(tmp_path, query) == [(1,), (2,), (3,)]
