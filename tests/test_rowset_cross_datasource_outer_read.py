"""A rowset whose body INTERNALLY joins two datasources (via a query-scoped join)
must apply that join when built as a sub-node. The rowset's own joins live on its
`SelectLineage.scoped_joins`; `gen_rowset_node` has to feed them to the inner build
(exactly like the union-arm path does) — otherwise the body builds with no join, the
two datasources come back as separate connected components, and the read-back raises
a (misleading) DisconnectedConceptsException naming a "missing join/merge" that is in
fact present inside the rowset.

Surfaced on enriched TPC-DS q64: the agent pre-aggregated catalog_sales +
catalog_returns in a rowset (joined on item.id + order_number), then read it back and
hit this — it could not diagnose the "missing join/merge" message and burned ~1.6M
tokens.

The rowset key alias also has to keep two identities: its authored source key for
outer discovery/enrichment, and the rowset body's scoped-join canonical for
rendering the materialized key column.
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

A = """key aid int;
property aid.av float;
property aid.aw float;
datasource a (i: aid, v: av, w: aw) grain (aid) address a_tbl;
"""

B = """key bid int;
property bid.bv float;
datasource b (i: bid, v: bv) grain (bid) address b_tbl;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "a.preql").write_text(A)
    (tmp_path / "b.preql").write_text(B)
    return tmp_path


def _gen(models: Path, body: str) -> str:
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    return eng.generate_sql(body)[-1]


def test_single_datasource_rowset_outer_read(models: Path):
    # Baseline: a rowset over ONE datasource reads back cleanly.
    sql = _gen(
        models,
        """
import a as a;
with rs as select a.aid as k, sum(a.av) as sa, sum(a.av) as sa2;
select rs.k, rs.sa, rs.sa2 limit 10;
""",
    )
    assert sql and "INVALID_REFERENCE_BUG" not in sql


def test_cross_datasource_rowset_body_inline(models: Path):
    # Control: the SAME cross-datasource scoped join resolves as an inline select.
    sql = _gen(
        models,
        """
import a as a;
import b as b;
where b.bv is not null
select a.aid as k, sum(a.av) as sa, sum(b.bv) as sb
subset join b.bid = a.aid
limit 10;
""",
    )
    assert sql and "INVALID_REFERENCE_BUG" not in sql


def test_cross_datasource_rowset_join_propagation(models: Path):
    # The fix: the rowset's own internal join must be applied when it is built as a
    # sub-node, so its two datasources are connected and the read-back resolves.
    # Read back the aggregates (one per datasource) + a non-key property — i.e.
    # everything except the collapsed join key, all of which materialize cleanly.
    sql = _gen(
        models,
        """
import a as a;
import b as b;
with rs as
where b.bv is not null
select a.aid as k, a.aw as extra, sum(a.av) as sa, sum(b.bv) as sb
subset join b.bid = a.aid;
select rs.extra, rs.sa, rs.sb limit 10;
""",
    )
    assert sql and "INVALID_REFERENCE_BUG" not in sql
    # both datasources are present in the body -> the join was applied
    assert "a_tbl" in sql and "b_tbl" in sql


def test_cross_datasource_rowset_join_resolves_correctly(models: Path):
    # End-to-end: the rowset's internal join must actually filter to matching keys
    # and aggregate from BOTH datasources. aid 1,2 match a bid; aid 3 has no b row
    # (dropped by `where b.bv is not null` over the subset join); bid 4 has no a row
    # (dropped). The non-key property `aw` anchors each output row so we can assert
    # per-row correctness without reading back the join key.
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    eng.execute_raw_sql("create table a_tbl (i int, v double, w double)")
    eng.execute_raw_sql("insert into a_tbl values (1,10,1000),(2,20,2000),(3,30,3000)")
    eng.execute_raw_sql("create table b_tbl (i int, v double)")
    eng.execute_raw_sql("insert into b_tbl values (1,100),(2,200),(4,400)")
    rows = eng.execute_query("""
import a as a;
import b as b;
with rs as
where b.bv is not null
select a.aid as k, a.aw as extra, sum(a.av) as sa, sum(b.bv) as sb
subset join b.bid = a.aid;
select rs.extra, rs.sa, rs.sb order by rs.extra;
""").fetchall()
    assert [tuple(r) for r in rows] == [(1000.0, 10.0, 100.0), (2000.0, 20.0, 200.0)]


def test_cross_datasource_rowset_outer_read_key(models: Path):
    sql = _gen(
        models,
        """
import a as a;
import b as b;
with rs as
where b.bv is not null
select a.aid as k, sum(a.av) as sa, sum(b.bv) as sb
subset join b.bid = a.aid;

select rs.k, rs.sa, rs.sb limit 10;
""",
    )
    assert sql and "INVALID_REFERENCE_BUG" not in sql


def test_rowset_key_read_back_aligns_with_source(models: Path):
    # `rs`'s key is the INTERSECTION a∩b = {1,2} (a has {1,2,3}, b has {1,2,4});
    # b contributes only its key, so the intersection is expressed as
    # `where a.aid in b.bid`. Joins are row-preserving and the rowset side is
    # filtered (the membership), so the read-back subset join cannot narrow —
    # restricting to rowset rows is the explicit `rs.sa is not null`; with it,
    # only rows 1 and 2 survive — NOT the full a dimension.
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    eng.execute_raw_sql("create table a_tbl (i int, v double, w double)")
    eng.execute_raw_sql("insert into a_tbl values (1,10,1000),(2,20,2000),(3,30,3000)")
    eng.execute_raw_sql("create table b_tbl (i int, v double)")
    eng.execute_raw_sql("insert into b_tbl values (1,100),(2,200),(4,400)")
    rows = eng.execute_query("""
import a as a;
import b as b;
with rs as
where a.aid in b.bid
select a.aid as k, sum(a.av) as sa;


where a.aw is not null and rs.sa is not null
select rs.k, a.aw
subset join a.aid = rs.k
order by rs.k;
""").fetchall()
    assert [tuple(r) for r in rows] == [(1, 1000.0), (2, 2000.0)]


def test_rowset_key_read_back_aligns_with_source_null_property(models: Path):
    # Same shape as above but aid 1's `aw` is NULL: BOTH outer WHERE atoms are
    # post-join predicates over the preserving read-back relation. Pre-filtering
    # the enrichment side re-admits aid 1 NULL-extended through the completion
    # merge (rows [(1, None), (2, ...)]); pre-filtering the rowset side re-admits
    # aid 3. Only post-join application yields v3's [(2, 2000.0)].
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    eng.execute_raw_sql("create table a_tbl (i int, v double, w double)")
    eng.execute_raw_sql("insert into a_tbl values (1,10,NULL),(2,20,2000),(3,30,3000)")
    eng.execute_raw_sql("create table b_tbl (i int, v double)")
    eng.execute_raw_sql("insert into b_tbl values (1,100),(2,200),(4,400)")
    rows = eng.execute_query("""
import a as a;
import b as b;
with rs as
where a.aid in b.bid
select a.aid as k, sum(a.av) as sa;


where a.aw is not null and rs.sa is not null
select rs.k, a.aw
subset join a.aid = rs.k
order by rs.k;
""").fetchall()
    assert [tuple(r) for r in rows] == [(2, 2000.0)]


# --- Read-back matrix: project the rowset key (`a.aid as k`) under each internal
# scoped-join type, alongside nothing / a count / a SOURCE-side property (a.aw,
# keyed by the join source a.aid) / a CANONICAL-side property (b.bv, keyed by the
# join target b.bid). Expected rows are the CORRECT answer; cells the current
# narrow read-back fix does not yet handle are strict-xfail so they are tracked
# (and flip loudly when fixed). Data: a {1,2,3}, b {1,2,4} ->
# LEFT (a preserved) {1,2,3}, FULL (union) {1,2,3,4}.
# ----------------------------------------------------------------------------

_READS = {
    "k": "select rs.k order by rs.k;",
    "count": "select count(rs.k) -> n;",
}


def _scoped_join(jt: str, left: str, right: str) -> str:
    # `left`/`full` query-scoped joins now spell `subset`/`union`; subset swaps operands.
    if jt == "full":
        return f"union join {left} = {right}"
    return f"subset join {right} = {left}"


def _read_query(jt: str, read: str) -> str:
    if read in _READS:
        return _READS[read]
    if read == "k_aw":
        return (
            f"select rs.k, a.aw,\n{_scoped_join(jt, 'rs.k', 'a.aid')}\norder by rs.k;"
        )
    if read == "k_bv":
        return (
            f"select rs.k, b.bv,\n{_scoped_join(jt, 'rs.k', 'b.bid')}\norder by rs.k;"
        )
    raise ValueError(f"Unknown matrix read {read}")


def _matrix_query(jt: str, read: str) -> str:
    if jt == "single":
        head = "import a as a;\nwith rs as select a.aid as k, sum(a.av) as sa;\n"
    else:
        head = (
            "import a as a;\nimport b as b;\n"
            f"with rs as {_scoped_join(jt, 'a.aid', 'b.bid')} "
            "select a.aid as k, sum(a.av) as sa, sum(b.bv) as sb;\n"
        )
    return head + _read_query(jt, read)


def _matrix_engine(models: Path) -> Executor:
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    eng.execute_raw_sql("create table a_tbl (i int, v double, w double)")
    eng.execute_raw_sql("insert into a_tbl values (1,10,1000),(2,20,2000),(3,30,3000)")
    eng.execute_raw_sql("create table b_tbl (i int, v double)")
    eng.execute_raw_sql("insert into b_tbl values (1,100),(2,200),(4,400)")
    return eng


# (jt, read, expected_correct_rows, xfail_reason_or_None)
_MATRIX: list[tuple[str, str, list, str | None]] = [
    ("single", "k", [(1,), (2,), (3,)], None),
    ("single", "count", [(3,)], None),
    ("single", "k_aw", [(1, 1000.0), (2, 2000.0), (3, 3000.0)], None),
    ("left", "k", [(1,), (2,), (3,)], None),
    ("left", "count", [(3,)], None),
    ("left", "k_aw", [(1, 1000.0), (2, 2000.0), (3, 3000.0)], None),
    ("left", "k_bv", [(1, 100.0), (2, 200.0), (3, None)], None),
    ("full", "k", [(1,), (2,), (3,), (4,)], None),
    ("full", "count", [(4,)], None),
    ("full", "k_aw", [(1, 1000.0), (2, 2000.0), (3, 3000.0), (4, None)], None),
    ("full", "k_bv", [(1, 100.0), (2, 200.0), (3, None), (4, 400.0)], None),
]


def _matrix_params():
    out = []
    for jt, read, expected, xfail in _MATRIX:
        marks = [pytest.mark.xfail(strict=True, reason=xfail)] if xfail else []
        out.append(pytest.param(jt, read, expected, id=f"{jt}-{read}", marks=marks))
    return out


@pytest.mark.parametrize("jt,read,expected", _matrix_params())
def test_rowset_key_readback_matrix(models: Path, jt: str, read: str, expected: list):
    eng = _matrix_engine(models)
    rows = [tuple(r) for r in eng.execute_query(_matrix_query(jt, read)).fetchall()]
    assert rows == expected


# --- Expanded, full-text versions of selected matrix cells, so the input trilogy
# is readable inline. These all pass; they mirror parametrized cells above.
# Data: a {1,2,3}, b {1,2,4}.
# ----------------------------------------------------------------------------


def test_rowset_key_readback_intersection_k(models: Path):
    # Read back only the rowset's intersection key. The body subset-joins b.bid = a.aid
    # guarded by `where b.bv is not null`, so the key is filtered to the matching ids
    # {1,2} (aid 3 and bid 4 dropped) -- the same set a scoped inner join produced.
    eng = _matrix_engine(models)
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    rows = [tuple(r) for r in eng.execute_query("""
import a as a;
import b as b;
with rs as
where b.bv is not null
select a.aid as k, a.av as sa, b.bv as sb
subset join b.bid = a.aid;
select rs.k order by rs.k;
""").fetchall()]
    assert rows == [(1,), (2,)]


# --- LEFT/FULL read-back cells, expanded. All read the rowset's collapsed join
# key (`rs.k`) beside an EXTERNAL property, joining the key back to a dimension
# with the SAME outer-join type as the rowset body. These exercise LEFT/FULL
# partial-key coalescing across the query boundary and now resolve correctly.
# Data: a {1,2,3}, b {1,2,4}.
# ----------------------------------------------------------------------------


def test_rowset_key_readback_left_k_aw(models: Path):
    # LEFT body preserves all a rows {1,2,3}; read the key beside a SOURCE-side
    # property (a.aw, keyed by the join source a.aid). The read-back join adds
    # a second relation onto the same canonical key group; the narrowing pass
    # arbitrates the same-address body pair by binding provenance
    # (subset_binding_sources) and still narrows the body onto its a anchor.
    eng = _matrix_engine(models)
    rows = [tuple(r) for r in eng.execute_query("""
import a as a;
import b as b;
with rs as subset join b.bid = a.aid
select a.aid as k, sum(a.av) as sa, sum(b.bv) as sb;
select rs.k, a.aw,
subset join a.aid = rs.k
order by rs.k;
""").fetchall()]
    assert rows == [(1, 1000.0), (2, 2000.0), (3, 3000.0)]


def test_rowset_key_readback_left_k_bv(models: Path):
    # LEFT body preserves all a rows {1,2,3}; read the key beside a CANONICAL-side
    # property (b.bv, keyed by the join target b.bid). aid 3 has no b row -> None.
    eng = _matrix_engine(models)
    rows = [tuple(r) for r in eng.execute_query("""
import a as a;
import b as b;
with rs as subset join b.bid = a.aid
select a.aid as k, sum(a.av) as sa, sum(b.bv) as sb;
select rs.k, b.bv,
subset join b.bid = rs.k
order by rs.k;
""").fetchall()]
    assert rows == [(1, 100.0), (2, 200.0), (3, None)]


def test_rowset_key_readback_full_k_aw(models: Path):
    # FULL body is the union of keys {1,2,3,4}; read the key beside a SOURCE-side
    # property (a.aw). bid 4 has no a row -> None.
    eng = _matrix_engine(models)
    rows = [tuple(r) for r in eng.execute_query("""
import a as a;
import b as b;
with rs as union join a.aid = b.bid
select a.aid as k, sum(a.av) as sa, sum(b.bv) as sb;
select rs.k, a.aw,
union join rs.k = a.aid
order by rs.k;
""").fetchall()]
    assert rows == [(1, 1000.0), (2, 2000.0), (3, 3000.0), (4, None)]


def test_rowset_key_readback_full_k_bv(models: Path):
    # FULL body is the union of keys {1,2,3,4}; read the key beside a CANONICAL-side
    # property (b.bv). aid 3 has no b row -> None.
    eng = _matrix_engine(models)
    rows = [tuple(r) for r in eng.execute_query("""
import a as a;
import b as b;
with rs as union join a.aid = b.bid
select a.aid as k, sum(a.av) as sa, sum(b.bv) as sb;
select rs.k, b.bv,
union join rs.k = b.bid
order by rs.k;
""").fetchall()]
    assert rows == [(1, 100.0), (2, 200.0), (3, None), (4, 400.0)]


# --- Subordinate coalesced-key read-back across the rowset boundary (TPC-DS q64).
# A coalescing scoped join (`subset`/`union`) collapses the join-key group
# (`a.aid = b.bid`) onto ONE canonical body column, leaving the authored side
# (`a.aid`) only as a pseudonym. When `a.aid` is projected UNALIASED into the
# rowset and referenced downstream by that authored address (`rs.a.aid`), the body
# had no source-map entry for it -> the renderer emitted a Missing-source sentinel.
# The read-back must render cleanly and match the single-statement oracle.
# Data: a {1,2,3}, b {1,2,4}.
# ----------------------------------------------------------------------------


@pytest.mark.parametrize("jt", ["subset", "union"])
def test_rowset_subordinate_key_readback_no_sentinel(models: Path, jt: str):
    sql = _gen(
        models,
        f"""
import a as a;
import b as b;
with rs as
select a.aid, count(a.av) as sa
{jt} join a.aid = b.bid;
select rs.a.aid, rs.sa order by rs.a.aid;
""",
    )
    assert sql and "INVALID_REFERENCE_BUG" not in sql


@pytest.mark.parametrize("jt", ["subset", "union"])
def test_rowset_subordinate_key_readback_matches_single_statement(
    models: Path, jt: str
):
    eng = _matrix_engine(models)
    single = [tuple(r) for r in eng.execute_query(f"""
import a as a;
import b as b;
select a.aid, count(a.av) as sa
{jt} join a.aid = b.bid
order by a.aid;
""").fetchall()]
    boundary = [tuple(r) for r in eng.execute_query(f"""
import a as a;
import b as b;
with rs as
select a.aid, count(a.av) as sa
{jt} join a.aid = b.bid;
select rs.a.aid, rs.sa order by rs.a.aid;
""").fetchall()]
    assert boundary == single
