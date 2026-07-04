"""Rowset body LIMIT semantics.

A `with rs as select ... order by ... limit N` truncates the BODY's final
(grouped) output. Contract, pinned here:

- the limit renders inside the rowset's own CTE chain (a dedicated
  ORDER BY + LIMIT node between the body and the translation wrapper), never
  on the outer statement;
- everything downstream is post-limit: an outer WHERE filters the limited
  rows (never changes which rows fill the limit), grouping required by the
  body happens below the limit;
- a limited rowset is a proper row subset, so the structural domain edge
  mints an UNCONDITIONED ⊑ (never ≡, and never a conditioned ⊑ whose
  condition understates the truncation), and join narrowing never claims
  value-completeness through a limited chain (row-identity pinned against
  the un-narrowed plan).
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG
from trilogy.core.domain_graph import DomainRelation, structural_domain_edge
from trilogy.core.models.author import RowsetItem
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine_v2 import clear_parse_cache

A = """key aid int;
property aid.av float;
property aid.aw float;
property aid.cat string;
datasource a (i: aid, v: av, w: aw, c: cat) grain (aid) address a_tbl;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "a.preql").write_text(A)
    return tmp_path


def _engine(models: Path) -> Executor:
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    eng.execute_raw_sql("create table a_tbl (i int, v double, w double, c varchar)")
    eng.execute_raw_sql(
        "insert into a_tbl values (1,10,1000,'x'),(2,20,2000,'x'),"
        "(3,30,3000,'y'),(4,40,4000,'z')"
    )
    return eng


def _rows(models: Path, query: str) -> list[tuple]:
    eng = _engine(models)
    sql = eng.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = [tuple(r) for r in eng.execute_raw_sql(sql).fetchall()]
    return sorted(rows, key=lambda r: tuple((x is None, x) for x in r))


def test_limit_applies_at_source_grain(models: Path):
    rows = _rows(
        models,
        """
import a as a;
with rs as select a.aid as k, sum(a.av) as sa order by k asc limit 2;
select rs.k, rs.sa order by rs.k;
""",
    )
    assert rows == [(1, 10.0), (2, 20.0)]


def test_limit_applies_after_required_grouping(models: Path):
    # totals: x=30, y=30, z=40; top-2 by (total desc, cat asc) = z, x
    rows = _rows(
        models,
        """
import a as a;
with rs as select a.cat as c, sum(a.av) as total order by total desc, c asc limit 2;
select rs.c, rs.total order by rs.c;
""",
    )
    assert rows == [("x", 30.0), ("z", 40.0)]


def test_limit_order_column_not_selected_downstream(models: Path):
    rows = _rows(
        models,
        """
import a as a;
with rs as select a.aid as k, sum(a.av) as sa order by k desc limit 2;
select rs.sa order by rs.sa;
""",
    )
    assert rows == [(30.0,), (40.0,)]


def test_outer_where_applies_post_limit(models: Path):
    # top-2 by k asc = {1, 2}; the filter reads the LIMITED rows, so it must
    # not pre-filter and let k=3 fill the limit.
    rows = _rows(
        models,
        """
import a as a;
with rs as select a.aid as k, sum(a.av) as sa order by k asc limit 2;
select rs.k, rs.sa where rs.sa > 15 order by rs.k;
""",
    )
    assert rows == [(2, 20.0)]


def test_left_readback_keeps_dim_rows_beyond_limit(models: Path):
    # rs = top-2 by k desc = {3, 4}; anchoring on the dim must preserve all
    # dim rows with NULL rs values beyond the limit — narrowing may not claim
    # rs complete through the limited chain.
    rows = _rows(
        models,
        """
import a as a;
with rs as select a.aid as k, sum(a.av) as sa order by k desc limit 2;
select a.aid, a.aw, rs.sa
left join a.aid = rs.k
order by a.aid;
""",
    )
    assert rows == [
        (1, 1000.0, None),
        (2, 2000.0, None),
        (3, 3000.0, 30.0),
        (4, 4000.0, 40.0),
    ]


FULL_READBACK = """
import a as a;
with rs as select a.aid as k, sum(a.av) as sa order by k desc limit 2;
select rs.k, a.aw
full join rs.k = a.aid
order by a.aw;
"""


def test_full_readback_row_identical_under_narrowing(models: Path):
    """The scoped full join coalesces the key group, so all four keys appear;
    rs (a limited subset of a) has no exclusive rows, so any narrowing must be
    row-identical with the un-narrowed preserving plan."""
    expected = [(1, 1000.0), (2, 2000.0), (3, 3000.0), (4, 4000.0)]
    assert _rows(models, FULL_READBACK) == expected
    CONFIG.optimizations.upgrade_outer_key_set_equivalence = False
    clear_parse_cache()
    try:
        assert _rows(models, FULL_READBACK) == expected
    finally:
        CONFIG.optimizations.upgrade_outer_key_set_equivalence = True
        clear_parse_cache()


def _rowset_edge(model_text: str):
    clear_parse_cache()
    eng = Dialects.DUCK_DB.default_executor()
    eng.parse_text(model_text)
    for concept in eng.environment.concepts.values():
        if isinstance(concept.lineage, RowsetItem):
            return structural_domain_edge(concept)
    raise AssertionError("no rowset concept found")


_MINT_BASE = (
    "key aid int;\nproperty aid.av float;\n"
    "datasource a (i: aid, v: av) grain (aid) address a_tbl;\n"
)


def test_limited_body_mints_unconditioned_subset():
    edge = _rowset_edge(
        _MINT_BASE + "with rs as select aid as k order by k asc limit 2;"
    )
    assert edge is not None
    assert edge.relation is DomainRelation.SUBSET
    assert edge.condition is None


def test_filtered_limited_body_drops_edge_condition():
    """A filtered AND limited body must not carry the filter on its ⊑ edge —
    the condition understates the truncation and could prove same-class
    equality via condition_implies."""
    edge = _rowset_edge(
        _MINT_BASE + "with rs as where av > 15 select aid as k order by k asc limit 2;"
    )
    assert edge is not None
    assert edge.relation is DomainRelation.SUBSET
    assert edge.condition is None


def test_unlimited_body_still_mints_equal():
    edge = _rowset_edge(_MINT_BASE + "with rs as select aid as k;")
    assert edge is not None
    assert edge.relation is DomainRelation.EQUAL
