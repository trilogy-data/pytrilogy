"""An OUTER (subset/union) query-scoped join between two aggregate rowsets that each
contribute a measure to the output must resolve and compile, and the intersection
idiom (subset join + a not-null filter on the optional side) must select matched
rows only.

Surfaced on TPC-DS q05: the agent built two aggregate rowsets off the unified
`all_sales` model (a sale-side sum keyed by channel/dim_id and a return-side sum
keyed by channel/return_dim_id) and tried to `union join` them to lay sales and
returns side by side.

Minimal shape (two single-key models, two aggregate rowsets, join on the key):

    with a as select sales.region as reg,   sum(sales.sales_amt)   as amt_a;
    with b as select returns.region as reg, sum(returns.return_amt) as amt_b;
    select a.reg, a.amt_a, b.amt_b  <JT> join a.reg = b.reg

A rowset OUTER join is sourceable both ways: the collapsed-away key keeps its own
identity + a pseudonym back to the canonical (LEFT) / a MUTUAL pseudonym (FULL)
so discovery connects the two rowset subgraphs. The merge node owns OUTER key
rendering: LEFT/RIGHT preserve the complete side for either authored key, and
FULL coalesces both physical keys. (Query-scoped `inner join` was removed; the
intersection is now a subset join + `where <optional measure> is not null`.)

Handoff: evals/tpcds_agent/handoff_q05_q80_rollup_label_via_join.md (q05 context).
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

SALES = """key region string;
property region.sales_amt float;
datasource sales (reg: region, amt: sales_amt)
grain (region)
query '''select 'east' as reg, 10.0 as amt union all select 'west', 20.0''';
"""

RETURNS = """key region string;
property region.return_amt float;
datasource returns (reg: region, amt: return_amt)
grain (region)
query '''select 'east' as reg, 1.0 as amt union all select 'south', 3.0''';
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "sales.preql").write_text(SALES)
    (tmp_path / "returns.preql").write_text(RETURNS)
    return tmp_path


def _executor(models: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )


_BLEND = """
import sales as sales;
import returns as returns;

with a as select sales.region as reg, sum(sales.sales_amt) as amt_a;
with b as select returns.region as reg, sum(returns.return_amt) as amt_b;

select a.reg, a.amt_a, b.amt_b
{join}
order by a.reg asc;
"""


def _reg_join(jt: str) -> str:
    # subset swaps operands (authored `left join a = b` == `subset join b = a`);
    # union keeps operand order (== the old `full join a = b`).
    return "subset join b.reg = a.reg" if jt == "subset" else "union join a.reg = b.reg"


def _blend_sql(models: Path, jt: str) -> str:
    return _executor(models).generate_sql(_BLEND.format(join=_reg_join(jt)))[-1]


def _blend_rows(models: Path, jt: str) -> list[tuple]:
    res = _executor(models).execute_text(_BLEND.format(join=_reg_join(jt)))[0]
    return [tuple(r) for r in res.fetchall()]


def test_subset_where_notnull_intersects_two_rowset_measures(models: Path):
    # Intersection idiom (replaces the removed scoped `inner join`): subset join
    # plus a not-null filter on the optional side's measure keeps matched rows only.
    # The not-null guard lets the optimizer upgrade the join to a SQL INNER JOIN --
    # exactly the "inner implicit from a condition" behavior -- so we assert on the
    # resulting rows, not the (optimizer-chosen) join keyword.
    text = _BLEND.replace(
        "order by a.reg asc", "where b.amt_b is not null\norder by a.reg asc"
    )
    sql = _executor(models).generate_sql(text.format(join=_reg_join("subset")))[-1]
    assert sql and "INVALID_REFERENCE_BUG" not in sql
    res = _executor(models).execute_text(text.format(join=_reg_join("subset")))[0]
    assert [tuple(r) for r in res.fetchall()] == [("east", 10.0, 1.0)]


def test_subset_join_blends_two_rowset_measures(models: Path):
    # subset preserves the sales side: the returns-less 'west' region stays.
    sql = _blend_sql(models, "subset")
    assert sql and "INVALID_REFERENCE_BUG" not in sql
    assert "LEFT OUTER JOIN" in sql
    assert _blend_rows(models, "subset") == [("east", 10.0, 1.0), ("west", 20.0, None)]


def test_subset_join_optional_key_projects_preserved_value(models: Path):
    # Projecting the OPTIONAL side's key (b.reg) under subset still yields the
    # preserved value: 'west' has no returns row but its key resolves from the
    # sales (preserved) side.
    text = _BLEND.replace(
        "select a.reg, a.amt_a, b.amt_b", "select b.reg, a.amt_a, b.amt_b"
    ).replace("order by a.reg asc", "order by b.reg asc")
    res = _executor(models).execute_text(text.format(join=_reg_join("subset")))[0]
    assert [tuple(r) for r in res.fetchall()] == [
        ("east", 10.0, 1.0),
        ("west", 20.0, None),
    ]


def test_union_join_blends_two_rowset_measures(models: Path):
    # union spans both sides: the returns-only 'south' region's key coalesces in.
    sql = _blend_sql(models, "union")
    assert sql and "INVALID_REFERENCE_BUG" not in sql
    assert "FULL JOIN" in sql.upper()
    assert _blend_rows(models, "union") == [
        ("east", 10.0, 1.0),
        ("south", None, 3.0),
        ("west", 20.0, None),
    ]
