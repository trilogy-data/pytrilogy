"""Repro: an OUTER (LEFT/FULL) query-scoped join between two aggregate rowsets
that each contribute a measure to the output fails, while the IDENTICAL INNER
join resolves and compiles.

Surfaced on TPC-DS q05: the agent built two aggregate rowsets off the unified
`all_sales` model (a sale-side sum keyed by channel/dim_id and a return-side sum
keyed by channel/return_dim_id) and tried to `full join` them to lay sales and
returns side by side. INNER works; LEFT raises DisconnectedConcepts; FULL emits
invalid SQL.

Minimal shape (two single-key models, two aggregate rowsets, join on the key):

    with a as select sales.region as reg,   sum(sales.sales_amt)   as amt_a;
    with b as select returns.region as reg, sum(returns.return_amt) as amt_b;
    select a.reg, a.amt_a, b.amt_b  <JT> join a.reg = b.reg

FIXED 2026-06-22: a rowset OUTER join now mirrors INNER for sourceability. The
collapsed-away key keeps its own identity + a pseudonym back to the canonical
(LEFT) / a MUTUAL pseudonym (FULL) so discovery connects the two rowset
subgraphs. The merge node owns OUTER key rendering: LEFT/RIGHT preserve the
complete side for either authored key, and FULL coalesces both physical keys.

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
{join_type} join a.reg = b.reg
order by a.reg asc;
"""


def _blend_sql(models: Path, join_type: str) -> str:
    return _executor(models).generate_sql(_BLEND.format(join_type=join_type))[-1]


def _blend_rows(models: Path, join_type: str) -> list[tuple]:
    res = _executor(models).execute_text(_BLEND.format(join_type=join_type))[0]
    return [tuple(r) for r in res.fetchall()]


def test_inner_join_blends_two_rowset_measures(models: Path):
    # Baseline: INNER co-sources a measure from each aggregate rowset.
    sql = _blend_sql(models, "inner")
    assert sql and "INVALID_REFERENCE_BUG" not in sql
    assert "INNER JOIN" in sql
    assert _blend_rows(models, "inner") == [("east", 10.0, 1.0)]


def test_left_join_blends_two_rowset_measures(models: Path):
    # LEFT preserves the sales side: the returns-less 'west' region stays.
    sql = _blend_sql(models, "left")
    assert sql and "INVALID_REFERENCE_BUG" not in sql
    assert "LEFT OUTER JOIN" in sql
    assert _blend_rows(models, "left") == [("east", 10.0, 1.0), ("west", 20.0, None)]


def test_left_join_optional_key_projects_preserved_value(models: Path):
    text = _BLEND.replace("select a.reg, a.amt_a, b.amt_b", "select b.reg, a.amt_a, b.amt_b")
    res = _executor(models).execute_text(text.format(join_type="left"))[0]
    assert [tuple(r) for r in res.fetchall()] == [
        ("east", 10.0, 1.0),
        ("west", 20.0, None),
    ]


def test_full_join_blends_two_rowset_measures(models: Path):
    # FULL spans both sides: the returns-only 'south' region's key coalesces in.
    sql = _blend_sql(models, "full")
    assert sql and "INVALID_REFERENCE_BUG" not in sql
    assert "FULL JOIN" in sql.upper()
    assert _blend_rows(models, "full") == [
        ("east", 10.0, 1.0),
        ("south", None, 3.0),
        ("west", 20.0, None),
    ]
