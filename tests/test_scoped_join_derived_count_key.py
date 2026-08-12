"""A declared scoped-join key must survive a derived count key on the consuming
side.

TPC-DS q06: a category-average rowset is `union join`ed back to sales by
category, and the measure counts line items via `count(grain(item.sk,
ticket_number) ? ...)`. The `grain(...)` hash inserts a projection (and a dedup
group) between the merge and the scan that binds `item.category`, so the
declared key stopped being carryable onto that side and the join rendered
`ON 1=1` — every sale compared against every category average (51 states
instead of 46).

One-token A/B: the same query with a bare `count(item.sk ? ...)` renders the
predicate, because there the merge's parent IS the scan. Both legs are pinned
here alongside the rows, so a re-regression shows up as a cross product rather
than only as a SQL-shape diff. The join key is deliberately NOT projected — a
projected key is already an output of the scan and never exercises the carry.

Handoff: evals/tpcds_agent/handoff_q06_rowset_union_join_key_pruned.md
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# (sk, category, brand, price). Category averages are 30 (x) and 200 (y), so
# under a `1=1` cross product brand b3's cheap item clears x's average and the
# count doubles.
ITEM_ROWS = [
    (1, "x", "b1", 10),
    (2, "x", "b1", 20),
    (3, "x", "b2", 60),
    (4, "y", "b3", 100),
    (5, "y", "b3", 300),
]

ITEMS = """key sk int;
property sk.category string;
property sk.brand string;
property sk.price int;
datasource items (i: sk, c: category, b: brand, p: price)
grain (sk)
query '''{source}''';
"""

QUERY = """import items as a;
import items as b;

rowset cat_avg <-
select
    b.category as category,
    avg(b.price) as cat_avg_price;

select
    a.brand,
    count({count_key} ? a.price > cat_avg.cat_avg_price) as above_avg
union join a.category = cat_avg.category
order by a.brand asc;
"""

EXPECTED = [("b1", 0), ("b2", 1), ("b3", 1)]


@pytest.fixture
def models(tmp_path: Path) -> Path:
    source = " union all ".join(
        f"select {i} i, '{c}' c, '{b}' b, {p} p" for i, c, b, p in ITEM_ROWS
    )
    (tmp_path / "items.preql").write_text(ITEMS.format(source=source))
    return tmp_path


def _executor(models: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )


@pytest.mark.parametrize("count_key", ["a.sk", "grain(a.sk)"])
def test_scoped_join_key_survives_derived_count_key(models: Path, count_key: str):
    executor = _executor(models)
    sql = executor.generate_sql(QUERY.format(count_key=count_key))[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    assert "on 1=1" not in sql, sql
    assert "cat_avg_category" in sql, sql
    rows = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    assert sorted(rows) == EXPECTED, sql
