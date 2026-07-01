"""A scoped INNER join between two independent multi-row rowset populations is a
genuine intersection and should resolve to correct rows -- NOT be rejected.

This is the target behavior for the TPC-DS q38 shape (names present in store AND
catalog sales for a year). Today the scoped-join collapse machinery folds both
operands onto one canonical column and prunes the other side's scan, so the
intersection is lost; `_validate_cross_rowset_inner_joins` (rowset_node.py)
currently raises a clean `UnresolvableQueryException` pointing at a `union(...)`
rewrite. The query is syntactically valid and semantically unambiguous, so the
planner should materialize both populations and INNER-join them.

These tests FAIL until the planner supports that. They supersede the guard-era
assertions in `test_cross_rowset_inner_join_intersect.py`.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

MODEL = """
key store_sale_id int;
property store_sale_id.s_last_name string;
property store_sale_id.s_yr int;
datasource store_sales (id: store_sale_id, ln: s_last_name, y: s_yr) grain (store_sale_id)
query '''select 1 id, 'Smith' ln, 2000 y union all select 2 id, 'Jones' ln, 2000 y union all select 3 id, 'Smith' ln, 1999 y''';

key cat_sale_id int;
property cat_sale_id.c_last_name string;
property cat_sale_id.c_yr int;
datasource cat_sales (id: cat_sale_id, ln: c_last_name, y: c_yr) grain (cat_sale_id)
query '''select 1 id, 'Smith' ln, 2000 y union all select 2 id, 'Brown' ln, 2000 y''';

rowset store_combos <- where store_sale_id.s_yr = 2000
    select store_sale_id.s_last_name as last_name;
rowset catalog_combos <- where cat_sale_id.c_yr = 2000
    select cat_sale_id.c_last_name as last_name;

rowset sc <-
    select store_combos.last_name as last_name
    inner join store_combos.last_name = catalog_combos.last_name;
"""


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_cross_rowset_inner_join_intersect_selects_correct_rows(executor: Executor):
    query = MODEL + "select sc.last_name order by sc.last_name;"
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [r[0] for r in executor.execute_text(query)[0].fetchall()]
    assert rows == ["Smith"]


def test_cross_rowset_inner_join_intersect_count(executor: Executor):
    query = MODEL + "select count(sc.last_name) as c;"
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [int(r[0]) for r in executor.execute_text(query)[0].fetchall()]
    assert rows == [1]
