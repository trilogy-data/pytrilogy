"""A scoped INNER join between two *rowset* outputs inside a rowset body is a
genuine intersection of two independent populations. The scoped-join machinery
collapses the two operands onto one canonical column and prunes the other side's
scan entirely -- so the intersection is silently lost. When the rowset then
projects (or filters on) the pruned side, its source is gone and the renderer
emits a bare `INVALID_REFERENCE_BUG` sentinel; `generate_sql` "succeeds" and
execution throws an uncaught ValueError.

Regression for TPC-DS q38 (count of name/date tuples present in store AND catalog
AND web sales). The eval agent modeled the three-way INTERSECT as per-channel
"combos" rowsets joined pairwise -- a shape the collapse model cannot express.
Rather than emit a sentinel (or, for the no-WHERE variant, silently wrong
results), raise a clean UnresolvableQueryException pointing at the working
`union(...)` + `count_distinct(channel)` rewrite.

Contrast `test_cross_rowset_join_rowset_as_set.py`: there the two operands share a
base (`sales.item`) and the WHERE forces both rowsets to be materialized, so the
join resolves -- those cases must keep working.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

MODEL = """
key store_sale_id int;
property store_sale_id.s_last_name string;
property store_sale_id.s_yr int;
datasource store_sales (id: store_sale_id, ln: s_last_name, y: s_yr) grain (store_sale_id)
query '''select 1 id, 'Smith' ln, 2000 y union all select 2 id, 'Jones' ln, 2000 y''';

key cat_sale_id int;
property cat_sale_id.c_last_name string;
property cat_sale_id.c_yr int;
datasource cat_sales (id: cat_sale_id, ln: c_last_name, y: c_yr) grain (cat_sale_id)
query '''select 1 id, 'Smith' ln, 2000 y union all select 2 id, 'Brown' ln, 2000 y''';

rowset store_combos <- where store_sale_id.s_yr = 2000
    select store_sale_id.s_last_name as last_name;
rowset catalog_combos <- where cat_sale_id.c_yr = 2000
    select cat_sale_id.c_last_name as last_name;
"""

# the cross-rowset INNER-join intersect rowset, projecting the (pruned) store side.
SC_WITH_WHERE = """
rowset sc <-
    where store_combos.last_name = catalog_combos.last_name
    select store_combos.last_name as last_name
    inner join store_combos.last_name = catalog_combos.last_name;
"""

# same shape without the redundant WHERE -- still drops a mandatory INNER operand,
# so it is also unresolvable (was silently wrong: returned the store-only count).
SC_NO_WHERE = """
rowset sc <-
    select store_combos.last_name as last_name
    inner join store_combos.last_name = catalog_combos.last_name;
"""


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


@pytest.mark.parametrize(
    "sc", [SC_WITH_WHERE, SC_NO_WHERE], ids=["with_where", "no_where"]
)
def test_cross_rowset_inner_join_intersect_raises_clean(executor: Executor, sc: str):
    query = MODEL + sc + "select count(sc.last_name) as c;"
    with pytest.raises(UnresolvableQueryException, match="union"):
        executor.generate_sql(query)


def test_union_count_distinct_rewrite_renders_clean(executor: Executor):
    # the documented rewrite the error points at must still build/run cleanly.
    query = MODEL + """
with combined as union(
    (where store_sale_id.s_yr = 2000 select store_sale_id.s_last_name as last_name, 1 as channel),
    (where cat_sale_id.c_yr = 2000 select cat_sale_id.c_last_name as last_name, 2 as channel)
) -> (last_name, channel);

auto channels_per <- count_distinct(combined.channel) by combined.last_name;
select count(combined.last_name ? channels_per = 2) as c;
"""
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
