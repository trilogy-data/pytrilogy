"""Enriching a *property* off a scoped-join key requires chaining the base key
into the join group — `join sk_a = sk_b = store_id` — NOT a planner bug.

Two aggregate rowsets over the same fact, split by year, each rename the store
key (`sk_a`, `sk_b`) so the self-join can distinguish them. The rename is
deliberate: `join sk_a = sk_b` forms an equivalence group `{sk_a, sk_b}` that
does NOT include `store_id`. So a property keyed on `store_id` (`name`) cannot be
sourced — there is no declared path from the join group to `store_id`. Extending
the join to `sk_a = sk_b = store_id` puts `store_id` in the group; the property
then sources naturally. (Auto-sourcing it without the chain would be wrong: the
rename exists precisely to break the self-join identity.)

`test_*_chained` is the supported idiom and must stay green. `test_*_unchained`
documents that the un-chained form does not resolve (it surfaces the general
disconnected/unresolvable-query error).
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.environment import Environment

# Fully self-contained via query-backed datasources (no external tables).
# store 1 (Alpha) has a sale in both years -> survives the inner self-join;
# store 2 (Beta) only in 2001 -> dropped.
MODEL = """
key store_id int;
property store_id.name string;
key sale_id int;
property sale_id.year int;

datasource sales (
    sale_id: sale_id,
    store_id: store_id,
    yr: year,
)
grain (sale_id)
query '''
select 1 sale_id, 1 store_id, 2001 yr
union all select 2 sale_id, 1 store_id, 2002 yr
union all select 3 sale_id, 2 store_id, 2001 yr
''';

datasource stores (
    store_id: store_id,
    nm: name,
)
grain (store_id)
query '''
select 1 store_id, 'Alpha' nm
union all select 2 store_id, 'Beta' nm
''';

rowset agg_a <- where year = 2001 select store_id as sk_a, count(sale_id) as cnt_a;
rowset agg_b <- where year = 2002 select store_id as sk_b, count(sale_id) as cnt_b;
"""

# Chaining store_id into the join group lets the store_id-keyed property source.
CHAINED = MODEL + """
inner join agg_a.sk_a = agg_b.sk_b = store_id
select name as store_name, agg_a.cnt_a, agg_b.cnt_b
order by store_name asc;
"""

# Without the chain, the join group is {sk_a, sk_b}; store_id (hence name) is
# unreachable, so discovery cannot resolve the query.
UNCHAINED = MODEL + """
inner join agg_a.sk_a = agg_b.sk_b
select name as store_name, agg_a.cnt_a, agg_b.cnt_b
order by store_name asc;
"""

EXPECTED = [("Alpha", 1, 1)]


def test_enrich_property_off_scoped_join_key_chained():
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    rows = [tuple(r) for r in eng.execute_text(CHAINED)[-1].fetchall()]
    assert rows == EXPECTED, rows


def test_enrich_property_off_scoped_join_key_unchained_unresolvable():
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    # Rowsets are islanded for connectivity (a base concept reachable only by
    # navigating into a rowset's renamed-key derivation is not a real join path),
    # so store_name splits into its own subgraph and surfaces a named
    # disconnected-subgraph error rather than the generic unresolvable one.
    with pytest.raises(DisconnectedConceptsException, match="local.store_name"):
        eng.generate_sql(UNCHAINED)
