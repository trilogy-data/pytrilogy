"""A WHERE narrows both sides of an EQUAL merge, not just the one that binds it.

`merge st into store_id` declares one identity over equal domains, which
licenses a FULL stitch between a `store_id` dimension scan and an aggregate at
`st`. Under `where yr = 2001` only the fact side can apply the predicate, so
the filtered fact has to drive the join: a store with no 2001 sale is not in
the row set, however its name is fetched.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

# st and store_id share one domain; store 40 sold only in 2002.
MODEL = """
key store_id int;
property store_id.sname string;
datasource stores (i: store_id, n: sname) grain (store_id)
query '''
select 10 i, 'S10' n union all select 20 i, 'S20' n
union all select 30 i, 'S30' n union all select 40 i, 'S40' n
''';

key sid int;
property sid.yr int;
property sid.st int;
property sid.amt int;
datasource sales (i: sid, y: yr, s: st, a: amt) grain (sid)
query '''
select 1 i, 2001 y, 10 s, 100 a union all select 2 i, 2001 y, 20 s, 200 a
union all select 3 i, 2001 y, 30 s, 300 a union all select 4 i, 2002 y, 20 s, 250 a
union all select 5 i, 2002 y, 30 s, 350 a union all select 6 i, 2002 y, 40 s, 400 a
''';
merge st into store_id;
"""

IN_2001 = [(10, "S10", 100), (20, "S20", 200), (30, "S30", 300)]
ALL_YEARS = [(10, "S10", 100), (20, "S20", 450), (30, "S30", 650), (40, "S40", 400)]


def _rows(query: str) -> list[tuple]:
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(MODEL)
    return [tuple(r) for r in engine.execute_text(query)[-1].fetchall()]


@pytest.mark.parametrize("key", ["st", "store_id"])
@pytest.mark.parametrize("aggregate", ["sum(amt)", "sum(amt) by {key}"])
def test_filtered_aggregate_beside_merged_dimension(key: str, aggregate: str):
    total = aggregate.format(key=key)
    assert (
        _rows(
            f"where yr = 2001 select {key}, sname, {total} as total"
            f" order by {key} asc;"
        )
        == IN_2001
    )


@pytest.mark.parametrize("key", ["st", "store_id"])
@pytest.mark.parametrize("aggregate", ["sum(amt)", "sum(amt) by {key}"])
def test_unfiltered_aggregate_beside_merged_dimension(key: str, aggregate: str):
    total = aggregate.format(key=key)
    assert (
        _rows(f"select {key}, sname, {total} as total order by {key} asc;") == ALL_YEARS
    )
