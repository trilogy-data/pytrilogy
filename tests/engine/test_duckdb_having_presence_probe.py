"""A null test on a coalescing join-key member must filter from HAVING too.

`member is [not] null` on a `union`/`full` key member is rewritten to a per-side
presence probe that has to materialize on the member's OWN side, before the
merge. From a WHERE that happens for free -- the condition's args enter the
concept graph and get planned. A HAVING wraps the already-merged node, so
without an explicit demand the probe re-derives inline over the fused coalesce
column and can never be NULL: the clause silently no-ops (TPC-DS q59/q77).
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key store_key int;
key week_seq int;
property <store_key, week_seq>.amt float;

datasource f (s: store_key, w: week_seq, a: amt)
grain (store_key, week_seq)
query '''
select 1 as s, 1 as w, 10.0 as a
union all select 2 as s, 1 as w, 20.0 as a
union all select 2 as s, 2 as w, 30.0 as a
union all select 3 as s, 2 as w, 40.0 as a
''';
"""

# store 1 is this-year only, store 3 next-year only, store 2 on both sides
ROWSETS = """
rowset this_year <- where week_seq = 1 select {key} as sk, sum(amt) as total;
rowset next_year <- where week_seq = 2 select {key} as sk, sum(amt) as total;
"""

HAVING_ON_KEY = """
select this_year.sk, this_year.total, next_year.total
union join this_year.sk = next_year.sk
having this_year.sk is not null and next_year.sk is not null;
"""

WHERE_ON_KEY = """
where this_year.sk is not null and next_year.sk is not null
select this_year.sk, this_year.total, next_year.total
union join this_year.sk = next_year.sk;
"""

HAVING_ON_MEASURE = """
select this_year.sk, this_year.total, next_year.total
union join this_year.sk = next_year.sk
having this_year.total is not null and next_year.total is not null;
"""

BOTH_KEYS_PROJECTED = """
select this_year.sk, this_year.total, next_year.total, next_year.sk
union join this_year.sk = next_year.sk
having this_year.sk is not null and next_year.sk is not null;
"""

# renaming the member in the SELECT rewrites the HAVING onto the alias, which is
# a BASIC of its own and never a key-group member (TPC-DS q77)
RENAMED_OUTPUTS = """
select this_year.sk as sk, this_year.total as total, next_year.total as nxt
union join this_year.sk = next_year.sk
having this_year.sk is not null and next_year.sk is not null;
"""


def _executor(key: str):
    env = Environment()
    env.parse(MODEL + ROWSETS.format(key=key))
    return Dialects.DUCK_DB.default_executor(environment=env)


@pytest.mark.parametrize("key", ["store_key", "store_key::int"])
@pytest.mark.parametrize(
    "query",
    [
        HAVING_ON_KEY,
        WHERE_ON_KEY,
        HAVING_ON_MEASURE,
        BOTH_KEYS_PROJECTED,
        RENAMED_OUTPUTS,
    ],
)
def test_presence_probe_intersects_from_having(query, key):
    expected = (2, 20.0, 30.0, 2) if query is BOTH_KEYS_PROJECTED else (2, 20.0, 30.0)
    rows = _executor(key).execute_query(query).fetchall()
    assert [tuple(float(v) for v in row) for row in rows] == [expected]
