"""Lock: a fact column merged into a keyless UNNEST key (date-spine shape) joins
on the merge key, not a cartesian.

`merge d1 into ~s1` is a LEFT_OUTER merge: the UNNEST `s1` (e.g. a date spine) is
the complete domain, `facts.d1` the partial side. `count(id) by d1` must group the
facts by `facts.d1` and LEFT/FULL-join the spine to surface spine values with no
facts. v4 used to source the group key entirely from the spine and emit the fact
scan with ONLY `id`, so the merge had no shared column and fell back to
`FULL JOIN ... on 1=1` (a cartesian, wrong counts). `_widen_merge_join_keys` now
includes a leaf datasource node's own columns when carrying a declared join key,
so the fact emits its partial `d1` and the merge joins `facts.d1 = spine.s1`.

Deterministic (static `unnest([...])`, no `current_date`) and executed, so it
guards rows under both planners — distinct from the shape-only date-spine asserts
in `test_canonical_collision_merge.py`."""

from trilogy import CONFIG, Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key id int;
property id.d1 int;
datasource facts ( id: id, d1: ?d1 )
grain (id)
query '''
select 1 id, 2 d1 union all select 2 id, 2 d1 union all select 3 id, 4 d1
''';
key s1 <- unnest([1,2,3,4,5]);
merge d1 into ~s1;
auto m1 <- count(id) by d1;
"""

_QUERY = "select s1, m1 order by s1 asc;"

# every spine value, with the count of facts whose d1 equals it (0 where none)
_EXPECTED = [(1, 0), (2, 2), (3, 0), (4, 1), (5, 0)]


def _run(v4: bool) -> list[tuple]:
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = v4
    try:
        env = Environment()
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        engine.parse_text(_MODEL)
        rows = engine.execute_text(_QUERY)[-1].fetchall()
        return [(int(r[0]), int(r[1])) for r in rows]
    finally:
        CONFIG.use_v4_discovery = prior


def test_merge_into_unnest_joins_on_key_not_cartesian():
    assert _run(v4=False) == _EXPECTED
    assert _run(v4=True) == _EXPECTED


def test_merge_into_unnest_v4_join_key_in_sql():
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        env = Environment()
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        engine.parse_text(_MODEL)
        sql = engine.generate_sql(_QUERY)[-1]
    finally:
        CONFIG.use_v4_discovery = prior
    # the fact's partial merge key is the join key, never a bare cross join
    assert "on 1=1" not in sql, sql
    assert '"facts"."d1"' in sql, sql
