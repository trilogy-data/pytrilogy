"""A bare struct-field key (`local.a`) can carry MULTIPLE derivable pseudonym
origins when two different struct arrays expose the same field name. Only the
origin whose array a datasource actually binds is satisfiable; the v4 graph
walk must select that one rather than committing to an arbitrary (hash-ordered)
pseudonym. `arrA` sorts before `arrB` and is intentionally left unsourced, so a
naive "first origin" pick lands on the dead-end arm.
"""

from trilogy import Environment
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.concept_strategies_v4 import V4History, search_concepts
from trilogy.core.processing.v4_helper.concept_graph import (
    _derivable_pseudonym_origins,
    _resolve_pseudonym_origin,
    build_concept_graph,
)

MULTI_ORIGIN_MODEL = """
key a int;
key b int;
key arrA list<struct<a,b>>;
key arrB list<struct<a,b>>;

auto uA <- unnest(arrA);
auto uB <- unnest(arrB);

datasource sourceB (
    arrB: arrB
) grain (arrB) query '''select [{a:1,b:2}] as arrB''';

SELECT uA.a;
SELECT uB.a;
"""


def _build() -> tuple[Environment, BuildEnvironment, frozenset[str]]:
    env = Environment()
    env.parse(MULTI_ORIGIN_MODEL)
    benv = env.materialize_for_select()
    ds = frozenset(
        c.address for d in benv.datasources.values() for c in d.output_concepts
    )
    return env, benv, ds


def test_bare_key_carries_two_derivable_origins():
    _, benv, _ = _build()
    a = benv.concepts["local.a"]
    assert a.pseudonyms == {"uA.a", "uB.a"}


def test_enumeration_returns_all_origins_deterministically():
    _, benv, ds = _build()
    origins = _derivable_pseudonym_origins(benv.concepts["local.a"], benv, ds)
    assert [o.address for o in origins] == ["uA.a", "uB.a"]


def test_resolution_selects_the_satisfiable_origin():
    """`arrA` sorts first but is unsourced; selection must skip it for `arrB`."""
    _, benv, ds = _build()
    chosen = _resolve_pseudonym_origin(benv.concepts["local.a"], benv, ds)
    assert chosen is not None
    assert chosen.address == "uB.a"


def test_end_to_end_resolves_via_sourceable_arm():
    env, benv, _ = _build()
    info = search_concepts(
        mandatory_list=[benv.concepts["local.a"]],
        history=V4History(base_environment=env),
        environment=benv,
        depth=0,
        g=generate_graph(benv),
        conditions=[],
    )
    assert info.strategy_node is not None
    names = {d.name for d in info.strategy_node.resolve().datasources}
    # The unnest produces a synthetic datasource named off its origin chain;
    # it must descend from the sourced `arrB` arm, never the unsourced `arrA`.
    assert names
    assert all("arrB" in n for n in names)
    assert not any("arrA" in n for n in names)


# Correlated arms: two fields, each reachable from the SAME pair of arrays, both
# arrays sourced. Per-field independent selection could land `a` on one array
# and `b` on the other — two scans plus a join. Cost-aware resolution with a
# running commitment must converge both fields on one array (one scan).
CORRELATED_MODEL = """
key a int;
key b int;
key arrA list<struct<a,b>>;
key arrB list<struct<a,b>>;

auto uA <- unnest(arrA);
auto uB <- unnest(arrB);

datasource sourceA (
    arrA: arrA
) grain (arrA) query '''select [{a:1,b:2}] as arrA''';

datasource sourceB (
    arrB: arrB
) grain (arrB) query '''select [{a:1,b:2}] as arrB''';

SELECT uA.a;
SELECT uB.a;
SELECT uA.b;
SELECT uB.b;
"""


def test_correlated_arms_converge_on_one_array():
    env = Environment()
    env.parse(CORRELATED_MODEL)
    benv = env.materialize_for_select()
    info = search_concepts(
        mandatory_list=[benv.concepts["local.a"], benv.concepts["local.b"]],
        history=V4History(base_environment=env),
        environment=benv,
        depth=0,
        g=generate_graph(benv),
        conditions=[],
    )
    assert info.strategy_node is not None
    names = {d.name for d in info.strategy_node.resolve().datasources}
    # Both fields drawn from `arrA` (lowest address arm) — a single array scan,
    # never a mix that would force scanning `arrB` too.
    assert names
    assert all("arrA" in n for n in names)
    assert not any("arrB" in n for n in names)


def test_no_alternative_edges_survive_resolution():
    """The OR is transient: the graph handed to the group stage is AND-only."""
    env = Environment()
    env.parse(CORRELATED_MODEL)
    benv = env.materialize_for_select()
    _, _, edges = build_concept_graph(
        [benv.concepts["local.a"], benv.concepts["local.b"]], benv, []
    )
    assert all(a.alt_group is None for a in edges.values())
