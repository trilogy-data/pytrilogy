"""gen_rowset_node must enrich a rowset across a DERIVED-expression scoped join.

Two grouped rowsets `a`, `b` are joined on a derived key (`a.grp + 1 = b.grp`).
When `b`'s measure is re-aggregated to `a`'s grain, the planner sources
`sum(b.tot)` by grouping `b.tot` at grain `a.grp`; that group node fetches the
rowset `a` (priority `a.grp`) with `b.tot` as an optional and expects BOTH back.

The fetch must materialize the derived join key (`a.grp + 1`, whose pseudonym is
`b.grp`) off `a`'s own output and merge in `b` — returning rowset `a` + the
optional `b.tot`. Before the fix it returned only `a` (no bridge found for the
derived key), so the two rowsets landed disconnected and discovery dead-ended
with a spurious DisconnectedConceptsException (TPC-DS q02 driver).

Repro/diagnosis: evals/tpcds_agent/bug_derived_rowset_join_key_reaggregate_disconnect.md
"""

from trilogy.core.enums import Derivation
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import search_concepts
from trilogy.core.processing.node_generators import gen_rowset_node
from trilogy.core.processing.nodes import History

ORDERS = """key oid int;
property oid.status int;
property oid.amt float;
property oid.grp int;
datasource orders (oid: oid, st: status, amt: amt, g: grp)
grain (oid) address orders_tbl;

with a as select grp as grp, sum(amt) as tot where status = 1;
with b as select grp as grp, sum(amt) as tot where status = 2;

select a.grp, sum(a.tot) as at, sum(b.tot) as bt inner join a.grp + 1 = b.grp;
"""


def _build_for_final_select(env: Environment):
    """Replicate the get_query_node prologue: build the final select's lineage
    with its scoped joins applied, returning (build_environment, graph, history,
    build_statement) ready for direct node-generator calls."""
    from trilogy.core.env_processor import generate_graph
    from trilogy.core.models.build import Factory, get_canonical_pseudonyms
    from trilogy.core.query_processor import _carry_order_by_concepts
    from trilogy.core.statements.author import SelectStatement

    _, statements = env.parse(ORDERS)
    select = [s for s in statements if isinstance(s, SelectStatement)][-1]
    scoped_joins = [
        (j.source_address, j.target_address, j.join_type) for j in select.join_clauses
    ]
    history = History(base_environment=env)
    caches = history.build_caches
    caches.scoped_joins = scoped_joins
    if caches.pseudonym_map is None:
        caches.pseudonym_map = get_canonical_pseudonyms(env)
    factory = Factory(
        environment=env,
        build_cache=caches.build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        grain_build_cache=caches.grain_build_cache,
        pseudonym_map=caches.pseudonym_map,
        scoped_joins=caches.scoped_joins,
    )
    build_statement = factory.build(select.as_lineage(env))
    build_environment = env.materialize_for_select(
        build_statement.local_concepts,
        build_cache=caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=factory.grain_build_cache,
        canonical_build_cache=caches.canonical_build_cache,
        datasource_build_cache=caches.datasource_build_cache,
        scoped_joins=caches.scoped_joins,
    )
    _carry_order_by_concepts(build_statement)
    graph = generate_graph(build_environment)
    return build_environment, graph, history


def test_gen_rowset_node_enriches_across_derived_join_key():
    env = Environment()
    build_environment, graph, history = _build_for_final_select(env)

    a_grp = build_environment.concepts["a.grp"]
    b_tot = build_environment.concepts["b.tot"]

    node = gen_rowset_node(
        concept=a_grp,
        local_optional=[b_tot],
        environment=build_environment,
        g=graph,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )

    assert node is not None
    outputs = {c.address for c in node.output_concepts}
    # The fetch of rowset `a` with `b.tot` optional must return BOTH, bridged
    # over the derived join key.
    assert "a.grp" in outputs
    assert "b.tot" in outputs

    # The bridge must be the derived join column (`a.grp + 1`) materialized off
    # `a`'s own output: a BASIC concept whose pseudonym is the other side's key
    # (`b.grp`). Its presence is what relates the two rowsets in the merge.
    derived_cols = [
        c
        for c in node.output_concepts
        if c.derivation == Derivation.BASIC and "b.grp" in c.pseudonyms
    ]
    assert derived_cols, "derived join key column not materialized on the merge"
    assert {a.address for a in derived_cols[0].concept_arguments} == {"a.grp"}
    node.resolve()


def test_weak_merge_bridges_derived_rowset_join():
    """The canonical network-discovery injection (`gen_merge_node` ->
    `resolve_weak_components` -> Steiner) must connect the two rowsets over the
    derived join key when handed the rowset outputs to merge.

    Repro/design: evals/tpcds_agent/canonical_address_fragmentation_in_weak_merge.md
    """
    from trilogy.core.processing.concept_strategies_v3 import search_concepts
    from trilogy.core.processing.node_generators.node_merge_node import gen_merge_node

    env = Environment()
    build_environment, graph, history = _build_for_final_select(env)
    targets = [
        build_environment.concepts["a.grp"],
        build_environment.concepts["a.tot"],
        build_environment.concepts["b.tot"],
    ]

    merged = gen_merge_node(
        all_concepts=targets,
        g=graph,
        environment=build_environment,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )

    assert merged is not None, "weak merge failed to bridge the two rowsets"
    produced = {c.address for c in merged.output_concepts}
    for t in targets:
        assert t.address in produced
