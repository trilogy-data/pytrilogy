from trilogy import parse
from trilogy.core.domain_graph import (
    BindingEdge,
    DomainEdge,
    DomainGraph,
    DomainRelation,
    EdgeProvenance,
    EdgeScope,
    FDEdge,
    ResolvedRelation,
    assemble_full_graph,
    declared_edge_from_join,
    mint_binding_edges,
    mint_fd_edges,
    mint_structural_edges,
)
from trilogy.core.enums import JoinType


def subset(source: str, target: str, **kwargs) -> DomainEdge:
    return DomainEdge(
        source=source, target=target, relation=DomainRelation.SUBSET, **kwargs
    )


def equal(source: str, target: str, **kwargs) -> DomainEdge:
    return DomainEdge(
        source=source, target=target, relation=DomainRelation.EQUAL, **kwargs
    )


def incomparable(source: str, target: str, **kwargs) -> DomainEdge:
    return DomainEdge(
        source=source, target=target, relation=DomainRelation.INCOMPARABLE, **kwargs
    )


def test_declared_edge_translation():
    edge = declared_edge_from_join(
        "anchor", "sub", JoinType.LEFT_OUTER, EdgeScope.GLOBAL
    )
    assert edge is not None
    assert (edge.source, edge.target, edge.relation) == (
        "sub",
        "anchor",
        DomainRelation.SUBSET,
    )
    edge = declared_edge_from_join("a", "b", JoinType.FULL, EdgeScope.GLOBAL)
    assert edge is not None and edge.relation is DomainRelation.EQUAL
    edge = declared_edge_from_join("a", "b", JoinType.FULL, EdgeScope.STATEMENT)
    assert edge is not None and edge.relation is DomainRelation.INCOMPARABLE
    assert (
        declared_edge_from_join("a", "b", JoinType.INNER, EdgeScope.STATEMENT) is None
    )


def test_registry_derivations():
    graph = DomainGraph.from_scoped_joins(
        [
            (("anchor", "opt", JoinType.LEFT_OUTER), EdgeScope.STATEMENT),
            (("a", "b", JoinType.FULL), EdgeScope.GLOBAL),
            (("c", "d", JoinType.FULL), EdgeScope.STATEMENT),
        ]
    )
    assert graph.canonical_map() == {"opt": "anchor", "a": "b", "c": "d"}
    assert graph.subset_sources() == {"opt"}
    assert graph.subset_join_map() == {"opt": "anchor"}
    assert graph.left_anchor_keys() == {"anchor"}
    # EQUAL and INCOMPARABLE endpoints both veto the outer-join upgrade
    assert graph.outer_relation_keys() == {"b", "d"}
    # ... but only the EQUAL key may narrow once completeness is proven
    assert graph.equal_narrowable_keys() == {"b"}
    assert graph.statement_incomparable_keys() == {"d"}
    assert graph.join_key_groups() == {
        "anchor": {"anchor", "opt"},
        "b": {"a", "b"},
        "d": {"c", "d"},
    }


def test_equal_key_also_statement_union_keeps_veto():
    # the same key authored as a global merge AND a statement union join keeps
    # the never-narrow veto (statement declaration wins)
    graph = DomainGraph.from_scoped_joins(
        [
            (("a", "b", JoinType.FULL), EdgeScope.STATEMENT),
            (("a", "b", JoinType.FULL), EdgeScope.GLOBAL),
        ]
    )
    # dedup keeps the first (statement) declaration
    assert graph.equal_narrowable_keys() == set()
    assert graph.outer_relation_keys() == {"b"}


def test_chained_collapse_matches_declaration_order():
    graph = DomainGraph.from_scoped_joins(
        [
            (("a", "b", JoinType.LEFT_OUTER), EdgeScope.STATEMENT),
            (("a", "c", JoinType.LEFT_OUTER), EdgeScope.STATEMENT),
        ]
    )
    # `a` anchors both LEFT relations: both optionals collapse onto it
    assert graph.canonical_map() == {"b": "a", "c": "a"}
    assert graph.subset_sources() == {"b", "c"}
    assert graph.join_key_groups() == {"a": {"a", "b", "c"}}


def test_relation_resolution():
    graph = DomainGraph(
        edges=[
            subset("a", "b"),
            subset("b", "c"),
            equal("c", "d"),
            incomparable("x", "y"),
        ]
    )
    assert graph.relation("a", "c") is ResolvedRelation.SUBSET
    assert graph.relation("c", "a") is ResolvedRelation.SUPERSET
    assert graph.relation("c", "d") is ResolvedRelation.EQUAL
    # subset reachability crosses the ≡-class: a ⊑ b ⊑ c ≡ d
    assert graph.relation("a", "d") is ResolvedRelation.SUBSET
    assert graph.relation("x", "y") is ResolvedRelation.INCOMPARABLE
    assert graph.relation("a", "x") is ResolvedRelation.UNKNOWN
    # mutual subset edges resolve as EQUAL
    mutual = DomainGraph(edges=[subset("m", "n"), subset("n", "m")])
    assert mutual.relation("m", "n") is ResolvedRelation.EQUAL


def test_fd_closure_transitive():
    graph = DomainGraph(
        fd_edges=[
            FDEdge(determinants=frozenset({"a"}), dependent="b"),
            FDEdge(determinants=frozenset({"b"}), dependent="c"),
        ]
    )
    assert graph.determines({"a"}, "c")
    assert not graph.determines({"c"}, "a")


def test_fd_composite_and_equivalence():
    graph = DomainGraph(
        edges=[equal("a", "a2")],
        fd_edges=[FDEdge(determinants=frozenset({"a", "b"}), dependent="c")],
    )
    # the composite FD fires through the ≡-class member
    assert graph.determines({"a2", "b"}, "c")
    assert not graph.determines({"a2"}, "c")


def test_fd_population_scope():
    scoped = FDEdge(
        determinants=frozenset({"k"}),
        dependent="v",
        provenance=EdgeProvenance.BINDING,
        scope="partial_source",
    )
    partial_binding = [
        BindingEdge("partial_source", "partial_source", "k", complete=False),
        BindingEdge("partial_source", "partial_source", "v", complete=True),
    ]
    graph = DomainGraph(binding_edges=partial_binding, fd_edges=[scoped])
    # holds on its own population
    assert graph.determines({"k"}, "v", population="partial_source")
    # a partial binding does NOT globalize the FD
    assert not graph.determines({"k"}, "v")
    # a complete binding of all involved concepts does (dimension-table rule)
    complete_binding = [
        BindingEdge("dim", "dim", "k", complete=True),
        BindingEdge("dim", "dim", "v", complete=True),
    ]
    global_ok = DomainGraph(
        binding_edges=complete_binding,
        fd_edges=[
            FDEdge(
                determinants=frozenset({"k"}),
                dependent="v",
                provenance=EdgeProvenance.BINDING,
                scope="dim",
            )
        ],
    )
    assert global_ok.determines({"k"}, "v")


def test_overlay_does_not_mutate_base():
    base = DomainGraph(edges=[equal("a", "b")])
    overlay = base.with_overlay([subset("c", "a")])
    assert len(base.edges) == 1
    assert overlay.relation("c", "b") is ResolvedRelation.SUBSET
    assert base.relation("c", "b") is ResolvedRelation.UNKNOWN


def test_contradiction_lint():
    graph = DomainGraph(edges=[subset("a", "b")])
    assert graph.contradicts(incomparable("a", "b")) is not None
    assert graph.contradicts(equal("a", "b")) is None
    declared_against_union = DomainGraph(edges=[incomparable("a", "b")])
    assert declared_against_union.contradicts(equal("a", "b")) is not None
    # declaring the reverse of a conditioned (filtered) structural subset is
    # the reversed-operands author error
    structural = DomainGraph(
        edges=[
            subset(
                "rs.k",
                "a.aid",
                provenance=EdgeProvenance.STRUCTURAL,
                condition="cond",
            )
        ]
    )
    assert structural.contradicts(subset("a.aid", "rs.k")) is not None
    assert structural.contradicts(subset("rs.k", "a.aid")) is None


MODEL = """
key aid int;
property aid.name string;

datasource a_source (
    aid: aid,
    name: name
)
grain (aid)
address a_table;

datasource partial_source (
    aid: ~aid
)
grain (aid)
address b_table;

auto filtered <- filter aid where aid > 2;

with rs as
select aid where name = 'x';
"""


def test_mint_structural_edges():
    env, _ = parse(MODEL)
    edges = {(e.source, e.target, e.relation) for e in mint_structural_edges(env)}
    assert ("local.filtered", "local.aid", DomainRelation.SUBSET) in edges
    filtered_edge = next(
        e for e in mint_structural_edges(env) if e.source == "local.filtered"
    )
    assert filtered_edge.condition is not None
    assert filtered_edge.provenance is EdgeProvenance.STRUCTURAL
    rowset_edges = [e for e in mint_structural_edges(env) if e.source.startswith("rs.")]
    assert rowset_edges, "rowset outputs should mint structural edges"
    assert all(
        e.relation is DomainRelation.SUBSET and e.condition is not None
        for e in rowset_edges
    ), "filtered rowset body means every output is a conditioned subset"


def test_mint_binding_and_fd_edges():
    env, _ = parse(MODEL)
    bindings = mint_binding_edges(env)
    by_key = {(b.datasource, b.concept): b for b in bindings}
    assert by_key[("a_source", "local.aid")].complete
    assert not by_key[("partial_source", "local.aid")].complete
    fds = mint_fd_edges(env)
    declared = [f for f in fds if f.provenance is EdgeProvenance.DECLARED]
    assert any(
        f.determinants == frozenset({"local.aid"}) and f.dependent == "local.name"
        for f in declared
    )
    grain_scoped = [f for f in fds if f.scope == "a_source"]
    assert any(f.dependent == "local.name" for f in grain_scoped)


def test_assemble_full_graph_grain_fd_globalizes_via_complete_binding():
    env, _ = parse(MODEL)
    declared = DomainGraph()
    graph = assemble_full_graph(env, declared)
    # a_source binds aid and name completely at grain(aid): aid -> name holds
    # globally through the complete-binding rule (and via the property key)
    assert graph.determines({"local.aid"}, "local.name")
    assert graph.binding_sources("local.aid") >= {"a_source", "partial_source"}


def test_build_environment_carries_domain_graph():
    env, _ = parse(MODEL)
    build_env = env.materialize_for_select()
    graph = build_env.domain_graph
    assert graph.determines({"local.aid"}, "local.name")
    assert any(
        e.provenance is EdgeProvenance.STRUCTURAL and e.source == "local.filtered"
        for e in graph.edges
    )


def test_author_lint_reversed_merge_operands():
    import pytest

    from trilogy.core.exceptions import InvalidSyntaxException

    # `filtered` is a conditioned structural subset of `aid`; declaring `aid`
    # as the subset side reverses the provable direction
    with pytest.raises(InvalidSyntaxException, match="reversed operands"):
        parse(MODEL + "\nmerge aid into ~filtered;")
    # the consistent direction is fine
    env, _ = parse(MODEL + "\nmerge filtered into ~aid;")
    assert ("local.aid", "local.filtered", JoinType.LEFT_OUTER) in env.merges
