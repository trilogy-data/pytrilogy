import networkx as nx

from trilogy.core.enums import ComparisonOperator, Derivation, Purpose
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.core import DataType
from trilogy.core.processing.v4_helper.condition_placement import (
    PlacementReason,
    plan_condition_placements,
)
from trilogy.core.processing.v4_helper.constants import (
    FINAL_NODE_ID,
    DepthLabel,
    EdgeKind,
)
from trilogy.core.processing.v4_helper.edges import EdgeMap, add_edge
from trilogy.core.processing.v4_helper.models import GroupBucket


def _addr(name: str) -> str:
    return f"local.{name}"


def _concept(name: str) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        namespace="local",
        grain=BuildGrain(),
        pseudonyms=set(),
    )


def _where(address: str) -> BuildWhereClause:
    return BuildWhereClause(
        conditional=BuildComparison(
            left=_concept(address),
            right="x",
            operator=ComparisonOperator.EQ,
        )
    )


def _bucket(
    derivation: Derivation,
    primary: list[str],
    *,
    secondary: list[str] | None = None,
    depth: DepthLabel = DepthLabel.STAR,
    grain: set[str] | None = None,
) -> GroupBucket:
    return GroupBucket(
        depth_label=depth,
        derivation=derivation,
        grain_components=frozenset(grain or ()),
        primary_members=primary,
        secondary_members=secondary or [],
    )


def _graph() -> tuple[nx.DiGraph, EdgeMap]:
    return nx.DiGraph(), {}


def test_root_condition_lands_on_upstream_root() -> None:
    graph, edges = _graph()
    buckets = {
        "root": _bucket(Derivation.ROOT, [_addr("x")]),
        "basic": _bucket(Derivation.BASIC, [_addr("y")]),
    }
    graph.add_nodes_from(buckets)
    add_edge(graph, edges, "root", "basic", EdgeKind.LINEAGE)
    add_edge(graph, edges, "root", FINAL_NODE_ID, EdgeKind.MERGE)
    add_edge(graph, edges, "basic", FINAL_NODE_ID, EdgeKind.MERGE)

    placements = plan_condition_placements(
        graph, edges, buckets, [_where("x")], [_concept("y")]
    )

    assert len(placements) == 1
    assert placements[0].group_ids == ("root",)
    assert placements[0].reason is PlacementReason.UPSTREAM_MOST


def test_window_output_condition_lands_on_downstream_consumer() -> None:
    graph, edges = _graph()
    buckets = {
        "root": _bucket(Derivation.ROOT, [_addr("x")]),
        "window": _bucket(
            Derivation.WINDOW,
            [_addr("ranked")],
            secondary=[_addr("x")],
            depth=DepthLabel.D0,
            grain={_addr("x")},
        ),
        "basic": _bucket(Derivation.BASIC, [_addr("out")]),
    }
    graph.add_nodes_from(buckets)
    add_edge(graph, edges, "root", "window", EdgeKind.LINEAGE)
    add_edge(graph, edges, "window", "basic", EdgeKind.LINEAGE)
    for gid in buckets:
        add_edge(graph, edges, gid, FINAL_NODE_ID, EdgeKind.MERGE)

    placements = plan_condition_placements(
        graph, edges, buckets, [_where("ranked")], [_concept("out")]
    )

    assert len(placements) == 1
    assert placements[0].group_ids == ("basic",)
    assert placements[0].reason is PlacementReason.UPSTREAM_MOST


def test_cross_grain_aggregate_comparison_defers_to_final() -> None:
    graph, edges = _graph()
    buckets = {
        "root": _bucket(Derivation.ROOT, [_addr("x"), _addr("y")]),
        "agg_x": _bucket(
            Derivation.AGGREGATE,
            [_addr("sum_x")],
            depth=DepthLabel.D0,
            grain={_addr("x")},
        ),
        "agg_y": _bucket(
            Derivation.AGGREGATE,
            [_addr("sum_y")],
            depth=DepthLabel.D0,
            grain={_addr("y")},
        ),
        "consumer": _bucket(Derivation.BASIC, [_addr("out")]),
    }
    graph.add_nodes_from(buckets)
    add_edge(graph, edges, "root", "agg_x", EdgeKind.LINEAGE)
    add_edge(graph, edges, "root", "agg_y", EdgeKind.LINEAGE)
    add_edge(graph, edges, "agg_x", "consumer", EdgeKind.LINEAGE)
    add_edge(graph, edges, "agg_y", "consumer", EdgeKind.LINEAGE)
    for gid in buckets:
        add_edge(graph, edges, gid, FINAL_NODE_ID, EdgeKind.MERGE)
    condition = BuildWhereClause(
        conditional=BuildComparison(
            left=_concept("sum_x"),
            right=_concept("sum_y"),
            operator=ComparisonOperator.EQ,
        )
    )

    placements = plan_condition_placements(
        graph, edges, buckets, [condition], [_concept("out")]
    )

    assert len(placements) == 1
    assert placements[0].group_ids == (FINAL_NODE_ID,)
    assert placements[0].reason is PlacementReason.FINAL_CROSS_GRAIN_AGGREGATE
