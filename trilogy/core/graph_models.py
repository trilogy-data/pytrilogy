from enum import Enum
from logging import Logger
from typing import cast

from trilogy.core.enums import Granularity
from trilogy.core.graph import DiGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildUnionDatasource,
    BuildWhereClause,
)
from trilogy.core.processing.condition_utility import (
    condition_implies,
    condition_implies_with_extras,
    decompose_condition,
)


class SearchCriteria(Enum):
    FULL_ONLY = "full_only"
    PARTIAL_UNSCOPED = "partial_unscoped"
    PARTIAL_INCLUDING_SCOPED = "partial_scoped"


def _union_is_exact_match(
    ds: BuildUnionDatasource,
    conditions: BuildWhereClause | None,
) -> bool:
    if not conditions:
        return True
    # When a child's partition is fully satisfied by conditions, prefer that child
    # over the union so the union gets pruned.
    if any(
        child.non_partial_for
        and condition_implies(conditions.conditional, child.non_partial_for.conditional)
        for child in ds.children
    ):
        return False
    return bool(
        ds.non_partial_for
        and condition_implies(conditions.conditional, ds.non_partial_for.conditional)
    )


def _datasource_is_exact_match(
    ds: BuildDatasource,
    criteria: SearchCriteria,
    conditions: BuildWhereClause | None,
    allow_intersection: bool = False,
) -> bool:
    if not conditions:
        return (
            not ds.non_partial_for
            or criteria == SearchCriteria.PARTIAL_INCLUDING_SCOPED
        )
    # All outputs are scalar — filtering has no effect, safe in any context.
    if not ds.non_partial_for:
        if allow_intersection:
            # When conditions are "covered" by another datasource's non_partial_for,
            # a datasource with no overlap is still an exact match (condition handled elsewhere).
            cond_concept_addresses = {
                c.address
                for atom in decompose_condition(conditions.conditional)
                for c in atom.concept_arguments
            }
            ds_output_addresses = {c.address for c in ds.output_concepts}
            if not cond_concept_addresses.intersection(ds_output_addresses):
                return True
        return all(c.granularity == Granularity.SINGLE_ROW for c in ds.output_concepts)
    implied, extras = condition_implies_with_extras(
        conditions.conditional, ds.non_partial_for.conditional
    )
    if not implied:
        return False
    ds_output_addresses = {c.address for c in ds.output_concepts}
    # Only gate on extras whose concepts are local to this datasource; foreign
    # extras (e.g. native_status IS NOT NULL against sf_tree_info) are handled
    # by a joined datasource and must not disqualify this one.
    required = {
        c.address
        for x in extras
        if any(c.address in ds_output_addresses for c in x.concept_arguments)
        for c in x.concept_arguments
    }
    return all(addr in ds_output_addresses for addr in required)


def get_graph_exact_match(
    g: "ReferenceGraph",
    criteria: SearchCriteria,
    conditions: BuildWhereClause | None,
    allow_intersection: bool = False,
) -> set[str]:
    exact: set[str] = set()
    for node, ds in g.datasources.items():
        if isinstance(ds, BuildUnionDatasource):
            if _union_is_exact_match(ds, conditions):
                exact.add(node)
        elif _datasource_is_exact_match(ds, criteria, conditions, allow_intersection):
            exact.add(node)
    return exact


def prune_sources_for_conditions(
    g: "ReferenceGraph",
    criteria: SearchCriteria,
    conditions: BuildWhereClause | None,
    allow_intersection: bool = False,
):
    complete = get_graph_exact_match(g, criteria, conditions, allow_intersection)
    to_remove = []
    for node in g.datasources:
        if node not in complete:
            to_remove.append(node)

    for node in to_remove:
        g.remove_node(node)


def prune_sources_for_aggregates(
    g: "ReferenceGraph",
    all_concepts: list[BuildConcept],
    logger: Logger,
) -> None:
    required_grains = []
    for x in all_concepts:
        logger.debug(
            f"Checking concept {x.address} at grain {x.grain}, is_aggregate={x.is_aggregate}"
        )
        if x.is_aggregate:
            logger.debug(f"Aggregate found: {x.address} at grain {x.grain}")
            required_grains.append(x.grain)
    # if no aggregates, exit
    logger.debug(f"Required grains for aggregates: {required_grains}")
    if not required_grains:

        return
    # if we have distinct grains required, exit
    if len(required_grains) > 1:
        logger.debug("Multiple required grains found, cannot prune datasources.")
        return
    to_remove = []
    for node, ds in g.datasources.items():
        if ds.grain != required_grains[0]:
            logger.debug(f"Removing datasource {node} at grain {ds.grain}")
            to_remove.append(node)
    for node in to_remove:
        g.remove_node(node)
    return


def concept_to_node(input: BuildConcept, stash: dict[str, str] | None = None) -> str:
    lookup = input.canonical_address_grain
    if stash and lookup in stash:
        return stash[lookup]
    # if input.purpose == Purpose.METRIC:
    #     return f"c~{input.namespace}.{input.name}@{input.grain}"

    r = f"c~{input.canonical_address}@{input.grain.str_no_condition}"
    if stash is not None:
        stash[lookup] = r
    return r


def datasource_to_node(input: BuildDatasource) -> str:
    # if isinstance(input, JoinedDataSource):
    #     return "ds~join~" + ",".join(
    #         [datasource_to_node(sub) for sub in input.datasources]
    #     )
    return f"ds~{input.identifier}"


class ReferenceGraph(DiGraph):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.concepts: dict[str, BuildConcept] = {}
        self.datasources: dict[str, BuildDatasource | BuildUnionDatasource] = {}
        self.pseudonyms: set[tuple[str, str]] = set()

    def copy(self) -> "ReferenceGraph":
        g = ReferenceGraph()
        g._copy_from(self)
        g.concepts = self.concepts.copy()
        g.datasources = self.datasources.copy()
        g.pseudonyms = self.pseudonyms.copy()
        return g

    def subgraph(self, nodes) -> "ReferenceGraph":
        keep = set(nodes)
        g = cast(ReferenceGraph, super().subgraph(keep))
        g.concepts = {
            node: concept for node, concept in self.concepts.items() if node in keep
        }
        g.datasources = {
            node: datasource
            for node, datasource in self.datasources.items()
            if node in keep
        }
        g.pseudonyms = {
            edge for edge in self.pseudonyms if edge[0] in keep and edge[1] in keep
        }
        return g

    def remove_node(self, n) -> None:
        if n in self.concepts:
            del self.concepts[n]
        if n in self.datasources:
            del self.datasources[n]
        super().remove_node(n)

    def add_node(self, node_for_adding, **attr):
        return super().add_node(node_for_adding, **attr)

    def add_datasource_node(self, node_name, datasource) -> None:
        super().add_node(node_name, datasource=datasource)

    def add_edge(self, u_of_edge, v_of_edge, **attr):
        return super().add_edge(u_of_edge, v_of_edge, **attr)
