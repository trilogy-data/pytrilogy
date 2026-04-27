from enum import Enum
from logging import Logger
from typing import cast

from trilogy.core.enums import Derivation, FunctionType, Granularity
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

ADDITIVE_ROLLUP_FUNCTIONS = {FunctionType.COUNT, FunctionType.SUM}


def _aggregate_signature(
    concept: BuildConcept,
) -> tuple[FunctionType, tuple[str, ...]] | None:
    from trilogy.core.models.build import BuildAggregateWrapper

    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return None
    return (
        concept.lineage.function.operator,
        tuple(
            sorted(
                arg.canonical_address
                for arg in concept.lineage.function.concept_arguments
            )
        ),
    )


def _datasource_materializes_aggregate(
    datasource: BuildDatasource, concept: BuildConcept
) -> bool:
    signature = _aggregate_signature(concept)
    if signature is None:
        return False
    for output in datasource.output_concepts:
        output_signature = _aggregate_signature(output)
        if output_signature == signature:
            return True
    return False


def _datasource_has_aggregate_output(datasource: BuildDatasource) -> bool:
    return any(
        _aggregate_signature(output) is not None
        for output in datasource.output_concepts
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


def _condition_canonical_addresses(conditions: BuildWhereClause) -> set[str]:
    return {
        c.canonical_address
        for c in conditions.row_arguments
        if c.derivation != Derivation.CONSTANT
    }


def datasource_has_filter_sensitive_aggregate(
    ds: BuildDatasource,
    conditions: BuildWhereClause | None,
) -> bool:
    if not conditions or not any(c.is_aggregate for c in ds.output_concepts):
        return False
    condition_addresses = _condition_canonical_addresses(conditions)
    output_addresses = {c.canonical_address for c in ds.output_concepts}
    return not condition_addresses.issubset(output_addresses)


def _datasource_is_exact_match(
    ds: BuildDatasource,
    criteria: SearchCriteria,
    conditions: BuildWhereClause | None,
    allow_intersection: bool = False,
    allow_filter_application: bool = True,
    relevant_concepts: set[str] | None = None,
) -> bool:
    if not conditions:
        return (
            not ds.non_partial_for
            or criteria == SearchCriteria.PARTIAL_INCLUDING_SCOPED
        )
    # Row filters still affect aggregates unless the datasource can apply them.
    if not ds.non_partial_for:
        if datasource_has_filter_sensitive_aggregate(ds, conditions):
            return False
        condition_addresses = _condition_canonical_addresses(conditions)
        ds_output_addresses = {c.canonical_address for c in ds.output_concepts}
        partial_addresses = {c.canonical_address for c in ds.partial_concepts}
        requested_addresses = relevant_concepts or ds_output_addresses
        if (
            allow_filter_application
            and not conditions.existence_arguments
            and condition_addresses.issubset(ds_output_addresses)
            and condition_addresses.issubset(requested_addresses)
        ) and requested_addresses.isdisjoint(partial_addresses):
            return True
        # Preserve partial-aggregate datasources (e.g. `count` non-partial,
        # `~origin.code` partial) whose condition columns are themselves
        # non-partial: the partial join keys get upgraded by a non-matching-grain
        # source via prune_sources_for_aggregates. Without this branch, condition
        # pruning strips the aggregate before the partial-upgrade step can run.
        has_aggregate_output = any(c.is_aggregate for c in ds.output_concepts)
        if (
            has_aggregate_output
            and partial_addresses
            and allow_filter_application
            and not conditions.existence_arguments
            and condition_addresses
            and condition_addresses.issubset(ds_output_addresses - partial_addresses)
        ):
            return True
        if allow_intersection:
            # When conditions are "covered" by another datasource's non_partial_for,
            # a datasource with no overlap is still an exact match (condition handled elsewhere).
            cond_concept_addresses = {
                c.canonical_address
                for atom in decompose_condition(conditions.conditional)
                for c in atom.concept_arguments
            }
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
    allow_filter_application: bool = True,
    relevant_concepts: set[str] | None = None,
) -> set[str]:
    exact: set[str] = set()
    for node, ds in g.datasources.items():
        if isinstance(ds, BuildUnionDatasource):
            if _union_is_exact_match(ds, conditions):
                exact.add(node)
        elif _datasource_is_exact_match(
            ds,
            criteria,
            conditions,
            allow_intersection,
            allow_filter_application,
            relevant_concepts,
        ):
            exact.add(node)
    return exact


def prune_sources_for_conditions(
    g: "ReferenceGraph",
    criteria: SearchCriteria,
    conditions: BuildWhereClause | None,
    allow_intersection: bool = False,
    relevant_concepts: set[str] | None = None,
):
    complete = get_graph_exact_match(
        g, criteria, conditions, allow_intersection, True, relevant_concepts
    )
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
    orig_g: "ReferenceGraph | None" = None,
) -> None:
    aggregate_concepts: list[BuildConcept] = []
    required_grains = []
    for x in all_concepts:
        logger.debug(
            f"Checking concept {x.address} at grain {x.grain}, is_aggregate={x.is_aggregate}"
        )
        if x.is_aggregate:
            logger.debug(f"Aggregate found: {x.address} at grain {x.grain}")
            aggregate_concepts.append(x)
            required_grains.append(x.grain)
    # if no aggregates, exit
    logger.debug(f"Required grains for aggregates: {required_grains}")
    if not required_grains:

        return
    # Compare grains by canonical address so canonically-equivalent components
    # (e.g. `x_date.year` and `year_via_func` sharing the same lineage hash)
    # don't disqualify a precomputed datasource that binds the named form.
    addr_to_canonical = {bc.address: bc.canonical_address for bc in g.concepts.values()}

    def grain_key(grain) -> frozenset[str]:
        if grain.abstract:
            return frozenset()
        return frozenset(addr_to_canonical.get(c, c) for c in grain.components)

    required_keys = {grain_key(rg) for rg in required_grains}
    if len(required_keys) > 1:
        logger.debug("Multiple required grains found, cannot prune datasources.")
        return
    target = next(iter(required_keys))

    exact_matches = [
        node
        for node, ds in g.datasources.items()
        if isinstance(ds, BuildDatasource)
        and grain_key(ds.grain) == target
        and all(_datasource_materializes_aggregate(ds, c) for c in aggregate_concepts)
    ]
    if exact_matches:
        keep = set(exact_matches)
    else:
        keep = set()
        additive = [
            c
            for c in aggregate_concepts
            if (signature := _aggregate_signature(c))
            and signature[0] in ADDITIVE_ROLLUP_FUNCTIONS
        ]
        if len(additive) == len(aggregate_concepts):
            for node, ds in g.datasources.items():
                if not isinstance(ds, BuildDatasource):
                    continue
                ds_grain = grain_key(ds.grain)
                if not target.issubset(ds_grain) or ds_grain == target:
                    continue
                if all(_datasource_materializes_aggregate(ds, c) for c in additive):
                    keep.add(node)

    keep.update(
        node
        for node, ds in g.datasources.items()
        if isinstance(ds, BuildDatasource) and not _datasource_has_aggregate_output(ds)
    )

    to_remove = []
    for node, ds in g.datasources.items():
        if node not in keep:
            logger.debug(f"Removing datasource {node} at grain {ds.grain}")
            to_remove.append(node)
    for node in to_remove:
        g.remove_node(node)
    # Re-inject upgrade-eligible datasources that condition-pruning removed:
    # a non-partial source for a partial join key in a kept aggregate is still
    # useful even if it can't apply the WHERE itself (the aggregate applies it).
    # Only consider datasources whose grain *is* the partial concept being
    # upgraded — i.e. dimension tables that enumerate that key. A datasource
    # that merely references the key as a foreign column (e.g. an `orders` table
    # with `customer_id`) isn't a full enumerating source.
    if orig_g is not None and partials_to_upgrade:
        for node, ds in orig_g.datasources.items():
            if node in g.datasources or isinstance(ds, BuildUnionDatasource):
                continue
            ds_partial = {c.canonical_address for c in ds.partial_concepts}
            ds_grain_components = grain_key(ds.grain) or frozenset()
            # Only re-inject single-grain dimension tables whose grain key
            # *enumerates* the partial concept being upgraded — either
            # directly (the grain key IS the partial concept) or as a 1:1
            # PROPERTY of the grain key. Foreign-key references (e.g. an
            # `orders` table that mentions `customer_id` at order_id grain)
            # are not full enumerators and must not be re-injected.
            if len(ds_grain_components) != 1:
                continue
            sole_key = next(iter(ds_grain_components))
            relevant = False
            for upgrade_addr in partials_to_upgrade:
                if upgrade_addr in ds_partial:
                    continue
                if upgrade_addr == sole_key:
                    relevant = True
                    break
                concept = orig_g.concepts.get("c~" + upgrade_addr) or next(
                    (
                        c
                        for c in ds.output_concepts
                        if c.canonical_address == upgrade_addr
                    ),
                    None,
                )
                if (
                    concept is not None
                    and concept.purpose == Purpose.PROPERTY
                    and concept.keys
                    and {k for k in concept.keys} == {sole_key}
                ):
                    relevant = True
                    break
            if not relevant:
                continue
            g.add_node(node)
            g.datasources[node] = ds
            for edge in orig_g.edges:
                if edge[0] != node and edge[1] != node:
                    continue
                other = edge[1] if edge[0] == node else edge[0]
                if other not in g:
                    g.add_node(other)
                    if other in orig_g.concepts:
                        g.concepts[other] = orig_g.concepts[other]
                g.add_edge(*edge)
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
