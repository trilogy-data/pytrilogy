from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trilogy.core import graph as nx

from trilogy.core.enums import JoinType, Modifier
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import (
    BaseJoin,
    ConceptPair,
    QueryDatasource,
)
from trilogy.core.processing.utility import NodeType


@dataclass
class JoinOrderOutput:
    right: str
    type: JoinType
    keys: dict[str, set[str]]
    left: str | None = None

    @property
    def lefts(self):
        return set(self.keys.keys())


def find_all_connecting_concepts(g: nx.Graph, ds1: str, ds2: str) -> set[str]:
    """Find all concepts that connect two datasources"""
    return set(g.neighbors(ds1)) & set(g.neighbors(ds2))


def get_connection_keys(
    all_connections: dict[tuple[str, str], set[str]], left: str, right: str
) -> set[str]:
    """Get all concepts that connect two datasources"""
    key: tuple[str, str] = (min(left, right), max(left, right))
    return all_connections.get(key, set())


def get_join_type(
    left: str,
    right: str,
    partials: dict[str, list[str]],
    nullables: dict[str, list[str]],
    all_connecting_keys: set[str],
) -> JoinType:
    left_is_partial = any(key in partials.get(left, []) for key in all_connecting_keys)
    left_is_nullable = any(
        key in nullables.get(left, []) for key in all_connecting_keys
    )
    right_is_partial = any(
        key in partials.get(right, []) for key in all_connecting_keys
    )
    right_is_nullable = any(
        key in nullables.get(right, []) for key in all_connecting_keys
    )

    left_complete = not left_is_partial and not left_is_nullable
    right_complete = not right_is_partial and not right_is_nullable

    if not left_complete and not right_complete:
        return JoinType.FULL
    elif not left_complete and right_complete:
        return JoinType.RIGHT_OUTER
    elif not right_complete and left_complete:
        return JoinType.LEFT_OUTER
    return JoinType.INNER


def reduce_join_types(join_types: set[JoinType]) -> JoinType:
    if JoinType.FULL in join_types:
        return JoinType.FULL
    has_left = JoinType.LEFT_OUTER in join_types
    has_right = JoinType.RIGHT_OUTER in join_types
    if has_left and has_right:
        return JoinType.FULL
    if has_left:
        return JoinType.LEFT_OUTER
    if has_right:
        return JoinType.RIGHT_OUTER
    return JoinType.INNER


def ensure_content_preservation(joins: list[JoinOrderOutput]):
    # ensure that for a join, if we have prior joins that would
    # introduce nulls, we are controlling for that
    for idx, review_join in enumerate(joins):
        predecessors = joins[:idx]
        if review_join.type == JoinType.FULL:
            continue
        has_prior_left = False
        has_prior_right = False
        for pred in predecessors:
            if (
                pred.type in (JoinType.LEFT_OUTER, JoinType.FULL)
                and pred.right in review_join.lefts
            ):
                has_prior_left = True
            if pred.type in (JoinType.RIGHT_OUTER, JoinType.FULL) and any(
                x in review_join.lefts for x in pred.lefts
            ):
                has_prior_right = True
        if has_prior_left and has_prior_right:
            target = JoinType.FULL
        elif has_prior_left:
            target = (
                JoinType.LEFT_OUTER
                if review_join.type != JoinType.RIGHT_OUTER
                else JoinType.FULL
            )
        elif has_prior_right:
            target = (
                JoinType.RIGHT_OUTER
                if review_join.type != JoinType.LEFT_OUTER
                else JoinType.FULL
            )
        else:
            target = review_join.type
        if review_join.type != target:
            review_join.type = target


def _score_join_candidate(
    x: str,
    *,
    root: str,
    eligible_left: set[str],
    partials: dict[str, list[str]],
    nullables: dict[str, list[str]],
    multi_partial: bool,
) -> tuple[int, int, str]:
    base = 1
    if x in eligible_left:
        base += 3
    is_partial = root in partials.get(x, [])
    if multi_partial and is_partial:
        # boost partial tables so they join together first
        base += 2
    elif is_partial:
        # single partial: lower weight as before
        base -= 1
    if root in nullables.get(x, []):
        base -= 1
    return (base, len(x), x)


def resolve_join_order_v2(
    g: nx.Graph, partials: dict[str, list[str]], nullables: dict[str, list[str]]
) -> list[JoinOrderOutput]:
    datasources = [x for x in g.nodes if x.startswith("ds~")]
    concepts = [x for x in g.nodes if x.startswith("c~")]

    # Pre-compute all possible connections between datasources
    all_connections: dict[tuple[str, str], set[str]] = {}
    for i, ds1 in enumerate(datasources):
        for ds2 in datasources[i + 1 :]:
            connecting_concepts = find_all_connecting_concepts(g, ds1, ds2)
            if connecting_concepts:
                all_connections[(min(ds1, ds2), max(ds1, ds2))] = connecting_concepts

    output: list[JoinOrderOutput] = []

    # create our map of pivots, or common join concepts
    pivot_map = {
        concept: [x for x in g.neighbors(concept) if x in datasources]
        for concept in concepts
    }
    pivots = list(
        sorted(
            [x for x in pivot_map if len(pivot_map[x]) > 1],
            key=lambda x: (len(pivot_map[x]), len(x), x),
        )
    )
    solo = [x for x in pivot_map if len(pivot_map[x]) == 1]
    eligible_left: set[str] = set()

    # while we have pivots, keep joining them in
    while pivots:
        next_pivots = [
            x for x in pivots if any(y in eligible_left for y in pivot_map[x])
        ]
        if next_pivots:
            root = next_pivots[0]
            pivots = [x for x in pivots if x != root]
        else:
            root = pivots.pop(0)

        # Check if multiple unjoined datasources have the concept as partial.
        # When true, partial (fact) tables should be joined together first
        # before complete (dimension) tables.
        unjoined_for_root = [x for x in pivot_map[root] if x not in eligible_left]
        multi_partial = (
            sum(1 for x in unjoined_for_root if root in partials.get(x, [])) > 1
        )

        # sort so less partials is last and eligible lefts are first
        score_key = partial(
            _score_join_candidate,
            root=root,
            eligible_left=eligible_left,
            partials=partials,
            nullables=nullables,
            multi_partial=multi_partial,
        )

        # get remaining un-joined datasets
        to_join = sorted(
            [x for x in pivot_map[root] if x not in eligible_left], key=score_key
        )
        while to_join:
            # need to sort this to ensure we join on the best match
            # but check ALL left in case there are non-pivot keys to join on
            base = sorted([x for x in eligible_left], key=score_key)
            if not base:
                new = to_join.pop()
                eligible_left.add(new)
                base = [new]
            right = to_join.pop()
            # we already joined it
            # this could happen if the same pivot is shared with multiple DSes
            if right in eligible_left:
                continue

            joinkeys: dict[str, set[str]] = {}
            # sorting puts the best candidate last for pop
            # so iterate over the reversed list
            join_types = set()

            for left_candidate in reversed(base):
                # Get all concepts that connect these two datasources
                all_connecting_keys = get_connection_keys(
                    all_connections, left_candidate, right
                )

                if not all_connecting_keys:
                    continue

                # Check if we already have this exact set of keys from
                # a non-partial left. If both left candidates share
                # the same keys and are partial, keep both so the
                # renderer can COALESCE them.
                exists = False
                for existing_left, v in joinkeys.items():
                    if v == all_connecting_keys:
                        left_is_partial = any(
                            key in partials.get(left_candidate, [])
                            for key in all_connecting_keys
                        )
                        existing_is_partial = any(
                            key in partials.get(existing_left, [])
                            for key in all_connecting_keys
                        )
                        if not (left_is_partial and existing_is_partial):
                            exists = True
                if exists:
                    continue

                join_type = get_join_type(
                    left_candidate, right, partials, nullables, all_connecting_keys
                )
                join_types.add(join_type)
                joinkeys[left_candidate] = all_connecting_keys

            final_join_type = reduce_join_types(join_types)

            output.append(
                JoinOrderOutput(
                    right=right,
                    type=final_join_type,
                    keys=joinkeys,
                )
            )
            eligible_left.add(right)

    for concept in solo:
        for ds in pivot_map[concept]:
            if ds in eligible_left:
                continue
            if not eligible_left:
                eligible_left.add(ds)
                continue
            # Try to find if there are any connecting keys with existing left tables
            best_left = None
            best_keys: set[str] = set()
            for existing_left in eligible_left:
                connecting_keys = get_connection_keys(
                    all_connections, existing_left, ds
                )
                if connecting_keys and len(connecting_keys) > len(best_keys):
                    best_left = existing_left
                    best_keys = connecting_keys

            if best_left and best_keys:
                output.append(
                    JoinOrderOutput(
                        left=best_left,
                        right=ds,
                        type=JoinType.FULL,
                        keys={best_left: best_keys},
                    )
                )
            else:
                output.append(
                    JoinOrderOutput(
                        # pick random one to be left
                        left=list(eligible_left)[0],
                        right=ds,
                        type=JoinType.FULL,
                        keys={},
                    )
                )
            eligible_left.add(ds)

    # only once we have all joins do we know if some inners need to be left outers
    ensure_content_preservation(output)

    return output


def get_modifiers(
    concept: str,
    join: JoinOrderOutput,
    ds_node_map: dict[str, QueryDatasource | BuildDatasource],
):
    base = []
    if join.right and concept in ds_node_map[join.right].nullable_concepts:
        base.append(Modifier.NULLABLE)
    if join.left and concept in ds_node_map[join.left].nullable_concepts:
        base.append(Modifier.NULLABLE)
    return list(set(base))


def _collect_deep_partial_addresses(
    ds: QueryDatasource | BuildDatasource,
) -> set[str]:
    """Collect partial concept addresses from a datasource and all its sub-datasources."""
    result: set[str] = {c.address for c in ds.partial_concepts}
    if isinstance(ds, QueryDatasource):
        for sub in ds.datasources:
            result |= _collect_deep_partial_addresses(sub)
    return result


def resolve_instantiated_concept(
    concept: BuildConcept, datasource: QueryDatasource | BuildDatasource
) -> BuildConcept:
    if concept.address in datasource.output_concepts:
        return concept
    for k in concept.pseudonyms:
        if k in datasource.output_concepts:
            return next(x for x in datasource.output_concepts if x.address == k)
        if any(k in x.pseudonyms for x in datasource.output_concepts):
            return next(x for x in datasource.output_concepts if k in x.pseudonyms)
    raise SyntaxError(
        f"Could not find {concept.address} in {datasource.identifier} output "
        f"{[c.address for c in datasource.output_concepts]}, "
        f"acceptable synonyms {concept.pseudonyms}"
    )


def reduce_concept_pairs(
    input: list[ConceptPair], right_source: QueryDatasource | BuildDatasource
) -> list[ConceptPair]:
    from trilogy.core.enums import Purpose

    left_keys = set()
    right_keys = set()
    for pair in input:
        if pair.left.purpose == Purpose.KEY:
            left_keys.add(pair.left.address)
        if pair.right.purpose == Purpose.KEY:
            right_keys.add(pair.right.address)
    final: list[ConceptPair] = []
    seen: set[tuple[str, str]] = set()
    # Track (right_addr, left_addr) combinations from different datasources.
    # Same left concept from multiple datasources: keep only when partial
    # (FULL JOIN → COALESCE needed). Different left concepts for the same
    # right: always keep (they are distinct join conditions).
    right_left_seen: dict[tuple[str, str], bool] = {}
    for pair in input:
        dedup_key = (pair.right.address, pair.existing_datasource.identifier)
        if dedup_key in seen:
            continue
        rl_key = (pair.right.address, pair.left.address)
        if rl_key in right_left_seen and not (
            right_left_seen[rl_key] or pair.is_partial
        ):
            continue
        if (
            pair.left.purpose == Purpose.PROPERTY
            and pair.left.keys
            and pair.left.keys.issubset(left_keys)
        ):
            continue
        if (
            pair.right.purpose == Purpose.PROPERTY
            and pair.right.keys
            and pair.right.keys.issubset(right_keys)
        ):
            continue

        seen.add(dedup_key)
        right_left_seen[rl_key] = right_left_seen.get(rl_key, False) or pair.is_partial
        final.append(pair)
    all_keys = {x.right.address for x in final}
    if right_source.grain.components and right_source.grain.components.issubset(
        all_keys
    ):
        return [x for x in final if x.right.address in right_source.grain.components]

    return final


def add_node_join_concept(
    graph: nx.Graph | nx.DiGraph,
    concept: BuildConcept,
    concept_map: dict[str, BuildConcept],
    ds_node: str,
    environment: BuildEnvironment,
):
    name = f"c~{concept.address}"
    graph.add_node(name, type=NodeType.CONCEPT)
    graph.add_edge(ds_node, name)
    concept_map[name] = concept
    for v_address in concept.pseudonyms:
        v = environment.alias_origin_lookup.get(
            v_address, environment.concepts[v_address]
        )
        if f"c~{v.address}" in graph.nodes:
            continue
        if v != concept.address:
            add_node_join_concept(
                graph=graph,
                concept=v,
                concept_map=concept_map,
                ds_node=ds_node,
                environment=environment,
            )


def get_node_joins(
    datasources: list[QueryDatasource | BuildDatasource],
    environment: BuildEnvironment,
) -> list[BaseJoin]:
    from trilogy.core import graph as nx

    graph = nx.Graph()
    partials: dict[str, list[str]] = {}
    nullables: dict[str, list[str]] = {}
    ds_node_map: dict[str, QueryDatasource | BuildDatasource] = {}
    concept_map: dict[str, BuildConcept] = {}
    for datasource in datasources:
        ds_node = f"ds~{datasource.identifier}"
        ds_node_map[ds_node] = datasource
        graph.add_node(ds_node, type=NodeType.NODE)
        partials[ds_node] = [f"c~{c.address}" for c in datasource.partial_concepts]
        nullables[ds_node] = [f"c~{c.address}" for c in datasource.nullable_concepts]
        for concept in datasource.output_concepts:
            if concept.address in datasource.hidden_concepts:
                continue
            add_node_join_concept(
                graph=graph,
                concept=concept,
                concept_map=concept_map,
                ds_node=ds_node,
                environment=environment,
            )

    # Propagate partial information from sub-datasources.
    # Mark concepts as partial when any sub-datasource has them as partial,
    # and add graph edges for partial pseudonyms to enable correct join ordering.
    for datasource in datasources:
        ds_node = f"ds~{datasource.identifier}"
        deep_partials = _collect_deep_partial_addresses(datasource)
        if not deep_partials:
            continue
        # Mark direct graph neighbors as partial
        for concept_node in list(graph.neighbors(ds_node)):
            addr = concept_node.removeprefix("c~")
            if addr in deep_partials and concept_node not in partials[ds_node]:
                partials[ds_node].append(concept_node)
        # Add edges for partial pseudonyms (merge aliases)
        for concept in datasource.output_concepts:
            if concept.address in datasource.hidden_concepts:
                continue
            for pseudo_addr in concept.pseudonyms:
                pseudo_name = f"c~{pseudo_addr}"
                if (
                    pseudo_name in graph.nodes
                    and not graph.has_edge(ds_node, pseudo_name)
                    and pseudo_addr in deep_partials
                ):
                    graph.add_edge(ds_node, pseudo_name)
                    if pseudo_name not in partials[ds_node]:
                        partials[ds_node].append(pseudo_name)

    joins = resolve_join_order_v2(graph, partials=partials, nullables=nullables)
    return [
        BaseJoin(
            left_datasource=ds_node_map[j.left] if j.left else None,
            right_datasource=ds_node_map[j.right],
            join_type=j.type,
            # preserve empty field for maps
            concepts=[] if not j.keys else None,
            concept_pairs=reduce_concept_pairs(
                [
                    ConceptPair(
                        left=resolve_instantiated_concept(
                            concept_map[concept], ds_node_map[k]
                        ),
                        right=resolve_instantiated_concept(
                            concept_map[concept], ds_node_map[j.right]
                        ),
                        existing_datasource=ds_node_map[k],
                        modifiers=get_modifiers(
                            concept_map[concept].address, j, ds_node_map
                        )
                        + (
                            [Modifier.PARTIAL] if concept in partials.get(k, []) else []
                        ),
                    )
                    for k, v in j.keys.items()
                    for concept in v
                ],
                ds_node_map[j.right],
            ),
        )
        for j in joins
    ]
