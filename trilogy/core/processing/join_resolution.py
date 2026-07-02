from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trilogy.core import graph as nx

from trilogy.core.enums import JoinType, Modifier, SourceType
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

DataSource = QueryDatasource | BuildDatasource


@dataclass
class JoinOrderOutput:
    right: str
    type: JoinType
    keys: dict[str, set[str]]
    left: str | None = None

    @property
    def lefts(self) -> set[str]:
        return set(self.keys.keys())


OUTER_JOIN_TYPES = (JoinType.FULL, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)
DIRECTIONAL_OUTER_JOIN_TYPES = (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)


def compute_outer_null_status(
    joins: list,
) -> dict[str, int]:
    """Score how often each datasource is null-extended by outer joins."""
    score: dict[str, int] = {}
    for join in joins:
        if not isinstance(join, BaseJoin):
            continue
        left_id = join.left_datasource.identifier if join.left_datasource else None
        right_id = join.right_datasource.identifier
        if join.join_type == JoinType.LEFT_OUTER:
            score[right_id] = score.get(right_id, 0) + 1
        elif join.join_type == JoinType.RIGHT_OUTER:
            if left_id is not None:
                score[left_id] = score.get(left_id, 0) + 1
        elif join.join_type == JoinType.FULL:
            if left_id is not None:
                score[left_id] = score.get(left_id, 0) + 1
            score[right_id] = score.get(right_id, 0) + 1
    return score


def prune_outer_join_pairs(
    joins: list,
    null_status: dict[str, int],
) -> None:
    """Drop redundant duplicate-key pairs from directional outer joins."""
    for join in joins:
        if not isinstance(join, BaseJoin) or not join.concept_pairs:
            continue
        if join.join_type not in DIRECTIONAL_OUTER_JOIN_TYPES:
            continue
        groups: dict[tuple[str, str], list[ConceptPair]] = {}
        for pair in join.concept_pairs:
            key = (pair.right.address, pair.left.address)
            groups.setdefault(key, []).append(pair)
        new_pairs: list[ConceptPair] = []
        for pairs in groups.values():
            if len(pairs) == 1:
                new_pairs.extend(pairs)
                continue
            best = min(
                pairs,
                key=lambda p: (
                    null_status.get(p.existing_datasource.identifier, 0),
                    p.existing_datasource.identifier,
                ),
            )
            new_pairs.append(best)
        join.concept_pairs = new_pairs


def find_all_connecting_concepts(g: nx.Graph, ds1: str, ds2: str) -> set[str]:
    return set(g.neighbors(ds1)) & set(g.neighbors(ds2))


def get_connection_keys(
    all_connections: dict[tuple[str, str], set[str]], left: str, right: str
) -> set[str]:
    key: tuple[str, str] = (min(left, right), max(left, right))
    return all_connections.get(key, set())


def _has_any(keys: set[str], source: str, lookup: dict[str, list[str]]) -> bool:
    return any(key in lookup.get(source, []) for key in keys)


def get_join_type(
    left: str,
    right: str,
    partials: dict[str, list[str]],
    nullables: dict[str, list[str]],
    all_connecting_keys: set[str],
    full_join_keys: set[str] | None = None,
) -> JoinType:
    # A query-scoped FULL join registers its canonical key here. Both sides bind
    # it completely (it is NOT partial) — the FULL JOIN itself spans the rows
    # absent from either side and `_build_joinkeys` coalesces the key. Driving
    # FULL from this registry (rather than the partial flag) keeps the key
    # complete, so the unresolvable-source gate and rowset enrichment never fire.
    if full_join_keys and all_connecting_keys & full_join_keys:
        return JoinType.FULL
    left_is_partial = _has_any(all_connecting_keys, left, partials)
    left_is_nullable = _has_any(all_connecting_keys, left, nullables)
    right_is_partial = _has_any(all_connecting_keys, right, partials)
    right_is_nullable = _has_any(all_connecting_keys, right, nullables)

    left_complete = not left_is_partial and not left_is_nullable
    right_complete = not right_is_partial and not right_is_nullable

    if not left_complete and not right_complete:
        return JoinType.FULL
    elif not left_complete and right_complete:
        # Partial means missing rows; nullable means complete rows with NULL keys.
        if left_is_nullable and left_is_partial:
            return JoinType.FULL
        if left_is_nullable:
            return JoinType.LEFT_OUTER
        return JoinType.RIGHT_OUTER
    elif not right_complete and left_complete:
        if right_is_nullable:
            return JoinType.FULL
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


def ensure_content_preservation(joins: list[JoinOrderOutput]) -> None:
    for idx, review_join in enumerate(joins):
        predecessors = joins[:idx]
        if review_join.type == JoinType.FULL:
            continue
        has_prior_left = False
        has_prior_right = False
        for pred in predecessors:
            on_pred_right = pred.right in review_join.lefts
            on_pred_left = any(x in review_join.lefts for x in pred.lefts)
            if pred.type == JoinType.FULL and (on_pred_right or on_pred_left):
                has_prior_left = True
                has_prior_right = True
                continue
            if pred.type == JoinType.LEFT_OUTER and on_pred_right:
                has_prior_left = True
            if pred.type == JoinType.RIGHT_OUTER and on_pred_left:
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


def _estimated_grain_size(ds: DataSource) -> int:
    return len(ds.grain.components)


def _score_join_candidate(
    x: str,
    *,
    root: str,
    eligible_left: set[str],
    partials: dict[str, list[str]],
    nullables: dict[str, list[str]],
    grain_size: dict[str, int],
    multi_partial: bool,
    anchor_sources: frozenset[str],
) -> tuple[int, int, str]:
    base = 1
    if x in eligible_left:
        base += 3
    # A query-scoped LEFT anchor must seed the join base AND be processed first in
    # the per-right dedup loop so each co-anchored optional source dedups against
    # the anchor (LEFT_OUTER) instead of against the other optional source (FULL).
    # The boost dominates the multi_partial bump so the anchor always outranks.
    if x in anchor_sources:
        base += 10
    is_partial = root in partials.get(x, [])
    if multi_partial and is_partial:
        base += 2
    elif is_partial:
        base -= 1
    if root in nullables.get(x, []):
        base += 1
    return (base, grain_size.get(x, 0), x)


def resolve_join_order_v2(
    g: nx.Graph,
    partials: dict[str, list[str]],
    nullables: dict[str, list[str]],
    grain_size: dict[str, int] | None = None,
    full_join_keys: set[str] | None = None,
    anchor_key_nodes: set[str] | None = None,
) -> list[JoinOrderOutput]:
    """Greedily order the datasources into a join tree.

    Pick a pivot (shared concept), then absorb datasources that connect to the
    growing left set, scoring candidates by eligibility / partial / nullable
    status and breaking ties on estimated grain size (``_score_join_candidate``).
    Every choice point sorts its inputs, so the plan is deterministic across runs.

    Ordering is a heuristic for plan shape only — ``ensure_content_preservation``
    guarantees the result set regardless of the order chosen here.
    """
    grain_size = grain_size or {}
    datasources = sorted(x for x in g.nodes if x.startswith("ds~"))
    concepts = sorted(x for x in g.nodes if x.startswith("c~"))

    # A source is an anchor when it provides a scoped-LEFT anchor key as a
    # COMPLETE (non-partial) concept. Optional sources reference the same key but
    # are partial against it, so they are excluded. An anchor key is only ACTIVE
    # when some present source is partial against it — the boost exists to keep
    # optional sources directional (LEFT, not FULL); if this particular plan has
    # no optional side (e.g. a global `merge ~` whose partial counterpart isn't
    # in the query), seeding the tree on it would just perturb unrelated joins.
    anchor_sources: frozenset[str] = frozenset()
    if anchor_key_nodes:
        active_anchor_keys = {
            key
            for key in anchor_key_nodes
            if any(key in partials.get(ds, []) for ds in datasources)
        }
        anchor_sources = frozenset(
            ds
            for ds in datasources
            if (set(g.neighbors(ds)) & active_anchor_keys)
            and not (active_anchor_keys & set(partials.get(ds, [])))
        )

    all_connections: dict[tuple[str, str], set[str]] = {}
    for i, ds1 in enumerate(datasources):
        for ds2 in datasources[i + 1 :]:
            connecting_concepts = find_all_connecting_concepts(g, ds1, ds2)
            if connecting_concepts:
                all_connections[(min(ds1, ds2), max(ds1, ds2))] = connecting_concepts

    output: list[JoinOrderOutput] = []

    pivot_map = {
        concept: [x for x in g.neighbors(concept) if x in datasources]
        for concept in concepts
    }
    pivots = sorted(
        [x for x in pivot_map if len(pivot_map[x]) > 1],
        key=lambda x: (len(pivot_map[x]), len(x), x),
    )
    solo = [x for x in pivot_map if len(pivot_map[x]) == 1]
    eligible_left: set[str] = set()

    while pivots:
        next_pivots = [
            x for x in pivots if any(y in eligible_left for y in pivot_map[x])
        ]
        if next_pivots:
            root = next_pivots[0]
            pivots = [x for x in pivots if x != root]
        else:
            root = pivots.pop(0)

        unjoined_for_root = [x for x in pivot_map[root] if x not in eligible_left]
        multi_partial = (
            sum(1 for x in unjoined_for_root if root in partials.get(x, [])) > 1
        )

        score_key = partial(
            _score_join_candidate,
            root=root,
            eligible_left=eligible_left,
            partials=partials,
            nullables=nullables,
            grain_size=grain_size,
            multi_partial=multi_partial,
            anchor_sources=anchor_sources,
        )

        to_join = sorted(
            [x for x in pivot_map[root] if x not in eligible_left], key=score_key
        )
        while to_join:
            base = sorted([x for x in eligible_left], key=score_key)
            if not base:
                new = to_join.pop()
                eligible_left.add(new)
                base = [new]
            right = to_join.pop()
            if right in eligible_left:
                continue

            joinkeys: dict[str, set[str]] = {}
            join_types: set[JoinType] = set()

            for left_candidate in reversed(base):
                all_connecting_keys = get_connection_keys(
                    all_connections, left_candidate, right
                )

                if not all_connecting_keys:
                    continue

                # A FULL-join key must keep EVERY left source that provides it:
                # the row may exist on only one of them, so the ON clause has to
                # coalesce across all (`coalesce(l1.k, l2.k) = r.k`). Skipping a
                # redundant left here would drop that source from the coalesce and
                # split rows present only on it. Non-FULL keys still dedup.
                is_full_key = bool(
                    full_join_keys and (all_connecting_keys & full_join_keys)
                )
                exists = False
                if not is_full_key:
                    for existing_left, v in joinkeys.items():
                        if v == all_connecting_keys:
                            left_is_partial = _has_any(
                                all_connecting_keys, left_candidate, partials
                            )
                            existing_is_partial = _has_any(
                                all_connecting_keys, existing_left, partials
                            )
                            if not (left_is_partial and existing_is_partial):
                                exists = True
                if exists:
                    continue

                join_type = get_join_type(
                    left_candidate,
                    right,
                    partials,
                    nullables,
                    all_connecting_keys,
                    full_join_keys,
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
            best_left = None
            best_keys: set[str] = set()
            for existing_left in sorted(eligible_left):
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
                        left=sorted(eligible_left)[0],
                        right=ds,
                        type=JoinType.FULL,
                        keys={},
                    )
                )
            eligible_left.add(ds)

    ensure_content_preservation(output)

    return output


def _side_nullable(concept: BuildConcept, side: DataSource | None) -> bool:
    if side is None:
        return False
    equivalent = concept.equivalent_addresses
    return any(equivalent & nc.equivalent_addresses for nc in side.nullable_concepts)


def get_modifiers(
    left_concept: BuildConcept,
    right_concept: BuildConcept,
    left: DataSource | None,
    right: DataSource | None,
) -> list[Modifier]:
    """Use null-safe equality only when both exposed join keys can be NULL."""
    if _side_nullable(left_concept, left) and _side_nullable(right_concept, right):
        return [Modifier.NULLABLE]
    return []


def _collect_deep_partial_addresses(
    ds: DataSource,
) -> set[str]:
    """Collect partial addresses, suppressing UNION table-level stamps."""
    result: set[str] = {c.address for c in ds.partial_concepts}
    if isinstance(ds, QueryDatasource):
        if ds.source_type == SourceType.UNION:
            for sub in ds.datasources:
                result |= _collect_intrinsic_partial_addresses(sub)
            return result
        for sub in ds.datasources:
            result |= _collect_deep_partial_addresses(sub)
    return result


def _collect_intrinsic_partial_addresses(
    ds: DataSource,
) -> set[str]:
    """Collect column-level partial addresses only."""
    if isinstance(ds, BuildDatasource):
        return set(ds.column_level_partial_addresses)
    if isinstance(ds, QueryDatasource):
        result: set[str] = set()
        for sub in ds.datasources:
            result |= _collect_intrinsic_partial_addresses(sub)
        return result
    return set()


def reduce_concept_pairs(
    pairs: list[ConceptPair],
    right_source: DataSource,
    join_type: JoinType = JoinType.INNER,
) -> list[ConceptPair]:
    from trilogy.core.enums import Purpose

    left_keys = {
        pair.left.address for pair in pairs if pair.left.purpose == Purpose.KEY
    }
    right_keys = {
        pair.right.address for pair in pairs if pair.right.purpose == Purpose.KEY
    }
    final: list[ConceptPair] = []
    seen: set[tuple[str, str]] = set()
    is_outer = join_type in OUTER_JOIN_TYPES
    right_left_seen: dict[tuple[str, str], bool] = {}
    for pair in pairs:
        dedup_key = (pair.right.address, pair.existing_datasource.identifier)
        if dedup_key in seen:
            continue
        rl_key = (pair.right.address, pair.left.address)
        if (
            rl_key in right_left_seen
            and not is_outer
            and not (right_left_seen[rl_key] or pair.is_partial)
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


def build_canonical_address_map(
    datasources: list[DataSource],
    environment: BuildEnvironment,
) -> dict[str, str]:
    """Collapse pseudonym-equivalent concept addresses to one canonical address.

    Join resolution treats each class as one graph node. Pseudonym addresses are
    also linked through ``alias_origin_lookup`` so merged targets and their
    pre-merge addresses share a class.
    """
    from trilogy.core import graph as nx

    pseudonym_graph = nx.Graph()
    for datasource in datasources:
        hidden = datasource.hidden_concepts
        for concept in datasource.output_concepts:
            if concept.address in hidden:
                continue
            pseudonym_graph.add_node(concept.address)
            for pseudo_addr in concept.pseudonyms:
                pseudonym_graph.add_edge(concept.address, pseudo_addr)
                origin = environment.alias_origin_lookup.get(pseudo_addr)
                if origin is not None and origin.address != pseudo_addr:
                    pseudonym_graph.add_edge(pseudo_addr, origin.address)

    canonical: dict[str, str] = {}
    for component in nx.connected_components(pseudonym_graph):
        root = min(component, key=lambda a: (a in environment.alias_origin_lookup, a))
        for address in component:
            canonical[address] = root
    return canonical


def get_node_joins(
    datasources: list[DataSource],
    environment: BuildEnvironment,
) -> list[BaseJoin]:
    from trilogy.core import graph as nx

    canonical = build_canonical_address_map(datasources, environment)

    def canon_node(address: str) -> str:
        return f"c~{canonical.get(address, address)}"

    graph = nx.Graph()
    partials: dict[str, list[str]] = {}
    nullables: dict[str, list[str]] = {}
    grain_size: dict[str, int] = {}
    ds_node_map: dict[str, DataSource] = {}
    ds_concept_map: dict[tuple[str, str], BuildConcept] = {}

    for datasource in datasources:
        ds_node = f"ds~{datasource.identifier}"
        ds_node_map[ds_node] = datasource
        grain_size[ds_node] = _estimated_grain_size(datasource)
        graph.add_node(ds_node, type=NodeType.NODE)
        partial_nodes = {
            canon_node(a) for a in _collect_deep_partial_addresses(datasource)
        }
        # A LEFT scoped join on a *derived* key has no datasource column binding
        # to carry Modifier.PARTIAL. The merge-mechanism keeps that key as a
        # distinct output concept present ONLY on the partial side (the complete
        # side outputs the canonical), so intersecting outputs with
        # scoped_partial_derived marks exactly the partial side here. (Root/rowset
        # partial keys are excluded — they carry partiality through the
        # column-partial / rowset machinery, and a rowset key also survives as a
        # distinct output, so marking it here would double-count.)
        if environment.scoped_partial_derived:
            partial_nodes |= {
                canon_node(c.address)
                for c in datasource.output_concepts
                if c.address in environment.scoped_partial_derived
            }
        nullable_nodes = {canon_node(c.address) for c in datasource.nullable_concepts}
        p_list: list[str] = []
        n_list: list[str] = []
        for concept in datasource.output_concepts:
            if concept.address in datasource.hidden_concepts:
                continue
            node = canon_node(concept.address)
            graph.add_node(node, type=NodeType.CONCEPT)
            graph.add_edge(ds_node, node)
            ds_concept_map.setdefault((ds_node, node), concept)
            if node in partial_nodes and node not in p_list:
                p_list.append(node)
            if node in nullable_nodes and node not in n_list:
                n_list.append(node)
        partials[ds_node] = p_list
        nullables[ds_node] = n_list

    # Canonical keys of query-scoped FULL joins, mapped into graph concept nodes.
    full_join_keys = {canon_node(a) for a in environment.scoped_full_join_keys}
    # Anchor-key nodes of query-scoped LEFT joins: the join tree bases on the
    # complete source providing one so co-anchored optional sources stay LEFT.
    anchor_key_nodes = {canon_node(a) for a in environment.scoped_left_anchor_keys}
    joins = resolve_join_order_v2(
        graph,
        partials=partials,
        nullables=nullables,
        grain_size=grain_size,
        full_join_keys=full_join_keys,
        anchor_key_nodes=anchor_key_nodes,
    )
    return [
        BaseJoin(
            left_datasource=ds_node_map[j.left] if j.left else None,
            right_datasource=ds_node_map[j.right],
            join_type=j.type,
            concepts=[] if not j.keys else None,
            concept_pairs=reduce_concept_pairs(
                [
                    ConceptPair(
                        left=ds_concept_map[(k, concept)],
                        right=ds_concept_map[(j.right, concept)],
                        existing_datasource=ds_node_map[k],
                        modifiers=get_modifiers(
                            ds_concept_map[(k, concept)],
                            ds_concept_map[(j.right, concept)],
                            ds_node_map[k],
                            ds_node_map[j.right],
                        )
                        + (
                            [Modifier.PARTIAL] if concept in partials.get(k, []) else []
                        ),
                    )
                    for k, v in j.keys.items()
                    for concept in v
                ],
                ds_node_map[j.right],
                j.type,
            ),
        )
        for j in joins
    ]
