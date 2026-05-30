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


@dataclass
class JoinOrderOutput:
    right: str
    type: JoinType
    keys: dict[str, set[str]]
    left: str | None = None

    @property
    def lefts(self):
        return set(self.keys.keys())


def compute_outer_null_status(
    joins: list,
) -> dict[str, int]:
    """Map each datasource identifier to a null-ability score.

    A score of 0 means the datasource is preserved by every upstream join —
    its concepts are guaranteed non-NULL in the final result. Higher scores
    mean the datasource sits on the NULL-able side of one or more outer
    joins; projecting from it could silently produce NULLs.

    Used by ``MergeNode._resolve`` to (a) order ``final_datasets`` so
    ``resolve_concept_map``'s first-wins picks the preserved side for shared
    concepts and (b) drop NULL-able-side ``ConceptPair`` entries from JOIN ON
    when a preserved-side pair exists for the same key.

    Conventions:
        - ``LEFT OUTER A↔B``: B is NULL-able.
        - ``RIGHT OUTER A↔B``: A is NULL-able.
        - ``FULL A↔B``: both are NULL-able.
        - ``INNER``: neither — no contribution.

    For multi-left joins (``join.left_datasource is None``), the LEFT side is
    a previously-merged set whose individual datasources keep whatever score
    they accumulated from earlier joins; only the right side is touched here.
    """
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
    """In-place prune of ``ConceptPair`` entries on ``LEFT/RIGHT OUTER`` joins.

    When the JOIN ON has multiple pairs sharing the same ``(right_addr,
    left_addr)`` (multi-left case — e.g. q80 ``thoughtful``: both sales and
    returns supply ``sales_channel`` as a join key), keep only the pair whose
    ``existing_datasource`` is most preserved per ``null_status``. The other
    pair would render ``coalesce(preserved.k, nullable.k) = right.k`` — which
    always evaluates to ``preserved.k`` and is pure SQL bloat.

    Skipped for ``FULL`` joins: both sides may be NULL there, so coalesce
    actually contributes — we want every left in JOIN ON.
    """
    for join in joins:
        if not isinstance(join, BaseJoin) or not join.concept_pairs:
            continue
        if join.join_type not in (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER):
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
        # Differentiate "partial" (left's row set is a subset of expected) from
        # "nullable" (left has all its rows but the join key can be NULL).
        # - partial-only: preserve the complete right (RIGHT_OUTER).
        # - nullable-only: preserve left's NULL-key rows (LEFT_OUTER). RIGHT_OUTER
        #   would drop them since NULL doesn't match anything on the right.
        # - both: FULL preserves the complete right AND left's NULL-key rows.
        if left_is_nullable and left_is_partial:
            return JoinType.FULL
        if left_is_nullable:
            return JoinType.LEFT_OUTER
        return JoinType.RIGHT_OUTER
    elif not right_complete and left_complete:
        # LEFT_OUTER would preserve the complete left and drop the right's
        # unmatched rows. With null-aware equality NULL only matches NULL, so
        # if the right has nulls on the join key the non-nullable left has
        # nothing to match them against — they'd land on the dropped side.
        # Upgrade to FULL so they survive.
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
        # Prefer the nullable side as LEFT so a LEFT_OUTER JOIN preserves its
        # NULL-key rows. Flipping it to RIGHT would force FULL (per
        # ``get_join_type``) just to keep them.
        base += 1
    return (base, len(x), x)


def resolve_join_order_v2(
    g: nx.Graph, partials: dict[str, list[str]], nullables: dict[str, list[str]]
) -> list[JoinOrderOutput]:
    datasources = sorted(x for x in g.nodes if x.startswith("ds~"))
    concepts = sorted(x for x in g.nodes if x.startswith("c~"))

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


def _side_nullable(
    concept: BuildConcept, side: QueryDatasource | BuildDatasource | None
) -> bool:
    if side is None:
        return False
    # A pseudonym is the same column under another name; compare across
    # equivalent_addresses so a NULLABLE stamp the datasource carries under a
    # pseudonym address is still seen for a key referenced by its canonical one.
    equivalent = concept.equivalent_addresses
    return any(equivalent & nc.equivalent_addresses for nc in side.nullable_concepts)


def get_modifiers(
    concept: BuildConcept,
    left: QueryDatasource | BuildDatasource | None,
    right: QueryDatasource | BuildDatasource | None,
) -> list[Modifier]:
    """NULLABLE only when ``concept`` is datasource-nullable on BOTH join sides.

    The null-safe equality wrapper (``a is not distinct from b``) only changes
    results when a NULL key must match a NULL key — i.e. both sides can be NULL.
    If either side is a non-null key (e.g. a dimension PK), ``is not distinct
    from`` is equivalent to ``=`` and just costs performance, so stay plain.

    ``left`` is the pair's own ``existing_datasource`` (``JoinOrderOutput.left``
    is ``None`` for the common multi-left join).
    """
    if _side_nullable(concept, left) and _side_nullable(concept, right):
        return [Modifier.NULLABLE]
    return []


def _collect_deep_partial_addresses(
    ds: QueryDatasource | BuildDatasource,
) -> set[str]:
    """Collect partial concept addresses from a datasource and all its sub-datasources.

    UNION nodes are special: their child branches carry table-level PARTIAL
    stamps (each `partial datasource` covers only its slice of the universe),
    but the UNION itself combines those slices into a covering view. Table-
    level stamps subside; only intrinsic column-level partials (``~col``)
    survive — those represent columns that are missing values *within* every
    branch's complete-for slice (e.g. ``~order_id`` on a returns table: not
    every order has a return), so the union remains partial for them too.

    Note: this only affects join-resolution (the join graph reads from
    ``_collect_deep_partial_addresses``). The UnionNode's ``partial_concepts``
    is unchanged at the QueryDatasource level — propagating intrinsic
    partials there breaks output validation for unions whose ``~col`` is a
    syntactic union-eligibility marker rather than a true partial column.
    """
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
    ds: QueryDatasource | BuildDatasource,
) -> set[str]:
    """Like ``_collect_deep_partial_addresses`` but only counts column-level
    (intrinsic) partials, never table-level stamps. Used when descending into
    a UNION's branches for join-resolution.

    A QueryDatasource's own ``partial_concepts`` is skipped because it may
    contain inherited table-level stamps (a SELECT wrapping a ``partial
    datasource`` reports every column as partial). Recurse to leaf
    BuildDatasources where intrinsic partials are tracked explicitly.
    """
    if isinstance(ds, BuildDatasource):
        return set(ds.column_level_partial_addresses)
    if isinstance(ds, QueryDatasource):
        result: set[str] = set()
        for sub in ds.datasources:
            result |= _collect_intrinsic_partial_addresses(sub)
        return result
    return set()


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
    # Reverse direction: an output concept may declare us as its pseudonym
    # without us declaring it as ours. Pseudonym maps from rename basics
    # (`local.channel` ← pseudonym → `l0_filtered.channel_l0`) are set on the
    # rename side only — the rowset concept doesn't carry the back-reference.
    # Without this check, a downstream merge that wants to join on the rowset
    # address can't find its renamed equivalent on the sibling side (q14:
    # rollup aggregate emits `l0_filtered.channel_l0` and tries to find it on
    # the basic side that only exposes `local.channel`).
    for x in datasource.output_concepts:
        if concept.address in x.pseudonyms:
            return x
    raise SyntaxError(
        f"Could not find {concept.address} in {datasource.identifier} output "
        f"{[c.address for c in datasource.output_concepts]}, "
        f"acceptable synonyms {concept.pseudonyms}"
    )


def reduce_concept_pairs(
    input: list[ConceptPair],
    right_source: QueryDatasource | BuildDatasource,
    join_type: JoinType = JoinType.INNER,
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
    # Same left concept from multiple datasources: kept when either pair is
    # partial (FULL JOIN → COALESCE needed) OR the join itself is an outer
    # join (one of the lefts may be NULL on the outer-joined branch — without
    # both pairs, the renderer can't COALESCE around that NULL). Different
    # left concepts for the same right: always kept (distinct conditions).
    is_outer = join_type in (
        JoinType.FULL,
        JoinType.LEFT_OUTER,
        JoinType.RIGHT_OUTER,
    )
    right_left_seen: dict[tuple[str, str], bool] = {}
    for pair in input:
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
        pseudo_name = f"c~{v.address}"
        if pseudo_name in graph.nodes:
            # Another datasource already registered this pseudonym node.
            # Still connect *this* datasource — otherwise the pseudonym
            # link is invisible to find_all_connecting_concepts and the
            # join falls back to a 1=1 cross product (q23 final merge:
            # basic's local.c_first_name ↔ aggregate's sales.customer.first_name).
            graph.add_edge(ds_node, pseudo_name)
            continue
        if v.address != concept.address:
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

    # Propagate partial/nullable status onto the graph so the join scorer sees
    # it on whatever address ends up being the shared join key node.
    #
    # Two things are propagated, both because a pseudonym is the same column
    # under another name:
    #   - PARTIAL from sub-datasources (deep partials), including across merge
    #     pseudonyms — e.g. q80 ``thoughtful`` where a partial fact column joins
    #     a dimension under an aliased key.
    #   - NULLABLE across merge pseudonyms — ``store_sales.date.id`` carries the
    #     NULLABLE modifier, but after ``MERGE store_sales.date.* into ~date.*``
    #     the join key is ``c~date.id`` while ``nullable_concepts`` still
    #     references the pre-merge address. Without this the scorer treats the
    #     merged key as non-nullable and emits ``date_dim LEFT JOIN fact``,
    #     dropping fact rows with NULL FK.
    for datasource in datasources:
        ds_node = f"ds~{datasource.identifier}"
        deep_partials = _collect_deep_partial_addresses(datasource)
        nullable_addrs = {c.address for c in datasource.nullable_concepts}
        # Deep partials may land on a direct neighbor without a pseudonym hop.
        for concept_node in list(graph.neighbors(ds_node)):
            addr = concept_node.removeprefix("c~")
            if addr in deep_partials and concept_node not in partials[ds_node]:
                partials[ds_node].append(concept_node)
        for concept in datasource.output_concepts:
            if concept.address in datasource.hidden_concepts:
                continue
            is_nullable = concept.address in nullable_addrs
            for pseudo_addr in concept.pseudonyms:
                pseudo_name = f"c~{pseudo_addr}"
                if pseudo_name not in graph.nodes:
                    continue
                if pseudo_addr in deep_partials:
                    # The pseudonym carries the partial column; wire it up so
                    # ``find_all_connecting_concepts`` and the scorer both see it.
                    if not graph.has_edge(ds_node, pseudo_name):
                        graph.add_edge(ds_node, pseudo_name)
                    if pseudo_name not in partials[ds_node]:
                        partials[ds_node].append(pseudo_name)
                if is_nullable and pseudo_name not in nullables[ds_node]:
                    nullables[ds_node].append(pseudo_name)

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
                            concept_map[concept],
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
