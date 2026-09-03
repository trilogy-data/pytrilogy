from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trilogy.core import graph as nx

from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import (
    AggregateGroupingMode,
    Derivation,
    Granularity,
    JoinType,
    Modifier,
    Purpose,
    SourceType,
)
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.functions import propagates_argument_nulls
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildRowsetItem,
    get_grouped_aggregate_wrapper,
)
from trilogy.core.models.build_environment import (
    BuildEnvironment,
    resolve_rowset_content_address,
)
from trilogy.core.models.execute import (
    BaseJoin,
    ConceptPair,
    QueryDatasource,
    UnnestJoin,
)
from trilogy.core.processing.condition_utility import is_scalar_condition
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


def rollup_padded_addresses(datasource: DataSource) -> set[str]:
    """Grouping-key addresses this source NULL-pads because it renders
    `GROUP BY ROLLUP/CUBE/GROUPING SETS` itself. A wrapper already computed
    upstream is a passthrough: it re-emits the padded rows, it does not
    create them."""
    if not isinstance(datasource, QueryDatasource):
        return set()
    upstream = {
        c.address for parent in datasource.datasources for c in parent.output_concepts
    }
    padded: set[str] = set()
    for concept in datasource.output_concepts:
        wrapper = get_grouped_aggregate_wrapper(concept)
        if (
            wrapper is None
            or wrapper.grouping == AggregateGroupingMode.STANDARD
            or concept.address in upstream
        ):
            continue
        padded.update(b.address for b in wrapper.by)
    return padded


def extent_null_addresses(
    datasource: DataSource, _memo: dict[int, set[str]] | None = None
) -> set[str]:
    """Addresses this source can genuinely emit NULL for or omit a member of
    BECAUSE of a `?` declaration: a `?` binding at a leaf, or a key every
    provider of which is null-extended by a VALUE-NULL-DRIVEN outer join in
    this source's own tree (or already extent-null within that provider).
    Narrower than ``nullable_concepts`` twice over: a side merely JOINED on a
    nullable condition gets no mark (an INNER join introduces no NULLs), and
    padding from partial-driven (`~`) preserving joins gets none either, since
    extension families ride the host machinery and claiming their padding here
    would re-preserve rows that machinery already keeps exactly once. ROLLUP
    padding is likewise excluded; ``rollup_padded_addresses`` owns it."""
    memo = _memo if _memo is not None else {}
    cached = memo.get(id(datasource))
    if cached is not None:
        return cached
    out: set[str] = set()
    memo[id(datasource)] = out
    if isinstance(datasource, BuildDatasource):
        for concept in datasource.nullable_concepts:
            out.add(concept.address)
            out.update(concept.pseudonyms)
        return out
    child_null: dict[str, set[str]] = {}
    for child in datasource.datasources:
        child_null[child.identifier] = extent_null_addresses(child, memo)
    base_joins = [j for j in datasource.joins if isinstance(j, BaseJoin)]
    right_ids = {j.right_datasource.identifier for j in base_joins}
    # Left-deep accumulation as in find_nullable_concepts: a RIGHT/FULL join
    # null-extends the whole accumulated left input, not just one operand.
    extended: set[str] = set()
    accumulated = {i for i in child_null if i not in right_ids}
    for join in base_joins:
        right_id = join.right_datasource.identifier
        value_driven = any(
            nulls_are_values(pair.left, pair.existing_datasource)
            or nulls_are_values(pair.right, join.right_datasource)
            for pair in join.concept_pairs or []
        )
        if value_driven:
            if join.join_type in (JoinType.LEFT_OUTER, JoinType.FULL):
                extended.add(right_id)
            if join.join_type in (JoinType.RIGHT_OUTER, JoinType.FULL):
                extended |= accumulated
        accumulated.add(right_id)
    for address, providers in datasource.source_map.items():
        idents = {
            p.identifier
            for p in providers
            if isinstance(p, (BuildDatasource, QueryDatasource))
        }
        if idents and all(
            ident in extended or address in child_null.get(ident, set())
            for ident in idents
        ):
            out.add(address)
    for concept in datasource.output_concepts:
        if concept.address in out:
            out.update(concept.pseudonyms)
    return out


def extension_padded_addresses(
    datasource: DataSource,
    spans: frozenset[str],
    _memo: dict[int, set[str]] | None = None,
) -> set[str]:
    """Addresses this source only emits NULL for because a ``~``-preserving
    join padded them to carry one of ``spans``' extension members.

    Read by a merge that is extent-free for those spans: the padded rows belong
    to the branch the statement elected to own them, so here they are absence,
    not content. Treating their NULLs as ordinary nullability would make this
    merge preserve rows whose values it can never supply, which is the copy the
    FINAL assembly then has to reunite or throw away. Only joins keyed on a licensed
    span count; an ordinary outer lookup pads for its own reasons and its
    nullability stands."""
    memo = _memo if _memo is not None else {}
    cached = memo.get(id(datasource))
    if cached is not None:
        return cached
    out: set[str] = set()
    memo[id(datasource)] = out
    if isinstance(datasource, BuildDatasource):
        return out
    child_padded: dict[str, set[str]] = {
        child.identifier: extension_padded_addresses(child, spans, memo)
        for child in datasource.datasources
    }
    base_joins = [j for j in datasource.joins if isinstance(j, BaseJoin)]
    right_ids = {j.right_datasource.identifier for j in base_joins}
    extended: set[str] = set()
    accumulated = {i for i in child_padded if i not in right_ids}
    for join in base_joins:
        right_id = join.right_datasource.identifier
        span_keyed = any(
            pair.left.address in spans or pair.right.address in spans
            for pair in join.concept_pairs or []
        ) or any(concept.address in spans for concept in join.concepts or [])
        if span_keyed:
            if join.join_type in (JoinType.LEFT_OUTER, JoinType.FULL):
                extended.add(right_id)
            if join.join_type in (JoinType.RIGHT_OUTER, JoinType.FULL):
                extended |= accumulated
        accumulated.add(right_id)
    for address, providers in datasource.source_map.items():
        idents = {
            p.identifier
            for p in providers
            if isinstance(p, (BuildDatasource, QueryDatasource))
        }
        if idents and all(
            ident in extended or address in child_padded.get(ident, set())
            for ident in idents
        ):
            out.add(address)
    for concept in datasource.output_concepts:
        if concept.address in out:
            out.update(concept.pseudonyms)
    return out


def _is_nullable_grain_aligned_merge(
    left: str,
    right: str,
    all_connecting_keys: set[str],
    node_grains: dict[str, set[str]] | None,
    extent_nullables: dict[str, list[str]] | None,
) -> bool:
    """Both sides are complete group-sets at the merge grain (each side's
    grain sits within the connecting keys, so each row is a result in its own
    right rather than feeder content for the other side) and no side can pair
    completely on intact keys. Only extent nullability (a `?` binding, outer
    join padding) weakens a domain claim; when the keys free of it still cover
    one side's grain, that side's intact claim makes the pairing total (a
    value-nullable attribute riding a solid key that determines it) and the
    ordinary directional/INNER typing stands."""
    if node_grains is None or extent_nullables is None:
        return False
    left_grain = node_grains.get(left) or set()
    right_grain = node_grains.get(right) or set()
    if not (
        left_grain
        and right_grain
        and left_grain <= all_connecting_keys
        and right_grain <= all_connecting_keys
    ):
        return False
    solid = {
        key
        for key in all_connecting_keys
        if key not in extent_nullables.get(left, [])
        and key not in extent_nullables.get(right, [])
    }
    return not (left_grain <= solid or right_grain <= solid)


def get_join_type(
    left: str,
    right: str,
    partials: dict[str, list[str]],
    nullables: dict[str, list[str]],
    all_connecting_keys: set[str],
    full_join_keys: set[str] | None = None,
    rollup_padded: dict[str, list[str]] | None = None,
    host_nodes: set[str] | None = None,
    value_nullables: dict[str, list[str]] | None = None,
    demanded_domains: set[str] | None = None,
    node_grains: dict[str, set[str]] | None = None,
    authored_keys: set[str] | None = None,
    extent_nullables: dict[str, list[str]] | None = None,
    extent_free_keys: set[str] | None = None,
    span_binding_sources: dict[str, dict[str, frozenset[str]]] | None = None,
) -> JoinType:
    # Rendering is row-preserving by default: a relation declares DOMAIN
    # knowledge, never row intent, and no join silently drops a row
    # (docs/subset_union_join_design.md). The narrowing pass
    # (UpgradeOuterFromKeySetEquivalence) restores a directional/INNER form
    # only when provably row-identical.
    #
    # UNION-declared keys (query-scoped `full join` / `union join`, non-partial
    # merges): neither domain contains the other, so FULL with the key
    # coalesced by `_build_joinkeys`; the registry also vetoes narrowing.
    # Driving FULL from this registry rather than the partial flag keeps the
    # key complete, so the unresolvable-source gate and rowset enrichment
    # never fire.
    if full_join_keys and all_connecting_keys & full_join_keys:
        return JoinType.FULL
    left_is_partial = _has_any(all_connecting_keys, left, partials)
    right_is_partial = _has_any(all_connecting_keys, right, partials)
    left_is_nullable = _has_any(all_connecting_keys, left, nullables)
    right_is_nullable = _has_any(all_connecting_keys, right, nullables)

    # A span the statement elected another group to own
    # (v4_helper/extent_ownership.py). Its extension members are not this
    # merge's to manufacture, so its `~` mark grants no row intent here: with a
    # clean fact/dimension split anchor the fact and let equality shed the
    # members it never referenced.
    if extent_free_keys and not (authored_keys and all_connecting_keys & authored_keys):
        span_keys = {
            key
            for key in all_connecting_keys & extent_free_keys
            if key in partials.get(left, []) or key in partials.get(right, [])
        }
        if span_keys:
            left_binds = bool(span_keys & set(partials.get(left, [])))
            right_binds = bool(span_keys & set(partials.get(right, [])))
            if left_binds != right_binds:
                return JoinType.LEFT_OUTER if left_binds else JoinType.RIGHT_OUTER
            # Both sides bind it. Two projections of ONE binding cover the same
            # subset, so the span carries no row intent between them and the
            # remaining keys decide the typing. PEER facts (sales and returns
            # each referencing their own slice of the group domain) each hold
            # rows the other lacks, and dropping either side's is a chasm, not
            # an extension: their typing stands whoever owns the extent.
            if span_binding_sources is not None and all(
                (sources := span_binding_sources.get(left, {}).get(key))
                and sources == span_binding_sources.get(right, {}).get(key)
                for key in span_keys
            ):
                typed_keys = all_connecting_keys - extent_free_keys
                left_is_partial = _has_any(typed_keys, left, partials)
                right_is_partial = _has_any(typed_keys, right, partials)

    # A partial side declares a SUBSET domain. Subset speaks to VALUES and
    # NULL is not a value, so partiality and nullability never interact here:
    # render preserving, and the narrowing pass restores direction exactly
    # when the superset side provably carries the key's full domain and the
    # subset side's NULLs have a null-safe partner.
    if left_is_partial or right_is_partial:
        # An AUTHORED relation key (`subset join` anchors, scoped coalescing
        # members) declares row intent of its own; its machinery types the
        # join and host inference must not override it (the rowset-enrichment
        # `subset join` preserves the anchor side, which hosting would flip).
        authored = bool(authored_keys and all_connecting_keys & authored_keys)
        # Preservation exists to keep extension rows, and those ride the HOST:
        # the side covering every `~`-licensed key the node emits (or the
        # node's grain when none are in play). When exactly one side hosts,
        # the other is a feeder whose unmatched rows carry no reachable
        # content; preserving it manufactures padded join keys the FINAL
        # merge then null-pairs across extension families. Symmetric or
        # absent hosting stays row-preserving, and so does a feeder carrying
        # VALUE nulls (`~?`) on the key: its NULL-keyed rows are real fact
        # rows equality would drop. Padding NULLs on the feeder are exactly
        # what the direction exists to shed, so only value nulls veto.
        if host_nodes is not None and not authored:
            left_is_host = left in host_nodes
            right_is_host = right in host_nodes
            if left_is_host != right_is_host:
                feeder = right if left_is_host else left
                if value_nullables is None or not _has_any(
                    all_connecting_keys, feeder, value_nullables
                ):
                    return JoinType.LEFT_OUTER if left_is_host else JoinType.RIGHT_OUTER
        partial_keys = {
            key
            for key in all_connecting_keys
            if key in partials.get(left, []) or key in partials.get(right, [])
        }
        # A `~` key the node never emits (not a visible output, no grain
        # component keyed by it) licenses no extension rows here. When the
        # pair is recognizably fact-to-dimension (one side's grain is the
        # connecting keys themselves) the dimension is a pure lookup whose
        # unmatched rows are grainless, so anchor the fact side. A demanded
        # key, ambiguous topology, or a value-null fact key stays
        # row-preserving.
        if (
            demanded_domains is not None
            and node_grains is not None
            and not authored
            and partial_keys
            and not partial_keys & demanded_domains
        ):
            left_grain = node_grains.get(left) or set()
            right_grain = node_grains.get(right) or set()
            left_is_dim = bool(left_grain) and left_grain <= all_connecting_keys
            right_is_dim = bool(right_grain) and right_grain <= all_connecting_keys
            if left_is_dim != right_is_dim:
                fact = left if right_is_dim else right
                if value_nullables is None or not _has_any(
                    all_connecting_keys, fact, value_nullables
                ):
                    return JoinType.LEFT_OUTER if right_is_dim else JoinType.RIGHT_OUTER
        return JoinType.FULL
    # A grouping-set NULL is padding, not a value: the subtotal/grand-total row
    # a ROLLUP/CUBE/GROUPING SETS emits has no counterpart on a side that does
    # not pad the same key, so null-safe equality has nothing to pair it with
    # and the INNER form below would silently drop it. Preserve toward the
    # padded side. Both sides padded is the ordinary case again: same grouping
    # sets, so the NULL groups do pair.
    if rollup_padded:
        left_pads = _has_any(all_connecting_keys, left, rollup_padded)
        right_pads = _has_any(all_connecting_keys, right, rollup_padded)
        if left_pads != right_pads:
            return JoinType.LEFT_OUTER if left_pads else JoinType.RIGHT_OUTER
    # Neither side partial: each binding declares the key's full domain
    # (EQUAL, mutual subset), whose narrowed form is INNER. NULL-key rows
    # must still never drop: when both sides are nullable the null-safe
    # equality (get_modifiers) pairs the NULL groups, and a nullable side
    # with no null-safe partner keeps the join preserving toward it.
    if left_is_nullable and right_is_nullable:
        # Null-pairing is only sound when the padded rows name the same thing.
        # When exactly one side carries the node's full grain (the host), its
        # padding is the grain-bearing extension family; the other side's
        # padding lacks grain columns entirely, so pairing the two invents
        # rows (extension-family cross products). Preserve the host and let
        # plain equality drop the feeder's padding. Authored relation keys
        # keep their own machinery's typing.
        if host_nodes is not None and not (
            authored_keys and all_connecting_keys & authored_keys
        ):
            left_is_host = left in host_nodes
            right_is_host = right in host_nodes
            if left_is_host != right_is_host:
                return JoinType.LEFT_OUTER if left_is_host else JoinType.RIGHT_OUTER
        # Grain-aligned sides both weakened their EQUAL-domain claims on the
        # merge axis itself, so INNER would drop each side's exclusive
        # members; preserve both and let the null-safe equality pair the
        # NULL groups.
        if _is_nullable_grain_aligned_merge(
            left, right, all_connecting_keys, node_grains, extent_nullables
        ):
            return JoinType.FULL
        return JoinType.INNER
    if left_is_nullable != right_is_nullable:
        # A nullable key weakens that side's EQUAL-domain claim to "some
        # subset, plus a NULL group". Between a fact and its lookup that
        # still directs the join: the other side is a feeder whose unmatched
        # rows carry no content. But when both sides are complete group-sets
        # at the merge grain and the nullability rides the merge axis, a
        # directional join would silently drop the non-nullable side's
        # exclusive members, the very rows its intact domain claim
        # promises. Preserve both, padded.
        if _is_nullable_grain_aligned_merge(
            left, right, all_connecting_keys, node_grains, extent_nullables
        ):
            return JoinType.FULL
        return JoinType.LEFT_OUTER if left_is_nullable else JoinType.RIGHT_OUTER
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


def ensure_content_preservation(
    joins: list[JoinOrderOutput], authored_axis_keys: set[str] | None = None
) -> None:
    authored_axis_keys = authored_axis_keys or set()
    for idx, review_join in enumerate(joins):
        predecessors = joins[:idx]
        if review_join.type == JoinType.FULL:
            continue
        has_prior_left = False
        has_prior_right = False
        review_keys: set[str] = set().union(set(), *review_join.keys.values())
        for pred in predecessors:
            on_pred_right = pred.right in review_join.lefts
            on_pred_left = any(x in review_join.lefts for x in pred.lefts)
            # A prior FULL padded rows into the accumulated stream; this join
            # must preserve the LEFT stream to keep them. Whether it must also
            # preserve its RIGHT relation depends on the FULL. An AUTHORED
            # axis FULL (query-scoped `full`/`union`/`subset` join) declares
            # row intent for both sides' content, facts hanging off either
            # side included, so both ways are preserved.
            # A partial-driven FULL preserves its right relation only when
            # this join is keyed ON the FULL's own spine: that key is
            # coalesced across both families, so the relation spans the
            # whole stream. A join keyed OFF a partial FULL's spine (one
            # side's non-key column, padded NULL for the other family) hangs
            # off a single family, and row-preservation there is a domain
            # license only get_join_type (a `~` partial / union declaration /
            # nullable key) can grant; upgrading to FULL would hand such
            # unlicensed dimensions extension rows.
            if pred.type == JoinType.FULL and (on_pred_right or on_pred_left):
                has_prior_left = True
                pred_keys: set[str] = set().union(set(), *pred.keys.values())
                if (review_keys and review_keys <= pred_keys) or (
                    pred_keys & authored_axis_keys
                ):
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
    authored_key_nodes: set[str] | None = None,
    rollup_padded: dict[str, list[str]] | None = None,
    host_nodes: set[str] | None = None,
    value_nullables: dict[str, list[str]] | None = None,
    demanded_domains: set[str] | None = None,
    node_grains: dict[str, set[str]] | None = None,
    authored_veto_keys: set[str] | None = None,
    extent_nullables: dict[str, list[str]] | None = None,
    extent_free_keys: set[str] | None = None,
    span_binding_sources: dict[str, dict[str, frozenset[str]]] | None = None,
) -> list[JoinOrderOutput]:
    """Greedily order the datasources into a join tree.

    Pick a pivot (shared concept), then absorb datasources that connect to the
    growing left set, scoring candidates by eligibility / partial / nullable
    status and breaking ties on estimated grain size (``_score_join_candidate``).
    Every choice point sorts its inputs, so the plan is deterministic across runs.

    Ordering is a heuristic for plan shape only; ``ensure_content_preservation``
    guarantees the result set regardless of the order chosen here.
    """
    grain_size = grain_size or {}
    datasources = sorted(x for x in g.nodes if x.startswith("ds~"))
    concepts = sorted(x for x in g.nodes if x.startswith("c~"))

    # A source is an anchor when it provides a scoped-LEFT anchor key as a
    # COMPLETE (non-partial) concept; optional sources are partial against it.
    # An anchor key is only ACTIVE when some present source is partial against
    # it: the boost exists to keep optional sources directional (LEFT, not
    # FULL), and with no optional side in the plan, seeding the tree on it
    # would just perturb unrelated joins.
    anchor_sources: frozenset[str] = frozenset()
    active_anchor_keys: set[str] = set()
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
    # An AUTHORED join key (scoped join / merge relation) pivots FIRST: its
    # equality is a semantic pairing contract, not a heuristic tree edge. If a
    # cheaper shared key seeds the tree instead, the sides pair on that key
    # alone and the authored predicate lands on a leaf dimension, where a
    # preserving join NULLs the dimension instead of un-pairing the rows.
    authored = authored_key_nodes or set()
    pivots = sorted(
        [x for x in pivot_map if len(pivot_map[x]) > 1],
        key=lambda x: (x not in authored, len(pivot_map[x]), len(x), x),
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
            deduped: list[tuple[str, set[str]]] = []

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
                    deduped.append((left_candidate, all_connecting_keys))
                    continue

                join_type = get_join_type(
                    left_candidate,
                    right,
                    partials,
                    nullables,
                    all_connecting_keys,
                    full_join_keys,
                    rollup_padded,
                    host_nodes,
                    value_nullables,
                    demanded_domains,
                    node_grains,
                    authored_veto_keys,
                    extent_nullables,
                    extent_free_keys,
                    span_binding_sources,
                )
                join_types.add(join_type)
                joinkeys[left_candidate] = all_connecting_keys

            final_join_type = reduce_join_types(join_types)

            # A FULL from get_join_type (a nullable-driven grain-aligned merge)
            # arrives after the dedup above ran; restore the dropped providers
            # so the ON clause coalesces across every left source, as
            # is_full_key pre-empts for registry keys. A single-source ON
            # misses rows that exist only on a previously-preserved side.
            if final_join_type == JoinType.FULL:
                for left_candidate, all_connecting_keys in deduped:
                    joinkeys[left_candidate] = all_connecting_keys

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
                        left=min(eligible_left),
                        right=ds,
                        type=JoinType.FULL,
                        keys={},
                    )
                )
            eligible_left.add(ds)

    authored_axis_keys = set(full_join_keys or set())
    if anchor_key_nodes:
        authored_axis_keys |= anchor_key_nodes
    if authored_key_nodes:
        authored_axis_keys |= authored_key_nodes
    ensure_content_preservation(output, authored_axis_keys)

    return output


def side_nullable(concept: BuildConcept, side: DataSource | None) -> bool:
    if side is None:
        return False
    # Intrinsic nullability: the concept's own definition can yield NULL (a
    # `?` column, a filtered value or aggregate, a no-else CASE) on any side
    # that carries it, regardless of that side's join structure.
    if concept.is_nullable:
        return True
    equivalent = concept.equivalent_addresses
    if any(equivalent & nc.equivalent_addresses for nc in side.nullable_concepts):
        return True
    # a side that COMPUTES the join key from nullable inputs yields NULL keys
    # too (`l_key + 1` is NULL wherever `l_key` is) even when the derived key
    # itself never got flagged
    if not propagates_argument_nulls(concept):
        return False
    args = {a.address for a in concept.concept_arguments}
    if not args:
        return False
    nullable_addrs: set[str] = set()
    for nc in side.nullable_concepts:
        nullable_addrs |= nc.equivalent_addresses
    return bool(args & nullable_addrs)


def _side_outputs(concept: BuildConcept, side: DataSource) -> bool:
    equivalent = concept.equivalent_addresses
    return any(equivalent & c.equivalent_addresses for c in side.output_concepts)


def nulls_are_values(
    concept: BuildConcept,
    side: DataSource,
    _seen: frozenset[tuple[str, int]] = frozenset(),
) -> bool:
    """Whether the NULLs this side carries for ``concept`` are VALUES (a `?`
    column, a nullable derivation, a nullable input to a null-propagating
    expression, a ROLLUP grouping key) rather than pure outer-join extension.

    Outer-join extension means absent: there is no row on that side, so no
    key. Pairing that against a real NULL group cross-joins the two."""
    if concept.is_nullable:
        return True
    # Argument chains can be mutually recursive; a repeat visit of the same
    # concept on the same source contributes nothing new.
    visit = (concept.address, id(side))
    if visit in _seen:
        return False
    seen = _seen | {visit}
    equivalent = concept.equivalent_addresses
    if isinstance(side, BuildDatasource):
        # Column-level `?` is the only value-NULL source on a physical table.
        return any(
            equivalent & nc.equivalent_addresses for nc in side.nullable_concepts
        )
    # A grouping-set NULL is padding too, but a twin-rollup partner pads the
    # same key, so it stays pairable here; get_join_type handles the mismatch.
    if equivalent & rollup_padded_addresses(side):
        return True
    carriers = [p for p in side.datasources if _side_outputs(concept, p)]
    if not carriers:
        # Nothing upstream to attribute the NULL to; stay conservative rather
        # than call an unexplained nullability extension.
        return True
    if any(nulls_are_values(concept, p, seen) for p in carriers):
        return True
    if not propagates_argument_nulls(concept):
        return False
    args = {a.address for a in concept.concept_arguments}
    if not args:
        return False
    return any(
        (args & nc.equivalent_addresses) and nulls_are_values(nc, side, seen)
        for nc in side.nullable_concepts
    )


def get_modifiers(
    left_concept: BuildConcept,
    right_concept: BuildConcept,
    left: DataSource | None,
    right: DataSource | None,
) -> list[Modifier]:
    """Use null-safe equality only when both exposed join keys can be NULL.

    Asymmetric padding is the exception: when one side's NULLs are outer-join
    extension (absence) and the other's are values, they name nothing in
    common and null-safe equality cross-joins them. Both sides extended is the
    ordinary case again: the padding shares provenance, so those rows pair."""
    if not (side_nullable(left_concept, left) and side_nullable(right_concept, right)):
        return []
    assert left is not None and right is not None
    if nulls_are_values(left_concept, left) != nulls_are_values(right_concept, right):
        return []
    return [Modifier.NULLABLE]


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


def partial_binding_sources(ds: DataSource, address: str) -> frozenset[str]:
    """Identifiers of the leaf tables whose own ``~`` column on ``address``
    makes this source partial against it.

    Two sides with the SAME set are two projections of one binding: whatever
    subset of the key they cover, they cover the same one, and neither holds
    members the other lacks. Different sets are peer facts (sales and returns
    each referencing their own slice of the group domain), and a join between
    them owes both sides' rows."""
    if isinstance(ds, BuildDatasource):
        return (
            frozenset({ds.identifier})
            if address in ds.column_level_partial_addresses
            else frozenset()
        )
    out: frozenset[str] = frozenset()
    for sub in ds.datasources:
        out |= partial_binding_sources(sub, address)
    return out


def deep_extent_free_spans(ds: DataSource) -> frozenset[str]:
    """Spans anything in this source's tree was built not to extend.

    Kept off the identifier (unlike a scan's own ``extent_free_spans``, which
    is identity): a wrapper does not change what it wraps, and folding the
    inherited set into wrapper names splits CTEs that should stay shared."""
    if not isinstance(ds, QueryDatasource):
        return frozenset()
    out = ds.extent_free_spans
    for sub in ds.datasources:
        out |= deep_extent_free_spans(sub)
    return out


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


def _is_authored_coalescing_pair(pair: ConceptPair, members: set[str]) -> bool:
    return pair.left.address in members or pair.right.address in members


def reduce_concept_pairs(
    pairs: list[ConceptPair],
    right_source: DataSource,
    join_type: JoinType = JoinType.INNER,
    domain_graph: DomainGraph | None = None,
) -> list[ConceptPair]:
    from trilogy.core.enums import Purpose

    left_keys = {
        pair.left.address for pair in pairs if pair.left.purpose == Purpose.KEY
    }
    right_keys = {
        pair.right.address for pair in pairs if pair.right.purpose == Purpose.KEY
    }
    grain_components = set(right_source.grain.components)
    # An authored coalescing-join (`full`/`union`) key member pairs by its own
    # physical column as part of the join's semantics. FD/grain implication
    # holds within one entity, not across independently-authored sides, so
    # inferring such a pair away changes which rows match.
    coalescing_members: set[str] = (
        domain_graph.coalescing_relation_members() if domain_graph else set()
    )
    # FD-closure pruning (docs/domain_graph_design.md step 4): a pair both of
    # whose sides are functionally determined by the SURVIVING joined keys is
    # redundant, since equality on the determinants implies equality here.
    # The closure sees what the local property check below cannot: transitive
    # dependencies and grain FDs carried through complete bindings. Greedy
    # over a working determinant set so mutually-dependent keys keep exactly
    # one pair; grain pairs are never pruned (the grain restriction below
    # relies on them).
    fd_pruned: set[int] = set()
    if domain_graph is not None and domain_graph.fd_edges:
        working_left = set(left_keys)
        working_right = set(right_keys)
        for index, pair in enumerate(pairs):
            left_addr, right_addr = pair.left.address, pair.right.address
            if right_addr in grain_components:
                continue
            if _is_authored_coalescing_pair(pair, coalescing_members):
                continue
            determinant_left = working_left - {left_addr}
            determinant_right = working_right - {right_addr}
            if not (determinant_left and determinant_right):
                continue
            if domain_graph.determines(
                determinant_left, left_addr
            ) and domain_graph.determines(determinant_right, right_addr):
                fd_pruned.add(index)
                working_left.discard(left_addr)
                working_right.discard(right_addr)
    final: list[ConceptPair] = []
    seen: set[tuple[str, str]] = set()
    is_outer = join_type in OUTER_JOIN_TYPES
    right_left_seen: dict[tuple[str, str], bool] = {}
    for index, pair in enumerate(pairs):
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
            and not _is_authored_coalescing_pair(pair, coalescing_members)
        ):
            continue
        if (
            pair.right.purpose == Purpose.PROPERTY
            and pair.right.keys
            and pair.right.keys.issubset(right_keys)
            and not _is_authored_coalescing_pair(pair, coalescing_members)
        ):
            continue
        if index in fd_pruned:
            continue

        seen.add(dedup_key)
        right_left_seen[rl_key] = right_left_seen.get(rl_key, False) or pair.is_partial
        final.append(pair)
    all_keys = {x.right.address for x in final}
    if right_source.grain.components and right_source.grain.components.issubset(
        all_keys
    ):
        return [
            x
            for x in final
            if x.right.address in right_source.grain.components
            or _is_authored_coalescing_pair(x, coalescing_members)
        ]

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


def _sole_projected_relation(ds: DataSource) -> str | None:
    """The identifier of the one relation this source only projects (and
    possibly dedups): a single parent, and nothing it computes itself changes
    the row set. Two such sources over the same relation hold the SAME rows
    under different columns, so pairing them is never a cross product no matter
    what their axes look like. An aggregating source is excluded: `sum(x) by k1`
    beside `sum(y) by k2` over one scan is an authored fan-out, not a lost key.
    """
    if not isinstance(ds, QueryDatasource) or len(ds.datasources) != 1:
        return None
    if any(
        concept.purpose == Purpose.METRIC or concept.derivation == Derivation.AGGREGATE
        for concept in ds.output_concepts
    ):
        return None
    return ds.datasources[0].identifier


def _row_independent(ds: DataSource) -> bool:
    """True when cross-joining this source cannot fan out row counts: no
    grain, an authored literal fan-out, or every column it exposes is
    single-row (the `utility.calculate_graph_relevance` rule: a single-row
    concept can always be crossjoined)."""
    if not ds.grain.components:
        return True
    # An UNNEST source here is the standalone literal flavor (`unnest([1,2,3])
    # as value` beside an unrelated scan): its fan-out is authored by the
    # query, not a lost join key. A row-correlated unnest rides an UnnestJoin,
    # never a keyless BaseJoin.
    if isinstance(ds, QueryDatasource) and ds.source_type == SourceType.UNNEST:
        return True
    outputs = ds.output_concepts
    if bool(outputs) and all(c.granularity == Granularity.SINGLE_ROW for c in outputs):
        return True
    # A global-aggregate scalar carries a SELF-grain (grain = the metric
    # itself) rather than an empty grain; with no keys there is no row axis to
    # pair on and the cross join is the plan (the `calculate_graph_relevance`
    # metric rule). A KEYED metric is a per-group aggregate with a real axis,
    # so it stays subject to the guard.
    output_by_addr = {c.address: c for c in outputs}
    return all(
        (c := output_by_addr.get(component)) is not None
        and c.purpose == Purpose.METRIC
        and not c.keys
        for component in ds.grain.components
    )


def _raise_if_keyless_row_bearing_join(
    joins: list[JoinOrderOutput],
    ds_node_map: dict[str, DataSource],
    canonical: dict[str, str],
    rollup_padded_addresses: frozenset[str],
    environment: BuildEnvironment | None,
) -> None:
    """A keyless join between row-bearing sources is a planner bug when the
    sides SHARE a join axis the planner failed to use, or when they are two
    projections of ONE relation (``_sole_projected_relation``) and so hold the
    same rows however their axes look. The axis test is FD-aware: one side's
    outputs (hidden included, since hiding is how an axis gets lost) closed
    over concept ``keys``, pseudonyms and rowset content, intersected with the
    other side's direct addresses, after canonicalization. Axis-DISJOINT
    row-bearing sides off DIFFERENT relations cross-join legitimately
    (selecting an aggregate without its grouping key is an authored fan-out),
    as does a row-independent side (constant / global-aggregate scalar).
    ROLLUP-padded keys are excluded: subtotal rows NULL them, so consumers
    deliberately avoid joining on them.

    Hard failure by design: silently shipping the cartesian is the worse
    outcome. When this fires, the fix belongs upstream in the demand/contract
    passes that let the axis go missing, not in relaxing the check."""

    # Both axis views are pure functions of the node and every keyless join
    # re-asks them for the same sources; memoize so the guard stays
    # proportional to the tree rather than to joins x tree.
    direct_cache: dict[str, frozenset[str]] = {}
    closure_cache: dict[str, frozenset[str]] = {}
    independent_cache: dict[str, bool] = {}

    def _canon(addr: str) -> str:
        content = resolve_rowset_content_address(addr, environment)
        return canonical.get(content, content)

    def row_independent(node: str) -> bool:
        if node not in independent_cache:
            independent_cache[node] = _row_independent(ds_node_map[node])
        return independent_cache[node]

    def direct_axis(node: str) -> frozenset[str]:
        """Addresses this source can actually be JOINED ON: the columns it
        projects. Hidden outputs count (rendered, just masked). A grain
        component the source never emits does NOT count: you cannot join on a
        column that isn't there."""
        if node in direct_cache:
            return direct_cache[node]
        ds = ds_node_map[node]
        addrs: set[str] = set()
        for concept in ds.output_concepts:
            addrs.add(concept.address)
            # Pseudonyms are same-value addresses (an alias output IS its
            # source column): the axis a sibling renders under the original
            # name.
            addrs.update(concept.pseudonyms)
        result = frozenset({_canon(a) for a in addrs}) - rollup_padded_addresses
        direct_cache[node] = result
        return result

    def key_closure(node: str) -> frozenset[str]:
        """Direct axis plus everything reachable through concept ``keys``,
        pseudonyms, and rowset content, to fixpoint: the addresses whose rows
        FD-determine this source's PROJECTED values. A rename chain can hide
        its key several environment hops deep. Seeded from outputs only, for
        the same reason as `direct_axis`."""
        if node in closure_cache:
            return closure_cache[node]
        ds = ds_node_map[node]
        frontier: set[str] = set()
        for concept in ds.output_concepts:
            frontier.add(concept.address)
            frontier.update(concept.pseudonyms)
            frontier.update(concept.keys or set())
        closure: set[str] = set()
        while frontier:
            addr = frontier.pop()
            if addr in closure:
                continue
            closure.add(addr)
            if environment is None:
                continue
            concept_ref = environment.concepts.get(
                addr
            ) or environment.alias_origin_lookup.get(addr)
            if concept_ref is None:
                continue
            frontier.update(concept_ref.keys or set())
            frontier.update(concept_ref.pseudonyms)
            if isinstance(concept_ref.lineage, BuildRowsetItem):
                frontier.add(concept_ref.lineage.content.address)
        result = frozenset({_canon(a) for a in closure}) - rollup_padded_addresses
        closure_cache[node] = result
        return result

    tree: set[str] = set()
    for j in joins:
        if j.left is not None:
            tree.add(j.left)
        tree.update(j.lefts)
        if not j.keys and not row_independent(j.right):
            right_direct = direct_axis(j.right)
            right_closure = key_closure(j.right)
            right_relation = _sole_projected_relation(ds_node_map[j.right])
            offenders = sorted(
                d
                for d in tree
                if not row_independent(d)
                and (
                    right_closure & direct_axis(d)
                    or right_direct & key_closure(d)
                    # Axis-disjoint but the same rows: two projections of one
                    # relation are correlated by construction, so the shared
                    # axis test never sees them (a union-TVF arm's key and
                    # value close over different domains).
                    or (
                        right_relation is not None
                        and right_relation == _sole_projected_relation(ds_node_map[d])
                    )
                )
            )
            if offenders:
                raise UnresolvableQueryException(
                    "Planner emitted a keyless join between row-bearing sources "
                    "that share a join axis or one source relation: "
                    f"{ds_node_map[j.right].identifier} onto "
                    f"{', '.join(ds_node_map[d].identifier for d in offenders)}. "
                    "This would render as a cross join (ON 1=1) and fan out; "
                    "the join axis was lost upstream. This is a planner bug."
                )
        tree.add(j.right)


def single_row_source(ds: DataSource) -> bool:
    """True when ``ds`` provably emits exactly one row: an ungrouped
    aggregate computing every output here (a passed-through column would be
    a group key), with no LIMIT, no ROLLUP and at most a scalar WHERE (which
    filters the aggregate's input, never its single output row), or an
    unfiltered, unjoined projection over one such source. A HAVING can
    delete that row, so a non-scalar condition disqualifies."""
    if not isinstance(ds, QueryDatasource):
        return False
    if ds.source_type in (SourceType.UNION, SourceType.RECURSIVE, SourceType.UNNEST):
        return False
    if ds.limit is not None or ds.rollup_concepts:
        return False
    if not ds.group_required:
        return (
            not ds.joins
            and ds.condition is None
            and len(ds.datasources) == 1
            and single_row_source(ds.datasources[0])
        )
    outputs = ds.output_concepts
    if any(ds.source_map.get(c.address) for c in outputs):
        return False
    output_by_addr = {c.address: c for c in outputs}
    if not all(
        (c := output_by_addr.get(component)) is not None
        and c.purpose == Purpose.METRIC
        and not c.keys
        for component in ds.grain.components
    ):
        return False
    if not any(get_grouped_aggregate_wrapper(c) is not None for c in outputs):
        return False
    if ds.condition is None:
        return True
    materialized = {address for address, v in ds.source_map.items() if v}
    return is_scalar_condition(ds.condition, materialized=materialized)


def _narrowed_keyless_type(left_has_rows: bool, right_has_rows: bool) -> JoinType:
    if left_has_rows and right_has_rows:
        return JoinType.INNER
    if left_has_rows:
        return JoinType.LEFT_OUTER
    if right_has_rows:
        return JoinType.RIGHT_OUTER
    return JoinType.FULL


def narrow_keyless_joins(joins: list[BaseJoin | UnnestJoin]) -> None:
    """A keyless FULL (``ON 1=1``) pairs every row with every row, so FULL
    only differs from INNER when a side is EMPTY. Walk the joins in order
    carrying whether the relation built so far provably has rows. While only
    keyless FULL joins precede, a join's explicit left is part of that
    relation and its rows count; a keyed or unnest join can empty the
    relation, so after one only the keyless right sides accumulate."""
    left_has_rows = False
    keyed_seen = False
    for join in joins:
        if (
            not isinstance(join, BaseJoin)
            or join.join_type != JoinType.FULL
            or join.concept_pairs
            or join.concepts
        ):
            left_has_rows = False
            keyed_seen = True
            continue
        if join.left_datasource is not None and not keyed_seen:
            left_has_rows = left_has_rows or single_row_source(join.left_datasource)
        right_has_rows = single_row_source(join.right_datasource)
        join.join_type = _narrowed_keyless_type(left_has_rows, right_has_rows)
        left_has_rows = left_has_rows or right_has_rows


def _padding_sources(
    side: DataSource, keys: set[str], canon: Callable[[str], str]
) -> set[str]:
    """Identifiers of the sources at or above `side` whose own rows carry the
    key as join-analysis padding. A leaf datasource never pads: a NULL in its
    column is a value, which the caller has already exempted."""
    found: set[str] = set()
    if not isinstance(side, QueryDatasource):
        return found
    if keys & {canon(c.address) for c in side.nullable_concepts}:
        found.add(side.identifier)
    for parent in side.datasources:
        found |= _padding_sources(parent, keys, canon)
    return found


def get_node_joins(
    datasources: list[DataSource],
    environment: BuildEnvironment,
    host_grain: set[str] | None = None,
    demanded_domains: set[str] | None = None,
    extent_free_spans: frozenset[str] = frozenset(),
) -> list[BaseJoin]:
    from trilogy.core import graph as nx

    canonical = build_canonical_address_map(datasources, environment)

    def canon_node(address: str) -> str:
        return f"c~{canonical.get(address, address)}"

    graph = nx.Graph()
    partials: dict[str, list[str]] = {}
    nullables: dict[str, list[str]] = {}
    extent_nullables: dict[str, list[str]] = {}
    extent_memo: dict[int, set[str]] = {}
    pad_memo: dict[int, set[str]] = {}
    grain_size: dict[str, int] = {}
    value_nullables: dict[str, list[str]] = {}
    ds_node_map: dict[str, DataSource] = {}
    ds_concept_map: dict[tuple[str, str], BuildConcept] = {}
    rollup_padded: dict[str, list[str]] = {}

    for datasource in datasources:
        ds_node = f"ds~{datasource.identifier}"
        ds_node_map[ds_node] = datasource
        grain_size[ds_node] = _estimated_grain_size(datasource)
        graph.add_node(ds_node, type=NodeType.NODE)
        partial_nodes = {
            canon_node(a) for a in _collect_deep_partial_addresses(datasource)
        }
        # A LEFT scoped join on a derived key has no datasource column binding
        # to carry Modifier.PARTIAL. The merge keeps that key as a distinct
        # output present ONLY on the partial side (the complete side outputs
        # the canonical), so intersecting outputs with scoped_partial_derived
        # marks exactly the partial side. Root/rowset partial keys carry
        # partiality through the column-partial / rowset machinery and are
        # excluded, since a rowset key also survives as a distinct output.
        if environment.scoped_partial_derived:
            partial_nodes |= {
                canon_node(c.address)
                for c in datasource.output_concepts
                if c.address in environment.scoped_partial_derived
            }
        nullable_nodes = {canon_node(c.address) for c in datasource.nullable_concepts}
        if extent_free_spans:
            nullable_nodes -= {
                canon_node(a)
                for a in extension_padded_addresses(
                    datasource, extent_free_spans, pad_memo
                )
            }
        padded_nodes = {canon_node(a) for a in rollup_padded_addresses(datasource)}
        p_list: list[str] = []
        n_list: list[str] = []
        r_list: list[str] = []
        v_list: list[str] = []
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
                if node not in v_list and nulls_are_values(concept, datasource):
                    v_list.append(node)
            if node in padded_nodes and node not in r_list:
                r_list.append(node)
        partials[ds_node] = p_list
        nullables[ds_node] = n_list
        rollup_padded[ds_node] = r_list
        value_nullables[ds_node] = v_list
        extent_addrs = extent_null_addresses(datasource, extent_memo)
        extent_nullables[ds_node] = [
            node for node in n_list if node.removeprefix("c~") in extent_addrs
        ]

    # Canonical keys of query-scoped FULL joins (EQUAL/∦ declared edges),
    # mapped into graph concept nodes.
    full_join_keys = {
        canon_node(a) for a in environment.domain_graph.outer_relation_keys()
    }
    # Anchor-key nodes of query-scoped LEFT joins (declared-subset anchors): the
    # join tree bases on the complete source providing one so co-anchored
    # optional sources stay LEFT.
    anchor_key_nodes = {
        canon_node(a) for a in environment.domain_graph.left_anchor_keys()
    }
    # Canonical keys of ROOT-member authored join groups: pivot the join tree
    # on these first so the authored equality is the pairing between the sides
    # regardless of cheaper shared-key edges. Derived/rowset-keyed groups are
    # excluded; their ordering rides the rowset exposure machinery. Local
    # import: common.py imports nodes.merge_node, which imports this module.
    from trilogy.core.processing.node_generators.common import (
        authored_join_pair_candidates,
    )

    authored_key_nodes = {
        canon_node(pair.canonical.address)
        for pair in authored_join_pair_candidates(environment)
    }
    extent_free_key_nodes = {canon_node(address) for address in extent_free_spans}
    span_binding_sources = {
        ds_node: {
            canon_node(address): partial_binding_sources(datasource, address)
            for address in extent_free_spans
        }
        for ds_node, datasource in ds_node_map.items()
    }
    host_nodes: set[str] | None = None
    if host_grain:
        host_canon = {canon_node(a) for a in host_grain}
        # Hosting a domain requires binding it COMPLETELY: a side carrying a
        # `~` key only partially (the fact's FK column) exposes the address
        # but not the domain, so it can never out-host the preserved span.
        # Own-level marks, not the deep collection: a span that completed a
        # key against its dimension clears its own mark while the raw fact
        # scan below it keeps one.
        host_nodes = {
            ds_node
            for ds_node, datasource in ds_node_map.items()
            if host_canon
            <= (
                {canon_node(c.address) for c in datasource.output_concepts}
                - {canon_node(c.address) for c in datasource.partial_concepts}
            )
        }
    # Keys whose join typing is owned by an authored relation (query-scoped
    # subset/coalescing joins, declared anchors): host/dim direction inference
    # stands down on these.
    authored_veto_keys = (
        {canon_node(a) for a in environment.scoped_partial_derived}
        | {
            canon_node(a)
            for canonical, members in environment.scoped_join_key_groups.items()
            for a in (canonical, *members)
        }
        | anchor_key_nodes
        | authored_key_nodes
    )
    demanded_nodes: set[str] | None = None
    node_grains: dict[str, set[str]] | None = None
    if demanded_domains is not None:
        demanded_nodes = {canon_node(a) for a in demanded_domains}
        node_grains = {
            ds_node: {canon_node(a) for a in datasource.grain.components}
            for ds_node, datasource in ds_node_map.items()
        }
    joins = resolve_join_order_v2(
        graph,
        partials=partials,
        nullables=nullables,
        grain_size=grain_size,
        full_join_keys=full_join_keys,
        anchor_key_nodes=anchor_key_nodes,
        authored_key_nodes=authored_key_nodes,
        rollup_padded=rollup_padded,
        host_nodes=host_nodes,
        value_nullables=value_nullables,
        demanded_domains=demanded_nodes,
        node_grains=node_grains,
        authored_veto_keys=authored_veto_keys,
        extent_nullables=extent_nullables,
        extent_free_keys=extent_free_key_nodes,
        span_binding_sources=span_binding_sources,
    )
    _raise_if_keyless_row_bearing_join(
        joins,
        ds_node_map,
        canonical,
        frozenset(
            node.removeprefix("c~")
            for nodes in rollup_padded.values()
            for node in nodes
        ),
        environment,
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
                    # sorted: v is a set, and reduce_concept_pairs prunes
                    # greedily in input order, so unordered iteration would
                    # make the surviving pair set vary run to run.
                    for concept in sorted(v)
                ],
                ds_node_map[j.right],
                j.type,
                domain_graph=environment.domain_graph,
            ),
        )
        for j in joins
    ]
