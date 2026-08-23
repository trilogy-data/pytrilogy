from enum import Enum
from typing import TYPE_CHECKING

from trilogy.core.enums import (
    Derivation,
    SourceType,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildConceptArgs,
    BuildRowsetItem,
    BuildWindowItem,
    nonstandard_grouping_lineage,
)
from trilogy.core.models.execute import (
    CTE,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    SENSITIVE_DERIVATIONS,
    consumed_parent_column,
    is_grouped_cte,
    is_sole_consumer,
    rebind_rename_to_consumed,
    rename_reference,
    repoint_consumers,
)
from trilogy.core.processing.condition_utility import merge_conditions_and_dedup

if TYPE_CHECKING:
    from trilogy.core.domain_graph import DomainGraph


class MergeMode(Enum):
    AGGREGATE = "aggregate"
    WINDOW = "window"
    BASIC = "basic"
    # A CTE that only re-projects a subset of its single parent's columns — no
    # local computation, no WHERE, no join, no regroup. It adds nothing the parent
    # can't render, so it folds away entirely (the FINAL merge reads the parent
    # directly). v4's per-contributor projection emits these (q81's dim scan ->
    # a bare 16-col projection of the customer⋈customer_address CTE).
    PASSTHROUGH = "passthrough"


def is_passthrough_projection(cte: CTE) -> bool:
    if cte.group_to_grain or cte.joins or cte.condition is not None:
        return False
    if cte.source.source_type in (
        SourceType.GROUP,
        SourceType.WINDOW,
        SourceType.UNNEST,
        SourceType.RECURSIVE,
        SourceType.SUBSELECT,
        SourceType.UNION,
    ):
        return False
    # A column with a non-empty source_map entry is pulled from upstream as a
    # plain column — safe to pass through whatever its derivation. Only a
    # column computed LOCALLY (inline aggregate/window render) disqualifies
    # the fold (usa_names: a stripped condition wrapper still carrying its
    # now-unused aggregate virts as passthrough columns).
    return all(
        bool(cte.source_map.get(concept.address))
        or (
            concept.derivation not in SENSITIVE_DERIVATIONS
            and concept.derivation != Derivation.AGGREGATE
            and not isinstance(
                concept.lineage, (BuildAggregateWrapper, BuildWindowItem)
            )
        )
        for concept in cte.output_columns
    )


def renders_off_parent_output(
    column: BuildConcept, cte: CTE, parent: CTE, parent_outputs: set[str]
) -> bool:
    """True when `column` is a rename the merged parent will render
    IDENTICALLY to the child's render today: the child renders it as a bare
    parent-column reference and `rename_rederivation_preserves_value` proves
    the parent-context re-derivation emits that same column's expression.
    A parent-side source_map binding for the column's own address is
    disqualifying — it wins over the lineage render and can name a RAW column
    where the child read a derived one. Lineage-less pseudonym twins never
    fold: candidate recovery re-runs in the parent's richer binding context
    and can settle on a different twin than it did in the child."""
    if parent.source_map.get(column.address):
        return False
    if rename_reference(column) is None:
        return False
    return consumed_parent_column(column, cte, parent) is not None


def passthrough_renders_from_parent(cte: CTE, parent: CTE) -> bool:
    """Every output the parent doesn't already expose is a rename of one it does."""
    parent_outputs = {c.address for c in parent.output_columns}
    for column in cte.output_columns:
        if column.address in parent_outputs:
            continue
        if not renders_off_parent_output(column, cte, parent, parent_outputs):
            return False
    return True


def has_unsafe_derivations(cte: CTE) -> bool:
    """Check if a CTE derives any concepts that can't be merged into an aggregate."""
    for concept in cte.output_columns:
        if concept.derivation in SENSITIVE_DERIVATIONS:
            return True
        if isinstance(concept.lineage, BuildWindowItem):
            return True
    return False


def has_basic_derivation(cte: CTE) -> bool:
    return any(concept.derivation == Derivation.BASIC for concept in cte.output_columns)


def produces_unbound_rowset(cte: CTE, domain_graph: "DomainGraph | None") -> bool:
    """True if `cte` is a rowset node whose rename output backs an UNBOUND
    merge/scoped canonical key — a key with no datasource binding, renderable
    only through this rename (`stages.stage` -> `local.step`). Collapsing folds
    the rename away and strands the key (INVALID_REFERENCE_BUG), so keep the
    boundary intact.

    A rowset output that is unaliased (no pseudonyms) or backs a BOUND key
    (`agg.item_sk` -> `ss.item.id`, resolvable from its own datasource columns)
    is safe to fold — the reference survives via the bound column. When no
    domain graph is available (direct construction), fall back to treating any
    aliased rowset output as unbound (conservative: correctness over size)."""
    for column in cte.output_columns:
        if not isinstance(column.lineage, BuildRowsetItem):
            continue
        for pseudonym in column.pseudonyms:
            if domain_graph is None or not domain_graph.binding_sources(pseudonym):
                return True
    return False


def unbound_rowset_blocks_merge(
    cte: CTE,
    parent: CTE,
    merge_mode: MergeMode,
    domain_graph: "DomainGraph | None",
) -> bool:
    if merge_mode == MergeMode.PASSTHROUGH and is_grouped_cte(parent):
        return False
    if merge_mode in (MergeMode.PASSTHROUGH, MergeMode.BASIC):
        # An identity fold: every child output address is one the parent
        # already outputs, so the merge removes no rename — every unbound key
        # stays renderable through the parent's own column. Without this the
        # guard permanently pins TVF union-arm layers (`SELECT x as x` over
        # synthetic ___tvf_arm_* concepts, whose pseudonyms are never bound).
        # Those layers classify as BASIC (their outputs carry BASIC/CONSTANT
        # derivations), so the carve-out must cover BASIC too, not just
        # PASSTHROUGH.
        parent_outputs = parent.output_lcl
        if all(c.address in parent_outputs for c in cte.output_columns):
            return False
    return produces_unbound_rowset(cte, domain_graph) or produces_unbound_rowset(
        parent, domain_graph
    )


def grouped_unbound_passthrough_should_wait(
    cte: CTE, domain_graph: "DomainGraph | None"
) -> bool:
    if get_merge_mode(cte) != MergeMode.PASSTHROUGH:
        return False
    parents = cte.dependency_nodes()
    if len(parents) != 1 or not isinstance(parents[0], CTE):
        return False
    parent = parents[0]
    return (
        is_grouped_cte(parent)
        and get_merge_mode(parent) == MergeMode.AGGREGATE
        and bool(parent.dependency_nodes())
        and (
            produces_unbound_rowset(cte, domain_graph)
            or produces_unbound_rowset(parent, domain_graph)
        )
    )


def get_merge_mode(cte: CTE) -> MergeMode | None:
    if cte.group_to_grain or cte.source.source_type == SourceType.GROUP:
        return MergeMode.AGGREGATE
    if cte.source.source_type == SourceType.WINDOW:
        return MergeMode.WINDOW
    if has_basic_derivation(cte):
        return MergeMode.BASIC
    if is_passthrough_projection(cte):
        return MergeMode.PASSTHROUGH
    return None


def lineage_contains_aggregate(concept: BuildConcept, seen: set[str]) -> bool:
    """True if `concept`'s lineage tree contains an aggregate anywhere — directly
    (`sum(x)`), or wrapped in a filter/function/rowset (`sum(x) ? cond`,
    `coalesce(sum(x), 0)`). Used to detect a parent column that renders an
    aggregate inline; folding it into an AGGREGATE child's `sum(...)` would
    nest aggregates ("aggregate function calls cannot be nested")."""
    if concept.address in seen:
        return False
    seen.add(concept.address)
    lineage = concept.lineage
    if isinstance(lineage, BuildAggregateWrapper):
        return True
    if isinstance(lineage, BuildConceptArgs):
        return any(
            lineage_contains_aggregate(arg, seen) for arg in lineage.concept_arguments
        )
    return False


def has_nonstandard_aggregate_grouping(concept: BuildConcept) -> bool:
    if isinstance(concept.lineage, BuildRowsetItem):
        return has_nonstandard_aggregate_grouping(concept.lineage.content)
    return nonstandard_grouping_lineage(concept) is not None


def parent_is_ineligible(parent: CTE, merge_mode: MergeMode) -> bool:
    if merge_mode == MergeMode.PASSTHROUGH:
        # A passthrough collapses into any single parent that renders its columns
        # (verified in `optimize`); it adds no computation, WHERE, or regroup, so
        # no parent shape is unsafe. UNION/RECURSIVE parents are excluded by the
        # isinstance guard in `optimize`.
        return False
    if merge_mode == MergeMode.AGGREGATE:
        return parent.group_to_grain or parent.source.source_type in (
            SourceType.GROUP,
            SourceType.WINDOW,
            SourceType.SUBSELECT,
        )
    if merge_mode == MergeMode.WINDOW:
        return (
            parent.group_to_grain
            or parent.condition is not None
            or parent.source.source_type
            in (
                SourceType.GROUP,
                SourceType.FILTER,
                SourceType.SUBSELECT,
                SourceType.WINDOW,
            )
        )
    # BASIC: a scalar projection over a GROUP parent folds into the GROUP's
    # SELECT list (`sum(a)/sum(b)` rendered alongside the GROUP BY is valid SQL,
    # giving a single-CTE shape). This is only sound for the safe subset
    # gated by `basic_fold_into_group_is_safe` (checked separately in `optimize`),
    # never blanket. WINDOW/SUBSELECT/UNNEST parents still can't absorb a
    # downstream row projection without changing row shape.
    return parent.source.source_type in (
        SourceType.WINDOW,
        SourceType.SUBSELECT,
        SourceType.UNNEST,
    )


def basic_fold_into_group_is_safe(parent: CTE, cte: CTE) -> bool:
    """Gate the BASIC-into-GROUP fold to the provably row-preserving subset.

    Folding a row projection into a GROUP parent is only sound when it cannot
    alter the parent's grouping. GROUP BY is rendered solely from
    ``parent.group_concepts`` (which `apply_child_merge`'s BASIC path leaves
    untouched), and a BASIC-merge child is row-preserving by construction (it is
    not ``group_to_grain`` and its source is not GROUP, so it never regroups).
    The remaining requirement is that every output the child *derives locally*
    is a scalar row projection over the parent's ``{grain keys, aggregates}``:

    - aggregate / window / unnest / recursive columns the child computes *anew*
      can't ride in the parent's GROUP BY select, so they disqualify the fold;
    - the same column kinds merely *passed through* from the GROUP parent are
      fine (already computed there) -- e.g. a dimension projection that joins a
      label onto a grouped fact carries the parent's aggregates straight through.

    The caller has already established the GROUP is the child's sole parent, so
    every child input necessarily resolves to a parent output."""
    parent_outputs = parent.output_lcl
    for column in cte.output_columns:
        if column.address in parent_outputs:
            continue  # passthrough of a parent grain key / aggregate -- safe
        if (
            column.derivation in SENSITIVE_DERIVATIONS
            or column.derivation == Derivation.AGGREGATE
        ):
            return False
        if isinstance(column.lineage, (BuildAggregateWrapper, BuildWindowItem)):
            return False
    return True


def child_has_merge_blockers(cte: CTE, merge_mode: MergeMode) -> bool:
    if merge_mode == MergeMode.WINDOW and cte.condition is not None:
        return True
    if merge_mode == MergeMode.BASIC and cte.condition is not None:
        return True
    if merge_mode == MergeMode.AGGREGATE:
        return any(
            has_nonstandard_aggregate_grouping(concept)
            for concept in cte.output_columns
        )
    return False


def apply_child_merge(parent: CTE, cte: CTE, merge_mode: MergeMode) -> None:
    for column in cte.output_columns:
        if column not in parent.output_columns:
            # A rename consumed as a bare parent-column reference is pinned to
            # the consumed OBJECT so its render can never drift to another
            # same-address variant (see rebind_rename_to_consumed).
            consumed = (
                consumed_parent_column(column, cte, parent)
                if rename_reference(column) is not None
                else None
            )
            if consumed is not None:
                column = rebind_rename_to_consumed(column, consumed)
            parent.output_columns.append(column)

    # Carry the child's nullability metadata: the merged CTE now exposes the
    # child's columns (rename addresses included), and an under-reported
    # nullable set lets SimplifyNullSafeJoins falsely prove a key non-null and
    # downgrade IS NOT DISTINCT FROM to `=`, dropping NULL-keyed groups.
    nullable_addresses = {c.address for c in parent.nullable_concepts}
    for column in cte.nullable_concepts:
        if column.address not in nullable_addresses:
            parent.nullable_concepts.append(column)

    # Carry the child's existence references: an `IN (<set>)` rendered by the
    # child resolves its set columns through existence_source_map; dropping
    # those entries strands the membership (INVALID_REFERENCE on every set
    # component) and lets the feeder CTE be pruned as unreferenced.
    for address, sources in cte.existence_source_map.items():
        if address not in parent.existence_source_map:
            parent.existence_source_map[address] = sources

    # AND-combine the child's WHERE into the parent — for AGGREGATE merges the
    # child sits BELOW the aggregate (its condition is the pre-aggregation
    # WHERE), and the parent becomes the aggregate after the merge, so its
    # WHERE has to carry the predicate forward. (WINDOW/BASIC modes are blocked
    # upstream by `child_has_merge_blockers` when the child has a condition.)
    # Dedup on AND-atoms so a chain of merges (or a predicate already carried
    # by the parent) can't re-stamp it into `H AND H AND H` — q31's HAVING.
    if cte.condition is not None:
        parent.condition = (
            merge_conditions_and_dedup(cte.condition, parent.condition)
            if parent.condition is not None
            else cte.condition
        )

    # The child's row limit (and the ORDER BY it selects under) applies to the
    # merged CTE's output — LIMIT is the last logical operation of a SELECT,
    # so absorbing the parent's shape below it is row-identical. (The caller
    # rejects limited PARENTS, the direction that would cross the boundary.)
    if cte.limit is not None:
        parent.limit = cte.limit
        parent.order_by = cte.order_by

    if merge_mode == MergeMode.AGGREGATE:
        # Aggregate merge: keep only columns the child exposes (grouping keys +
        # aggregate outputs). Everything else is rolled up.
        parent.output_columns = [
            column
            for column in parent.output_columns
            if column.address in cte.output_lcl
        ]
        parent.group_to_grain = True
        parent.grain = cte.grain
    elif merge_mode == MergeMode.WINDOW:
        # Window merge: the parent's intermediate columns may still be referenced
        # by window expressions (e.g. inlined CASE branches or unmaterialized
        # aggregates). Don't prune — just extend.
        parent.source.source_type = SourceType.WINDOW
    # BASIC / PASSTHROUGH merge: keep parent's source_type; the child's columns
    # render alongside parent's outputs. HideUnusedConcepts handles pruning later.


def destroys_subset_anchor_boundary(
    cte: CTE, parent: CTE, domain_graph: "DomainGraph | None"
) -> bool:
    """True when folding `cte` into `parent` would erase a rowset definition
    boundary that a declared SUBSET relation narrows against.

    `_rowset_definition_boundary` (value_set_join_upgrade) proves a rowset
    output complete-by-construction only at a CTE that has CTE parents which
    do not carry the output's own address. Folding the boundary projection
    into a datasource-bound scan removes that structure and the FULL join a
    `subset join x = rs.k` should narrow to the anchored LEFT stays FULL.
    Only outputs that are the SUPERSET target of a declared subset edge feed
    that proof, so only those block the fold. With no domain graph the edge
    set is unknowable — treat any rowset output as potentially targeted."""
    from trilogy.core.domain_graph import DomainRelation, EdgeProvenance

    targets: set[str] | None = None
    if domain_graph is not None:
        targets = {
            e.target
            for e in domain_graph.edges
            if e.relation is DomainRelation.SUBSET
            and e.provenance is EdgeProvenance.DECLARED
        }
    parent_preserves = bool(parent.parent_ctes)
    for column in cte.output_columns:
        if column.derivation != Derivation.ROWSET:
            continue
        if targets is not None and column.address not in targets:
            continue
        if not parent_preserves or any(
            out.address == column.address
            for grandparent in parent.parent_ctes
            if isinstance(grandparent, CTE)
            for out in grandparent.output_columns
        ):
            return True
    return False


class CollapseSingleParent(OptimizationRule):
    """Collapse a child CTE into its single parent by folding the parent's
    datasources and conditions into the child's shape.

    Handles three merge modes (AGGREGATE, WINDOW, BASIC). When the child CTE
    has a single parent that:
    1. Is only used by this child (no other children)
    2. Doesn't derive window functions or other unsafe derivations
    3. Has compatible datasources

    We can merge the parent's datasources and conditions directly into the
    child, eliminating an unnecessary subquery.
    """

    def __init__(
        self,
        domain_graph: "DomainGraph | None" = None,
        passthrough_only: bool = False,
    ) -> None:
        super().__init__()
        self.domain_graph = domain_graph
        # A bare passthrough (single parent, no local compute/WHERE/regroup) is
        # pure noise regardless of aggregate merging, so it is collapsed even
        # when the merge_aggregate config (which gates the full rule) is off --
        # e.g. to clean up a projection that predicate pushdown just reduced to
        # a passthrough by relocating its WHERE onto the parent scan.
        self.passthrough_only = passthrough_only

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, (UnionCTE, RecursiveCTE)):
            return False, None

        if cte.joins:
            return False, None

        merge_mode = get_merge_mode(cte)
        if merge_mode is None:
            return False, None

        if self.passthrough_only and merge_mode != MergeMode.PASSTHROUGH:
            return False, None

        if child_has_merge_blockers(cte, merge_mode):
            self.debug(f"CTE {cte.name} has child-specific merge blockers, skipping")
            return False, None

        parents = cte.dependency_nodes()
        if not parents:
            return False, None

        # Only merge single-parent scenarios for simplicity
        if len(parents) != 1:
            self.debug(f"CTE {cte.name} has multiple parents, skipping")
            return False, None

        parent = parents[0]
        if cte.base_alias != parent.safe_identifier:
            self.debug(
                f"CTE {cte.name} base alias {cte.base_alias} does not match parent {parent.safe_identifier}, skipping"
            )
            return False, None
        if isinstance(parent, (UnionCTE, RecursiveCTE)):
            self.debug(f"Parent {parent.name} is union/recursive, skipping")
            return False, None
        # A row LIMIT on the PARENT is an opaque boundary: folding the child's
        # shape into it moves work below the limit (pre-limit rows change).
        # A limited CHILD is fine — LIMIT evaluates last in the merged SELECT
        # and `apply_child_merge` carries it (with its ORDER BY) across.
        if parent.limit is not None:
            self.debug(f"Parent {parent.name} carries a row limit, skipping")
            return False, None
        if parent_is_ineligible(parent, merge_mode):
            self.debug(
                f"Parent {parent.name} is ineligible type {parent.source.source_type}, skipping"
            )
            return False, None
        if (
            merge_mode == MergeMode.BASIC
            and is_grouped_cte(parent)
            and not basic_fold_into_group_is_safe(parent, cte)
        ):
            self.debug(
                f"BASIC fold of {cte.name} into GROUP parent {parent.name} is "
                "not row-preserving (grain change or non-scalar output), skipping"
            )
            return False, None
        if merge_mode == MergeMode.PASSTHROUGH and not passthrough_renders_from_parent(
            cte, parent
        ):
            # Not a true passthrough — the child renders a column the parent
            # neither exposes nor can express as a rename, so folding would
            # drop it. Leave it.
            self.debug(
                f"Passthrough {cte.name} renders a column absent from parent "
                f"{parent.name}, skipping"
            )
            return False, None
        if destroys_subset_anchor_boundary(cte, parent, self.domain_graph):
            self.debug(
                f"CTE {cte.name} is a subset-narrowing rowset boundary its "
                f"parent {parent.name} cannot preserve, skipping"
            )
            return False, None
        # An existence subselect must always read FROM a CTE other than its
        # host. Two self-reference shapes, one per direction: the child's
        # exists() reads from the parent (merged, the exists() references the
        # CTE it lives in), or the parent's exists() reads from the child
        # (the post-merge repoint rewrites the parent's own existence map to
        # name itself). Either way the feeder must stay a separate CTE.
        if any(
            parent.name in (sources or [])
            for sources in cte.existence_source_map.values()
        ) or any(
            cte.name in (sources or [])
            for sources in parent.existence_source_map.values()
        ):
            self.debug(
                f"CTE {cte.name} and parent {parent.name} are linked by an "
                "existence reference; merging would self-reference, skipping"
            )
            return False, None

        # Parent must only be used by this CTE — except for a rename-only
        # projection, whose fold only appends output columns to the parent (no
        # condition, no regroup, no joins), leaving the parent's other consumers
        # unaffected. That covers PASSTHROUGH, and BASIC CTEs whose only local
        # computation is aliasing (`x as y` renders the parent's column under a
        # new name — get_merge_mode calls that BASIC, but it carries no compute).
        # A limited child is excluded: carrying its LIMIT onto a shared parent
        # would truncate the siblings' rows.
        if not is_sole_consumer(cte, parent, inverse_map):
            rename_only = (
                merge_mode in (MergeMode.PASSTHROUGH, MergeMode.BASIC)
                and cte.limit is None
                and is_passthrough_projection(cte)
                and passthrough_renders_from_parent(cte, parent)
            )
            # A consumer that already depends on the parent would end up
            # joining the parent to itself after the repoint (two rowset
            # renames of one scan, joined to each other, both folding into
            # that scan -> `p JOIN p` under one alias). Keep this child
            # materialized; the sibling that folded first already saved a CTE.
            consumer_joins_parent = any(
                parent.name in {d.name for d in consumer.dependency_nodes()}
                for consumer in inverse_map.get(cte.name, [])
            )
            if not rename_only or consumer_joins_parent:
                self.debug(f"Parent {parent.name} has multiple children, skipping")
                return False, None
            merge_mode = MergeMode.PASSTHROUGH

        if has_unsafe_derivations(parent):
            self.log(f"Parent {parent.name} has unsafe derivations, skipping")
            return False, None

        # Never fold away a rowset node whose rename backs an unbound merge/
        # scoped key: collapsing strands the key (INVALID_REFERENCE_BUG). A
        # rowset over bound keys (or unaliased outputs) folds normally.
        if unbound_rowset_blocks_merge(cte, parent, merge_mode, self.domain_graph):
            self.debug(
                f"CTE {cte.name} or parent {parent.name} is a rowset node backing "
                "an unbound key, skipping"
            )
            return False, None

        # A source_map entry pointing at the parent under an address the parent
        # does not itself output is a pseudonym rename — e.g. a merge node
        # exposing a canonical key (`local.step`) from a side that materializes
        # its merged pseudonym (`stages.stage`). That mapping exists ONLY in
        # this CTE's source_map; `apply_child_merge` does not carry it, and a
        # lineage-less key has no local derivation to fall back on, so the
        # collapsed CTE could never render the address (INVALID_REFERENCE_BUG).
        parent_outputs = parent.output_lcl
        renders_from_lineage = {
            column.address
            for column in cte.output_columns
            if renders_off_parent_output(column, cte, parent, set(parent_outputs))
        }
        for address, sources in cte.source_map.items():
            if (
                parent.name in sources
                and address not in parent_outputs
                # A rename whose referent the parent exposes needs no source_map
                # entry after the fold — the merged CTE renders it from lineage.
                and address not in renders_from_lineage
            ):
                self.log(
                    f"CTE {cte.name} sources {address} from parent {parent.name} "
                    "under a pseudonym rename the merge cannot carry, skipping"
                )
                return False, None
        if merge_mode == MergeMode.AGGREGATE:
            # An AGGREGATE merge wraps the parent's exposed columns in the child's
            # aggregates. A parent column that is rendered inline (no source_map
            # entry — not materialized by a grouping below) and whose lineage
            # contains an aggregate would be folded *inside* the child's `sum(...)`,
            # producing illegal nested aggregates. This covers a direct aggregate
            # column and one wrapped in a filter/function (`sum(x) ? cond`).
            for x in parent.output_columns:
                if not parent.source_map.get(x.address) and lineage_contains_aggregate(
                    x, set()
                ):
                    self.log(
                        f"Parent {parent.name} renders inline aggregate {x.address}, skipping"
                    )
                    return False, None
            # An aggregate ARGUMENT renders inside the merged CTE by lineage
            # re-derivation — it is not an output, so the rename rebind can
            # never pin it. A ROWSET boundary re-exposure re-derived in the
            # parent's context can settle on a different same-address twin
            # (count(rs.k) over a coalescing body picked the one-arm authored
            # key — unrenderable), so the fold must keep the boundary CTE
            # that materializes it.
            for column in cte.output_columns:
                for arg in column.concept_arguments:
                    if (
                        arg.derivation == Derivation.ROWSET
                        and arg.address not in parent_outputs
                    ):
                        self.log(
                            f"Aggregate argument {arg.address} of {column.address} "
                            f"is a rowset re-exposure parent {parent.name} does "
                            "not output, skipping"
                        )
                        return False, None

        self.log(
            f"Collapsing {merge_mode.value} CTE {cte.name} into parent {parent.name} ({parent.source.source_type})."
        )

        apply_child_merge(parent, cte, merge_mode)
        repoint_consumers(cte, parent, inverse_map)

        # Return merged map: old CTE name -> replacement CTE name
        return True, {cte.name: parent.name}
