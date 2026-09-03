from collections import Counter

from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildRowsetItem,
)
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    SENSITIVE_DERIVATIONS,
    is_grouped_cte,
    is_sole_consumer,
    render_cte_used_map,
    repoint_consumers,
)
from trilogy.core.processing.grain_utility import (
    join_preserves_left_rows,
    stacks_duplicate_rows,
)

# The child must be pure scalar transforms so its GROUP BY is vacuous
# relative to the parent.
CHILD_INELIGIBLE_DERIVATIONS = SENSITIVE_DERIVATIONS | {Derivation.AGGREGATE}


def _aggregate_inputs(concept: BuildConcept) -> list[BuildConcept]:
    """The concepts an aggregate column reads, unwrapping rowset items. A rowset
    concept over `sum(x)` reports ROWSET derivation and wraps the real aggregate."""
    lineage = concept.lineage
    if isinstance(lineage, BuildRowsetItem):
        content = lineage.content
        if isinstance(content, BuildConcept) and content.address != concept.address:
            return _aggregate_inputs(content)
        return []
    if isinstance(lineage, BuildAggregateWrapper):
        return list(lineage.function.concept_arguments)
    return []


def _drops_dedup_measure(cte: CTE, parent: CTE) -> bool:
    """True when a child aggregate would coarsen past a non-aggregate parent's
    GROUP BY that deduplicates on the very measure being aggregated.

    A non-aggregate parent GROUP BY keyed by real keys is a vacuous DISTINCT
    and safe to fold into. When the parent's grain folds in a non-key measure
    and the child aggregates that measure, dropping the parent's group
    double-counts the rows the dedup collapsed. Aggregating a parent-grain key
    stays safe (the key is already unique), so only non-key grain components
    trip this."""
    parent_grain = set(parent.grain.components)
    for column in cte.output_columns:
        for arg in _aggregate_inputs(column):
            if arg.address in parent_grain and arg.purpose != Purpose.KEY:
                return True
    return False


def _is_child_ineligible(concept: BuildConcept, cte: CTE, parent: CTE) -> bool:
    if concept.derivation not in CHILD_INELIGIBLE_DERIVATIONS:
        return False
    return cte.source_map.get(concept.address) != [parent.name]


def _identity_group_single_use_aggregate(cte: CTE, parent: CTE) -> bool:
    """Whether `parent` is an identity grouping safe to fuse into `cte`."""
    if not parent.source.datasources:
        return False
    right_ids = {
        join.right_datasource.identifier
        for join in parent.source.joins
        if isinstance(join, BaseJoin)
    }
    roots = [
        source
        for source in parent.source.datasources
        if source.identifier not in right_ids
    ]
    if len(roots) != 1:
        return False
    # The grain check below only proves identity if the root is actually unique
    # at its declared grain; a union stack is not.
    if stacks_duplicate_rows(roots[0]):
        return False
    if not set(roots[0].grain.components) <= set(parent.grain.components):
        return False
    if any(
        not isinstance(join, BaseJoin) or not join_preserves_left_rows(join)
        for join in parent.source.joins
    ):
        return False
    parent_outputs = {concept.address for concept in parent.output_columns}
    input_counts: Counter[str] = Counter()
    for concept in cte.output_columns:
        if concept.derivation != Derivation.AGGREGATE:
            continue
        inputs = _aggregate_inputs(concept)
        if not inputs or any(arg.address not in parent_outputs for arg in inputs):
            return False
        input_counts.update(arg.address for arg in inputs)
    return bool(input_counts) and max(input_counts.values()) == 1


def _active_parent_ctes(cte: CTE) -> list[CTE | UnionCTE]:
    used_map = render_cte_used_map(cte)
    referenced = set(used_map)
    parents = cte.dependency_nodes()
    active = [parent for parent in parents if parent.name in referenced]
    if active:
        return active
    referenced = {
        source
        for source_list in [
            *cte.source_map.values(),
            *cte.existence_source_map.values(),
        ]
        for source in source_list
    }
    active = [parent for parent in parents if parent.name in referenced]
    return active or parents


class MergeIrrelevantGroupBy(OptimizationRule):
    """Merge a GROUP BY CTE into its parent GROUP BY CTE when the grouping is redundant.

    When a CTE groups by keys that are all functionally determined by its parent's
    grain, the GROUP BY adds no new deduplication. We fold the child's computed columns
    into the parent, replace the parent's grain/output with the child's (coarser) grain,
    and eliminate the child CTE.
    """

    def __init__(self) -> None:
        super().__init__()
        self.completed: set[str] = set()

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, (UnionCTE, RecursiveCTE)):
            return False, None
        if cte.name in self.completed:
            return False, None
        if cte.joins:
            return False, None
        if cte.condition:
            return False, None
        if not is_grouped_cte(cte):
            return False, None
        active_parents = _active_parent_ctes(cte)
        if len(active_parents) != 1:
            return False, None

        cte.parent_ctes = active_parents
        parent = active_parents[0]
        if cte.base_alias != parent.safe_identifier:
            self.debug(
                f"CTE {cte.name} base alias {cte.base_alias} != parent {parent.safe_identifier}, skipping"
            )
            return False, None
        if isinstance(parent, (UnionCTE, RecursiveCTE)):
            return False, None
        # A limited parent is an opaque boundary (fusing the child's group into
        # it would re-aggregate below the limit); a limited child is fine, its
        # limit transfers to the merged CTE below.
        if parent.limit is not None:
            return False, None
        if not is_grouped_cte(parent):
            return False, None

        if not is_sole_consumer(cte, parent, inverse_map):
            self.debug(f"Parent {parent.name} has multiple children, skipping")
            return False, None

        # An existence subselect must read from a CTE other than its host;
        # merging either side of an existence link makes the exists()
        # reference the CTE it renders in (or a dropped name).
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

        identity_aggregate_fold = _identity_group_single_use_aggregate(cte, parent)
        for concept in cte.output_columns:
            if _is_child_ineligible(concept, cte, parent) and not (
                identity_aggregate_fold and concept.derivation == Derivation.AGGREGATE
            ):
                return False, None

        parent_has_aggregate = False
        for concept in parent.output_columns:
            if concept.derivation in SENSITIVE_DERIVATIONS:
                return False, None
            if concept.derivation == Derivation.AGGREGATE:
                parent_has_aggregate = True

        if not parent_has_aggregate and _drops_dedup_measure(cte, parent):
            self.debug(
                f"CTE {cte.name} aggregates a measure {parent.name} deduplicates on, skipping"
            )
            return False, None

        # When the parent computes aggregates its GROUP BY grain matters: the
        # child must preserve it. Compare via equivalent_addresses so aliased
        # keys count as equal.
        if parent_has_aggregate:
            child_grain_addresses: set[str] = set()
            for column in cte.output_columns:
                if column.address in cte.grain.components:
                    child_grain_addresses.update(column.equivalent_addresses)
            for component in parent.grain.components:
                if component not in child_grain_addresses:
                    return False, None

        self.log(f"Merging  group-by {cte.name} into irrelevant parent {parent.name}")
        # An empty source_map entry makes the renderer compute the expression
        # from concept lineage.
        parent_output_addresses = {x.address for x in parent.output_columns}
        for x in cte.output_columns:
            if x.address not in parent_output_addresses:
                parent.output_columns.append(x)
            if x.address not in parent.source_map:
                parent.source_map[x.address] = []

        # Carry the child's existence references and nullability: dropping
        # them strands memberships and lets null-safe joins be falsely
        # downgraded (same contract as CollapseSingleParent).
        for address, sources in cte.existence_source_map.items():
            if address not in parent.existence_source_map:
                parent.existence_source_map[address] = sources
        nullable_addresses = {c.address for c in parent.nullable_concepts}
        for column in cte.nullable_concepts:
            if column.address not in nullable_addresses:
                parent.nullable_concepts.append(column)

        # The child's output_columns already carry the hidden group-by keys,
        # so the GROUP BY survives the swap.
        cte_output_addresses = {x.address for x in cte.output_columns}
        parent.output_columns = [
            x for x in parent.output_columns if x.address in cte_output_addresses
        ]
        parent.grain = cte.grain
        parent.hidden_concepts = parent.hidden_concepts.union(cte.hidden_concepts)
        # LIMIT is the last logical operation of a SELECT, so the child's limit
        # and ORDER BY carry onto the merged CTE unchanged.
        if cte.limit is not None:
            parent.limit = cte.limit
            parent.order_by = cte.order_by

        repoint_consumers(cte, parent, inverse_map)

        self.completed.add(cte.name)
        self.completed.add(parent.name)
        return True, {cte.name: parent.name}
