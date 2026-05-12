from typing import List, Optional

from trilogy.core.enums import SourceType
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildGrain,
    BuildOrderBy,
    BuildParenthetical,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.nodes.base_node import (
    StrategyNode,
)


class RowsetNode(StrategyNode):
    """A typed boundary around a rowset materialization.

    A rowset declares a SELECT DISTINCT shape: its full output set defines
    the grain at which it dedups. Two consumers of different rowset items
    that independently re-source the rowset would prune its outputs to the
    subset each consumer needs and silently split the declared grain across
    consumers, producing two CTEs with incompatible GROUP BYs.

    RowsetNode pins the rowset's full declared outputs as ``full_outputs``
    and freezes the grain at construction. Downstream consumers may attach
    BASIC derivations (e.g. ``_virt_filter_*`` CASE wrappers, alias
    projections) to ``output_concepts`` — these are scalar transforms of
    existing rowset items and don't alter the grain, so the underlying CTE
    stays shareable. ``set_output_concepts`` is a no-op so callers can't
    *replace* the projection (which would shrink it below the declared
    shape and corrupt the History cache for other consumers).

    ``resolve`` delegates to the inner select node's resolve rather than
    wrapping it in a new QueryDatasource — wrapping would introduce a
    redundant ``SELECT * FROM inner`` CTE layer that downstream
    optimizations like CollapseSingleParent don't recognize.
    """

    source_type = SourceType.ROWSET

    def __init__(
        self,
        rowset_name: str,
        full_outputs: List[BuildConcept],
        environment: BuildEnvironment,
        parents: List["StrategyNode"],
        depth: int = 0,
        partial_concepts: Optional[List[BuildConcept]] = None,
        nullable_concepts: Optional[List[BuildConcept]] = None,
        rollup_concepts: Optional[List[BuildConcept]] = None,
        conditions: (
            BuildConditional | BuildComparison | BuildParenthetical | None
        ) = None,
        preexisting_conditions: (
            BuildConditional | BuildComparison | BuildParenthetical | None
        ) = None,
        hidden_concepts: set[str] | None = None,
        grain: BuildGrain | None = None,
        ordering: BuildOrderBy | None = None,
        # Below are accepted for compatibility with the StrategyNode copy()
        # protocol but are recomputed from full_outputs.
        input_concepts: Optional[List[BuildConcept]] = None,
        output_concepts: Optional[List[BuildConcept]] = None,
        whole_grain: bool = True,
        force_group: bool | None = None,
        existence_concepts: Optional[List[BuildConcept]] = None,
        virtual_output_concepts: Optional[List[BuildConcept]] = None,
    ):
        if not parents or len(parents) != 1:
            raise ValueError(
                f"RowsetNode requires exactly one parent (the rowset materialization), got {parents}"
            )
        self.rowset_name = rowset_name
        self.full_outputs: List[BuildConcept] = list(full_outputs)
        frozen_grain = grain or BuildGrain.from_concepts(
            self.full_outputs, environment=environment
        )
        super().__init__(
            input_concepts=list(self.full_outputs),
            output_concepts=list(self.full_outputs),
            environment=environment,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            nullable_concepts=nullable_concepts,
            rollup_concepts=rollup_concepts,
            conditions=conditions,
            preexisting_conditions=preexisting_conditions,
            hidden_concepts=hidden_concepts,
            grain=frozen_grain,
            ordering=ordering,
            existence_concepts=existence_concepts,
            virtual_output_concepts=virtual_output_concepts,
            force_group=force_group,
        )

    def set_output_concepts(
        self,
        concepts: List[BuildConcept],
        rebuild: bool = True,
        change_visibility: bool = True,
    ):
        # Refuse to *replace* the projection — that would shrink it below
        # the rowset's declared shape and propagate through the History
        # cache to other consumers. Adding new derivations is fine and
        # goes through ``add_output_concepts`` (e.g. BASIC wrappers like
        # ``_virt_filter_*`` that derive from existing rowset items).
        if rebuild:
            self.rebuild_cache()
        return self

    def hide_output_concepts(self, concepts, rebuild: bool = True):
        # Refuse to hide rowset outputs — multiple consumers share this
        # node via the History cache, and hiding here would prevent later
        # consumers (e.g. the outer WHERE clause needing a presence flag
        # that an earlier consumer's basic_node didn't request) from
        # seeing it. Consumers should hide on their own wrapper instead.
        if rebuild:
            self.rebuild_cache()
        return self

    def resolve(self) -> QueryDatasource:
        # Inline through to the parent's QDS rather than wrapping it. The
        # rowset boundary's behavior is "the parent select, frozen at full
        # grain, possibly with extra BASIC derivations downstream consumers
        # attached". Wrapping with a new QDS would introduce a redundant
        # CTE layer (visible as a `SELECT * FROM rowset_inner` passthrough)
        # that downstream optimizations like CollapseSingleParent don't
        # recognize. Delegating preserves the single-CTE shape while still
        # giving us a typed boundary at the planner level.
        if self.resolution_cache:
            return self.resolution_cache
        inner = self.parents[0].copy()
        # Add any consumer-attached derivations onto the inner before
        # resolving — these are BASIC scalars over existing rowset items,
        # so they don't change the inner's grain.
        full_addresses = {c.address for c in self.full_outputs}
        extras = [c for c in self.output_concepts if c.address not in full_addresses]
        if extras:
            inner.add_output_concepts(extras, rebuild=False)
        if self.hidden_concepts:
            inner.hide_output_concepts(set(self.hidden_concepts), rebuild=False)
        if self.ordering is not None:
            inner.ordering = self.ordering
        inner.rebuild_cache()
        qds = inner.resolve()
        self.resolution_cache = qds
        return qds

    def _resolve(self) -> QueryDatasource:
        # Unused — ``resolve`` is overridden directly so we can return the
        # inner QDS without an extra wrapping layer. Kept for parity with
        # the StrategyNode protocol.
        return self.resolve()

    def copy(self) -> "RowsetNode":
        return RowsetNode(
            rowset_name=self.rowset_name,
            full_outputs=list(self.full_outputs),
            environment=self.environment,
            parents=list(self.parents),
            depth=self.depth,
            partial_concepts=list(self.partial_concepts),
            nullable_concepts=list(self.nullable_concepts),
            rollup_concepts=list(self.rollup_concepts),
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            hidden_concepts=set(self.hidden_concepts),
            grain=self.grain,
            ordering=self.ordering,
            whole_grain=self.whole_grain,
        )
