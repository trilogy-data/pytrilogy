"""Hoist predicate-only inner joins from a child CTE to its shared parent CTE.

Motivation:

When several siblings all read from a shared parent
CTE AND all apply the same dim joins purely to filter rows via a
shared WHERE predicate, the dim joins + predicate are redundantly evaluated
N times — once per sibling. The classic example is q73, where three siblings
each re-join (date_dim, store, household_demographics) only to evaluate
``D_DOM BETWEEN 1 AND 2 AND S_COUNTY IN (...) AND HD_BUY_POTENTIAL = ...``.

This rule recognizes the pattern and pushes the dim join + its predicate up to
the shared parent so the work happens once.

Conceptually we treat ``JOIN dim WHERE dim.col = X`` as an existence-style
predicate on the FK column already on the parent. Once hoisted, the parent
materializes the same row set for every sibling, and the siblings shrink to
thin projections of the parent.

Safety constraints:

  - INNER join only — outer joins change row presence.
  - Right side at-grain (dim grain ⊆ join keys) — no fan-out.
  - The dim concepts the join brings in are referenced ONLY by the candidate
    predicate, nowhere else in the child (no SELECT, no other WHERE clauses,
    no GROUP BY, no other joins).
  - All siblings of the parent already carry the same predicate, so the
    post-hoist row set matches every sibling's existing expectation.

Runs before InlineDatasource so the moved join is still in non-inlined form;
the subsequent inlining pass folds the parent's now-extended FROM into a flat
table reference exactly like any other dim join.
"""

from typing import cast

from trilogy.core.enums import JoinType, SourceType
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
    BuildWindowItem,
)
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import (
    MergedCTEMap,
    OptimizationRule,
)
from trilogy.core.optimizations.join_upgrade import (
    UpgradeJoinOnGuards,
    _gather_proofs,
)
from trilogy.core.optimizations.utils import (
    add_datasource_sorted,
    add_parent_cte,
    append_condition,
    condition_contains_atom,
    render_cte_used_map,
    strip_condition_atom,
)
from trilogy.core.processing.condition_utility import is_scalar_condition

HOISTABLE_JOIN_TYPES = {JoinType.INNER, JoinType.LEFT_OUTER}


def _is_grouped_cte(cte: CTE) -> bool:
    return cte.group_to_grain or cte.source.source_type == SourceType.GROUP


def _is_child_of(candidate, condition) -> bool:
    """Value-based: True if `candidate` appears anywhere in an AND-tree of
    `condition`. Siblings carry structurally identical but referentially
    distinct copies of the same predicate, so we compare by ``==``."""
    return condition_contains_atom(candidate, condition)


def _strip_candidate(condition, candidate):
    return strip_condition_atom(condition, candidate)


class JoinHoist(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.complete: dict[str, bool] = {}

    def _find_left_base_cte(
        self, parent_cte: CTE, fk_addresses: set[str]
    ) -> CTE | UnionCTE | None:
        """Find which of parent_cte.parent_ctes provides the FK columns. That CTE
        becomes the left side (pair.cte) of the hoisted join."""
        for p in parent_cte.parent_ctes:
            if isinstance(p, (CTE, UnionCTE)) and fk_addresses.issubset(
                {c.address for c in p.output_columns}
            ):
                return p
        return None

    def _find_left_base_datasource(
        self, parent_cte: CTE, fk_addresses: set[str]
    ) -> QueryDatasource | BuildDatasource | None:
        """Find which datasource on parent_cte.source provides the FK columns
        (used as ConceptPair.existing_datasource for the new BaseJoin)."""
        for ds in parent_cte.source.datasources:
            ds_outputs = {c.address for c in ds.output_concepts}
            if fk_addresses.issubset(ds_outputs):
                return ds
        return None

    def _find_left_base_join_cte(
        self,
        parent_cte: CTE,
        left_base_ds: QueryDatasource | BuildDatasource,
        fk_addresses: set[str],
    ) -> tuple[CTE | UnionCTE | None, bool]:
        left_base_cte = self._find_left_base_cte(parent_cte, fk_addresses)
        if left_base_cte is not None:
            return left_base_cte, False
        if isinstance(left_base_ds, BuildDatasource):
            return CTE.from_datasource(left_base_ds), True
        return None, False

    def _candidates(self, cte: CTE) -> list:
        if not cte.condition:
            return []
        if isinstance(cte.condition, BuildConditional):
            return cte.condition.decompose()
        return [cte.condition]

    def _collect_referenced_addresses_excluding(
        self, cte: CTE, exclude_candidates: list
    ) -> set[str]:
        """Addresses cte still consumes from any parent after the bundled
        candidates are hypothetically removed.

        Renders cte (mirroring ``HideUnusedConcepts``) so the check follows
        alias/lineage chains a shallow scan of ``output_columns`` would miss —
        e.g. ``store_cumulative <- alias(store_cume)`` is rendered as
        ``parent.store_cume`` even though only ``store_cumulative`` is in
        ``output_columns``. The bundled candidates are temporarily stripped
        from ``cte.condition`` so their own references aren't counted as
        "needed elsewhere".
        """
        original_condition = cte.condition
        stripped_condition = original_condition
        for cand in exclude_candidates:
            stripped_condition = _strip_candidate(stripped_condition, cand)
        cte.condition = stripped_condition
        try:
            used_map = render_cte_used_map(cte)
        finally:
            cte.condition = original_condition
        referenced: set[str] = set()
        for addrs in used_map.values():
            referenced.update(addrs)
        # Concepts cte exposes downstream — these don't show up in cte's own
        # used_map (consumers haven't been re-rendered) but the renderer still
        # has to project them, so they pin source_map entries we shouldn't
        # strip.
        referenced.update(c.address for c in cte.output_columns)
        return referenced

    def _join_hoist_plan(
        self,
        cte: CTE,
        parent_cte: CTE,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> list[tuple[Join, list, list, JoinType]] | None:
        """Per-join plan: which joins to hoist, and which candidate predicates
        ride along with each.

        For each predicate-only inner join, gather every candidate predicate in
        ``cte.condition`` that references concepts the join brings in. The join
        + candidates move together. Joins are only included when:
          - the dim concepts the join supplies are referenced ONLY by the
            bundled candidates, nowhere else in cte;
          - all siblings of parent_cte already carry every bundled candidate;
          - the FK keys are materialized on parent_cte.
        Returns the plan, or None if nothing's hoistable."""
        if parent_cte.condition and not is_scalar_condition(parent_cte.condition):
            return None
        non_materialized = {k for k, v in parent_cte.source_map.items() if v == []}
        for x in parent_cte.output_columns:
            if x.address in non_materialized and isinstance(x.lineage, BuildWindowItem):
                return None
        materialized = {k for k, v in parent_cte.source_map.items() if v != []}
        siblings = inverse_map.get(parent_cte.name, [])
        if not siblings:
            return None
        if not _is_grouped_cte(parent_cte):
            return None
        candidates = [
            c
            for c in self._candidates(cte)
            if isinstance(c, (BuildComparison, BuildConditional, BuildParenthetical))
            and is_scalar_condition(c)
            # `existence_arguments` may carry literal IN-list values for
            # SubselectComparison; only reject actual concept-bearing ones
            and not any(arg for tup in c.existence_arguments for arg in tup)
        ]
        if not candidates:
            return None

        child_joins = [
            j
            for j in cte.joins
            if isinstance(j, Join)
            and isinstance(j.right_cte, CTE)
            and j.right_cte.name != parent_cte.name
            and j.right_cte.source is not parent_cte.source
        ]
        plan: list[tuple[Join, list, list, JoinType]] = []
        for j in child_joins:
            if j.jointype not in HOISTABLE_JOIN_TYPES:
                continue
            if (
                j.right_cte.name == parent_cte.name
                or j.right_cte.source is parent_cte.source
            ):
                continue
            if not j.joinkey_pairs:
                continue
            if j.condition is not None:
                continue
            join: Join = j
            join_keys_left = {p.left.address for p in join.joinkey_pairs or []}
            join_keys_right = {p.right.address for p in join.joinkey_pairs or []}
            if not join_keys_left.issubset(materialized):
                continue
            dim_grain = set(join.right_cte.grain.components)
            if dim_grain and not dim_grain.issubset(join_keys_right):
                continue
            join_brings = {c.address for c in join.right_cte.output_columns}
            filter_concepts = join_brings - join_keys_right
            # bundle candidates that reference this dim's concepts. Two flavors:
            #   - to_push: not yet on parent.condition; we'll AND-extend parent
            #   - to_strip_only: already on parent.condition (hoisted via a
            #     sibling earlier this round); just strip from cte
            to_push: list = []
            to_strip_only: list = []
            bail = False
            for cand in candidates:
                cand_args = {x.address for x in cand.row_arguments}
                if not (cand_args & filter_concepts):
                    continue
                if not cand_args.issubset(filter_concepts | materialized):
                    bail = True
                    break
                already_on_parent = _is_child_of(cand, parent_cte.condition)
                if already_on_parent:
                    to_strip_only.append(cand)
                    continue
                # if not already on parent, the candidate must apply to all
                # siblings — either on each sibling's condition, or already
                # pushed up to parent
                if not all(
                    _is_child_of(cand, s.condition)
                    or _is_child_of(cand, parent_cte.condition)
                    for s in siblings
                ):
                    bail = True
                    break
                to_push.append(cand)
            if bail:
                continue
            bundled = to_push + to_strip_only
            if not bundled:
                continue
            join_type = self._join_type_after_hoist(join, bundled)
            if join_type is None:
                continue
            # dim concepts must be unused outside the bundled candidates
            needed_elsewhere = self._collect_referenced_addresses_excluding(
                cte, bundled
            )
            if filter_concepts & needed_elsewhere:
                continue
            plan.append((join, to_push, to_strip_only, join_type))
        if len(siblings) == 1 and len(plan) != len(child_joins):
            return None
        return plan or None

    def _lock_in_guarded_upgrades(
        self,
        cte: CTE,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> None:
        """Realize guard-enabled join upgrades on ``cte`` before its guards are
        hoisted away.

        Hoisting a dim join up to the shared parent strips the filter predicate
        from ``cte``. That predicate may be the only thing letting
        UpgradeJoinOnGuards downgrade an OUTER join in ``cte`` (classically a
        filter-only RIGHT_OUTER to the very parent the dim is hoisted into,
        which contributes no output columns and only restricts rows). Once the
        guard is gone UpgradeJoinOnGuards bails (no condition) and the
        conservative OUTER join sticks. Apply that upgrade now, while the guard
        is still present, so the hoist can't silently regress the join."""
        UpgradeJoinOnGuards().optimize(cte, inverse_map)

    def _parent_already_joins_dim(
        self, parent_cte: CTE, dim_qds: QueryDatasource
    ) -> bool:
        return any(
            isinstance(bj, BaseJoin)
            and bj.right_datasource.identifier == dim_qds.identifier
            for bj in parent_cte.source.joins
        )

    def _join_type_after_hoist(self, join: Join, bundled: list) -> JoinType | None:
        if join.jointype == JoinType.INNER:
            return JoinType.INNER
        if join.jointype != JoinType.LEFT_OUTER:
            return None
        right_addresses = {c.address for c in join.right_cte.output_columns}
        forced = {addr for cand in bundled for addr in _gather_proofs(cand)}
        if forced & right_addresses:
            return JoinType.INNER
        return None

    def _hoist_join(
        self,
        cte: CTE,
        parent_cte: CTE,
        join: Join,
        join_type: JoinType,
    ) -> bool:
        """Construct fresh BaseJoin + Join state on parent_cte for `join`, and
        strip the original from cte. If parent_cte already joins the same dim
        (a sibling hoisted it earlier), only strip from cte. Returns True on
        success."""
        assert isinstance(join.right_cte, CTE)
        dim_cte = join.right_cte
        dim_qds = dim_cte.source

        # find the corresponding BaseJoin in cte.source.joins (matches by right ds)
        cte_base_join: BaseJoin | None = None
        for bj in cte.source.joins:
            if (
                isinstance(bj, BaseJoin)
                and bj.right_datasource.identifier == dim_qds.identifier
            ):
                cte_base_join = bj
                break

        if not self._parent_already_joins_dim(parent_cte, dim_qds):
            fk_addresses = {p.left.address for p in (join.joinkey_pairs or [])}
            left_base_ds = self._find_left_base_datasource(parent_cte, fk_addresses)
            if left_base_ds is None:
                self.debug(
                    f"Cannot locate left base for FK {fk_addresses} on "
                    f"{parent_cte.name}; "
                    f"parents={[p.name for p in parent_cte.parent_ctes]}, "
                    f"datasources={[d.identifier for d in parent_cte.source.datasources]}"
                )
                return False
            left_base_cte, inline_left_base = self._find_left_base_join_cte(
                parent_cte,
                left_base_ds,
                fk_addresses,
            )
            if left_base_cte is None:
                self.debug(
                    f"Cannot locate left CTE for FK {fk_addresses} on "
                    f"{parent_cte.name}; "
                    f"parents={[p.name for p in parent_cte.parent_ctes]}, "
                    f"datasources={[d.identifier for d in parent_cte.source.datasources]}"
                )
                return False

            new_concept_pairs = [
                ConceptPair(
                    left=p.left,
                    right=p.right,
                    existing_datasource=left_base_ds,
                    modifiers=p.modifiers,
                )
                for p in (join.joinkey_pairs or [])
            ]
            new_base_join = BaseJoin(
                right_datasource=dim_qds,
                join_type=join_type,
                concept_pairs=new_concept_pairs,
                modifiers=list(join.modifiers),
            )
            parent_cte.source.joins.append(new_base_join)
            add_datasource_sorted(parent_cte, dim_qds)
            existing_input_addrs = {c.address for c in parent_cte.source.input_concepts}
            for c in dim_cte.output_columns:
                if c.address not in existing_input_addrs:
                    parent_cte.source.input_concepts.append(c)
                    existing_input_addrs.add(c.address)
            for c in dim_cte.output_columns:
                parent_cte.source.source_map.setdefault(c.address, set()).add(dim_qds)
            new_joinkey_pairs = [
                CTEConceptPair(
                    left=p.left,
                    right=p.right,
                    existing_datasource=left_base_ds,
                    modifiers=p.modifiers,
                    cte=left_base_cte,
                )
                for p in (join.joinkey_pairs or [])
            ]
            new_join = Join(
                right_cte=dim_cte,
                jointype=join_type,
                left_cte=None,
                joinkey_pairs=new_joinkey_pairs,
                modifiers=list(join.modifiers),
            )
            if (
                inline_left_base
                and isinstance(left_base_ds, BuildDatasource)
                and isinstance(left_base_cte, CTE)
            ):
                new_join.inline_cte(left_base_cte, left_base_ds)
            parent_cte.joins.append(new_join)
            add_parent_cte(parent_cte, dim_cte)
            for c in dim_cte.output_columns:
                parent_cte.source_map.setdefault(c.address, [])
                if dim_cte.name not in parent_cte.source_map[c.address]:
                    parent_cte.source_map[c.address].append(dim_cte.name)

        # ---- strip the join from cte ----
        cte.joins.remove(join)
        if cte_base_join is not None and cte_base_join in cte.source.joins:
            cte.source.joins.remove(cte_base_join)
        # filter-only concepts (not join keys) the dim brought in — these go
        # away entirely. Join keys remain because cte may still need them
        # from the FK side.
        join_keys_right = {p.right.address for p in (join.joinkey_pairs or [])}
        dim_filter_addresses = {
            c.address for c in dim_cte.output_columns
        } - join_keys_right
        still_referenced = any(
            isinstance(bj, BaseJoin)
            and bj.right_datasource.identifier == dim_qds.identifier
            for bj in cte.source.joins
        )
        if not still_referenced and dim_qds in cte.source.datasources:
            cte.source.datasources = [
                d for d in cte.source.datasources if d.identifier != dim_qds.identifier
            ]
        for addr in dim_filter_addresses:
            qds_sources = cte.source.source_map.get(addr)
            if qds_sources is not None:
                qds_sources = {
                    s
                    for s in qds_sources
                    if not hasattr(s, "identifier")
                    or s.identifier != dim_qds.identifier
                }
                if not qds_sources:
                    del cte.source.source_map[addr]
                else:
                    cte.source.source_map[addr] = qds_sources
        cte.source.input_concepts = [
            c
            for c in cte.source.input_concepts
            if c.address not in dim_filter_addresses
        ]
        for addr in dim_filter_addresses:
            cte.source_map.pop(addr, None)
        # for join keys in cte.source_map that pointed to the dim, redirect
        # to the FK source — the original pair.cte. Otherwise cte renders
        # `dim_cte.col` for a dim that's no longer in its FROM.
        for pair in join.joinkey_pairs or []:
            addr = pair.left.address
            rendering_sources = cte.source_map.get(addr)
            if not rendering_sources or dim_cte.name not in rendering_sources:
                continue
            new_sources = [s for s in rendering_sources if s != dim_cte.name]
            if pair.cte is not None and pair.cte.name not in new_sources:
                new_sources.append(pair.cte.name)
            cte.source_map[addr] = new_sources
            # mirror the redirect at the QDS level
            qds_set = cte.source.source_map.get(addr)
            if qds_set is not None:
                qds_set = {
                    s
                    for s in qds_set
                    if not hasattr(s, "identifier")
                    or s.identifier != dim_qds.identifier
                }
                if not qds_set:
                    if pair.existing_datasource is not None:
                        cte.source.source_map[addr] = {pair.existing_datasource}
                    else:
                        del cte.source.source_map[addr]
                else:
                    cte.source.source_map[addr] = qds_set
        still_used = any(
            isinstance(jj, Join)
            and isinstance(jj.right_cte, CTE)
            and jj.right_cte.name == dim_cte.name
            for jj in cte.joins
        )
        if not still_used:
            cte.parent_ctes = [p for p in cte.parent_ctes if p.name != dim_cte.name]
        return True

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, UnionCTE):
            return False, None
        if not isinstance(cte, CTE):
            return False, None
        if self.complete.get(cte.name):
            return False, None
        if not cte.condition or not cte.joins or not cte.parent_ctes:
            self.complete[cte.name] = True
            return False, None
        candidate_parents = [
            p
            for p in cte.parent_ctes
            if isinstance(p, CTE)
            and p.source.source_type not in (SourceType.WINDOW, SourceType.UNNEST)
        ]
        if not candidate_parents:
            self.complete[cte.name] = True
            return False, None

        actions = False
        locked_in_upgrades = False
        for parent_cte in candidate_parents:
            plan = self._join_hoist_plan(cte, parent_cte, inverse_map)
            if not plan:
                continue
            if not locked_in_upgrades:
                self._lock_in_guarded_upgrades(cte, inverse_map)
                locked_in_upgrades = True
                # join types may have tightened; recompute against current state
                plan = self._join_hoist_plan(cte, parent_cte, inverse_map)
                if not plan:
                    continue
            for join, to_push, to_strip_only, join_type in plan:
                if not self._hoist_join(cte, parent_cte, join, join_type):
                    continue
                for cand in to_push:
                    parent_cte.condition = append_condition(
                        parent_cte.condition,
                        cast(
                            BuildComparison | BuildConditional | BuildParenthetical,
                            cand,
                        ),
                    )
                    cte.condition = _strip_candidate(cte.condition, cand)
                for cand in to_strip_only:
                    cte.condition = _strip_candidate(cte.condition, cand)
                self.log(
                    f"Hoisted join {join.right_cte.name} from {cte.name} to "
                    f"{parent_cte.name}: pushed {len(to_push)}, "
                    f"stripped {len(to_strip_only)}"
                )
                actions = True
                self.complete[parent_cte.name] = False
        self.complete[cte.name] = True
        return actions, None
