from trilogy.core.enums import (
    BooleanOperator,
    JoinType,
    SourceType,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildConceptArgs,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
    BuildWindowItem,
)
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    append_condition,
    condition_contains_atom,
    rebuild_and_condition,
)
from trilogy.core.processing.condition_utility import (
    condition_value_implies,
    conditions_mutually_exclusive,
    is_scalar_condition,
)
from trilogy.utility import unique


def is_child_of(a, comparison):
    return condition_contains_atom(a, comparison)


def _consumer_outer_joins_union(consumer: CTE | UnionCTE, union: UnionCTE) -> bool:
    """True if ``consumer`` references ``union`` on the nullable side of an
    outer join — pushing predicates into the union branches in that case
    changes outer-join semantics."""
    if not isinstance(consumer, CTE):
        return True
    for j in consumer.joins:
        if not isinstance(j, Join) or j.jointype == JoinType.INNER:
            continue
        # right side of any outer join is nullable
        if isinstance(j.right_cte, UnionCTE) and j.right_cte.name == union.name:
            return True
        # left side is nullable for RIGHT_OUTER/FULL only
        if j.jointype in (JoinType.RIGHT_OUTER, JoinType.FULL) and (
            isinstance(j.left_cte, UnionCTE) and j.left_cte.name == union.name
        ):
            return True
    return False


def _branch_constraint_implies(branch: CTE | UnionCTE, condition) -> bool:
    """True if the branch's base_datasource ``non_partial_for`` constraint
    value-implies the condition (so the condition is tautologically true on
    rows from this branch). Handles the partial-datasource case where a
    branch is bounded to a literal — e.g. ``complete where channel='CATALOG'``
    makes any pushed ``channel in ('WEB','CATALOG')`` redundant on that
    branch."""
    if not isinstance(branch, CTE):
        return False
    base = branch.source.base_datasource
    if not isinstance(base, BuildDatasource) or base.non_partial_for is None:
        return False
    return condition_value_implies(base.non_partial_for.conditional, condition)


def _parent_covers_condition(parent: CTE | UnionCTE, condition) -> bool:
    """True if the consumer's ``condition`` is implied by ``parent``.

    For a plain CTE that's just ``is_child_of(condition, parent.condition)``.
    For a UnionCTE, the parent has no top-level condition — we check that
    every branch's condition covers the consumer atom (so the post-union
    rows are already filtered). A branch can also cover via its base
    datasource's ``non_partial_for`` constraint (e.g. a partial datasource
    bounded to ``channel='CATALOG'`` covers any pushed
    ``channel in ('WEB','CATALOG')`` predicate).
    """
    if isinstance(parent, UnionCTE):
        if not parent.internal_ctes:
            return False
        return all(
            is_child_of(condition, branch.condition)
            or _branch_constraint_implies(branch, condition)
            for branch in parent.internal_ctes
        )
    return is_child_of(condition, parent.condition)


class PredicatePushdown(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.complete: dict[str, bool] = {}

    def _push_into_union_branches(
        self,
        cte: CTE | UnionCTE,
        parent_cte: UnionCTE,
        candidate: BuildConditional | BuildComparison | BuildParenthetical | None,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> bool:
        """AND-extend each surviving branch's condition with ``candidate``.

        Mirrors the plain-CTE path in ``_check_parent``: only fires when every
        consumer of the union already carries the atom (so applying it once
        per branch is identical to applying it post-union).
        """
        if not isinstance(candidate, BuildConceptArgs):
            return False
        row_conditions = {x.address for x in candidate.row_arguments}
        if not row_conditions:
            return False
        if parent_cte.source.source_type == SourceType.FILTER:
            return False

        # Concepts the union can expose. ``output_columns`` lists what the
        # union QDS chose to project, but each branch's ``source_map`` often
        # carries additional concepts (joined dim columns, etc.) that the
        # rendered UNION ALL still emits, so use the wider view.
        union_reachable = {x.address for x in parent_cte.output_columns}
        for b in parent_cte.internal_ctes:
            union_reachable |= set(b.source_map.keys())
        if not row_conditions.issubset(union_reachable):
            return False

        existence_conditions = {
            y.address for x in candidate.existence_arguments for y in x
        }
        # Existence concepts produced inside the union itself can't be
        # referenced as external IN targets from a branch.
        if existence_conditions and existence_conditions.intersection(union_reachable):
            return False

        all_inputs = {x.address for x in candidate.concept_arguments}
        existence_extras = all_inputs - row_conditions

        children = inverse_map.get(parent_cte.name, [])
        if not children:
            return False
        if not all(is_child_of(candidate, child.condition) for child in children):
            return False
        # Bail when any consumer outer-joins the union — predicates on the
        # union's outputs interact with NULL-padding semantics and pushing
        # them into branches changes which rows survive the outer join.
        if any(_consumer_outer_joins_union(child, parent_cte) for child in children):
            return False

        # Only handle plain-CTE branches for now (mirrors UnionDimPushdown).
        if not all(isinstance(b, CTE) for b in parent_cte.internal_ctes):
            return False

        actionable: list[CTE] = []
        for raw_branch in parent_cte.internal_ctes:
            assert isinstance(raw_branch, CTE)
            branch = raw_branch
            if branch.condition is not None and is_child_of(
                candidate, branch.condition
            ):
                continue
            # Branch's partial-datasource constraint may already value-imply
            # the candidate (e.g. branch is fixed to ``channel='CATALOG'`` and
            # candidate is ``channel in ('WEB','CATALOG')``). No need to push
            # the tautology in — the rendered SQL would just carry
            # ``'CATALOG' in ('WEB','CATALOG')``.
            if _branch_constraint_implies(branch, candidate):
                continue
            if branch.condition is not None and not is_scalar_condition(
                branch.condition
            ):
                return False
            non_materialized = [k for k, v in branch.source_map.items() if v == []]
            concrete = [
                x for x in branch.output_columns if x.address in non_materialized
            ]
            if any(isinstance(x.lineage, BuildWindowItem) for x in concrete):
                return False
            join_derived_addrs = {x.address for x in branch.join_derived_concepts}
            if row_conditions & join_derived_addrs:
                return False
            materialized = {k for k, v in branch.source_map.items() if v != []}
            if branch.is_root_datasource:
                base = branch.source.base_datasource
                if base is not None:
                    extra = {x.address for x in base.output_concepts}
                    if row_conditions.issubset(extra):
                        materialized |= row_conditions
            if not row_conditions.issubset(materialized):
                return False
            if existence_extras:
                for x in existence_extras:
                    if (
                        x not in cte.source_map
                        and x not in cte.existence_source_map
                        and x not in branch.source_map
                        and x not in branch.existence_source_map
                    ):
                        return False
            actionable.append(branch)

        if not actionable:
            return False

        union_dependencies_changed = False
        for branch in actionable:
            if branch.is_root_datasource:
                base = branch.source.base_datasource
                if base is not None:
                    materialized_now = {
                        k for k, v in branch.source_map.items() if v != []
                    }
                    extra = {x.address for x in base.output_concepts}
                    if row_conditions.issubset(extra):
                        for x in row_conditions:
                            if x not in materialized_now:
                                branch.source_map[x] = [base.name]
            if branch.condition is None:
                branch.condition = candidate
            else:
                branch.condition = append_condition(branch.condition, candidate)
            for x in existence_extras:
                if x in branch.source_map or x in branch.existence_source_map:
                    continue
                # Propagate from whichever map the consumer used (regular
                # source_map vs. existence_source_map for subselect-only).
                if x in cte.source_map:
                    origin = list(cte.source_map[x])
                    branch.source_map[x] = origin
                elif x in cte.existence_source_map:
                    origin = list(cte.existence_source_map[x])
                    branch.existence_source_map[x] = origin
                else:
                    continue
                sources = [p for p in cte.parent_ctes if p.name in origin]
                branch.parent_ctes = unique(branch.parent_ctes + sources, "name")
                union_dependencies_changed = True
            self.log(
                f"Pushed {candidate} into union branch {branch.name} of {parent_cte.name}"
            )

        if union_dependencies_changed:
            # Re-derive union.parent_ctes so reorder_ctes sees the new edges.
            parent_cte.parent_ctes = unique(
                [p for branch in parent_cte.internal_ctes for p in branch.parent_ctes],
                "name",
            )
        return True

    def _prune_union_parent(
        self,
        parent_cte: UnionCTE,
        candidate: BuildConditional | BuildComparison | BuildParenthetical | None,
    ) -> bool:
        if not isinstance(candidate, BuildConceptArgs):
            return False
        row_conditions = {x.address for x in candidate.row_arguments}
        if not row_conditions:
            return False
        union_outputs = {x.address for x in parent_cte.output_columns}
        if not row_conditions.issubset(union_outputs):
            return False

        kept = []
        dropped = []
        for internal in parent_cte.internal_ctes:
            base = internal.source.base_datasource
            if (
                isinstance(base, BuildDatasource)
                and base.non_partial_for
                and conditions_mutually_exclusive(
                    candidate, base.non_partial_for.conditional
                )
            ):
                dropped.append(internal)
            else:
                kept.append(internal)
        if not dropped or not kept:
            return False

        self.log(
            f"Pruning union {parent_cte.name} from {len(parent_cte.internal_ctes)} "
            f"to {len(kept)} branch(es) using {candidate}"
        )
        parent_cte.internal_ctes = kept
        kept_identifiers = {cte.source.identifier for cte in kept}
        parent_cte.source.datasources = [
            source
            for source in parent_cte.source.datasources
            if source.identifier in kept_identifiers
        ]
        parent_cte.parent_ctes = unique(
            [parent for cte in kept for parent in cte.parent_ctes],
            "name",
        )
        return True

    def _check_parent(
        self,
        cte: CTE | UnionCTE,
        parent_cte: CTE | UnionCTE,
        candidate: BuildConditional | BuildComparison | BuildParenthetical | None,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ):
        if not isinstance(candidate, BuildConceptArgs):
            return False
        if isinstance(parent_cte, UnionCTE):
            pruned = self._prune_union_parent(parent_cte, candidate)
            pushed = self._push_into_union_branches(
                cte, parent_cte, candidate, inverse_map
            )
            return pruned or pushed
        if not isinstance(parent_cte, CTE):
            return False
        row_conditions = {x.address for x in candidate.row_arguments}
        existence_conditions = {
            y.address for x in candidate.existence_arguments for y in x
        }
        all_inputs = {x.address for x in candidate.concept_arguments}
        if is_child_of(candidate, parent_cte.condition):
            return False
        non_materialized = [k for k, v in parent_cte.source_map.items() if v == []]
        concrete = [
            x for x in parent_cte.output_columns if x.address in non_materialized
        ]
        if any(isinstance(x.lineage, BuildWindowItem) for x in concrete):
            self.debug(
                f"CTE {parent_cte.name} has window clause calculation, cannot push up to this without changing results"
            )
            return False
        materialized = {k for k, v in parent_cte.source_map.items() if v != []}

        if not row_conditions or not materialized:
            return False
        output_addresses = {x.address for x in parent_cte.output_columns}
        # if any of the existence conditions are created on the asset, we can't push up to it
        if existence_conditions and existence_conditions.intersection(output_addresses):
            return False
        if existence_conditions:
            self.log(
                f"Not pushing up existence {candidate} to {parent_cte.name} as it is a filter node"
            )
            if parent_cte.source.source_type == SourceType.FILTER:
                return False
        # if it's a root datasource, we can filter on _any_ of the output concepts
        if parent_cte.is_root_datasource:
            base = parent_cte.source.base_datasource
            assert base is not None  # is_root_datasource guarantees this
            extra_check = {x.address for x in base.output_concepts}
            if row_conditions.issubset(extra_check):
                for x in row_conditions:
                    if x not in materialized:
                        materialized.add(x)
                        parent_cte.source_map[x] = [base.name]
        if row_conditions.issubset(materialized):
            children = inverse_map.get(parent_cte.name, [])
            if all([is_child_of(candidate, child.condition) for child in children]):
                self.log(
                    f"All concepts [{row_conditions}] and existence conditions [{existence_conditions}] not block pushup of [{output_addresses}]found on {parent_cte.name} with existing {parent_cte.condition} and all it's {len(children)} children include same filter; pushing up {candidate}"
                )
                if parent_cte.condition and not is_scalar_condition(
                    parent_cte.condition
                ):
                    self.log("Parent condition is not scalar, not safe to push up")
                    return False
                if parent_cte.condition:
                    parent_cte.condition = append_condition(
                        parent_cte.condition, candidate
                    )
                else:
                    parent_cte.condition = candidate
                # promote up existence sources
                if all_inputs.difference(row_conditions):
                    for x in all_inputs.difference(row_conditions):
                        if x not in parent_cte.source_map and x in cte.source_map:
                            sources = [
                                parent
                                for parent in cte.parent_ctes
                                if parent.name in cte.source_map[x]
                            ]
                            parent_cte.source_map[x] = cte.source_map[x]
                            parent_cte.parent_ctes = unique(
                                parent_cte.parent_ctes + sources, "name"
                            )
                return True
        self.debug(
            f"conditions {row_conditions} not subset of parent {parent_cte.name} parent has {materialized} "
        )
        return False

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        # TODO - pushdown through unions
        if isinstance(cte, UnionCTE):
            return False, None
        optimized = False

        if not cte.parent_ctes:
            self.debug(f"No parent CTEs for {cte.name}")
            return False, None

        if not cte.condition:
            self.debug(f"No CTE condition for {cte.name}")
            return False, None

        if self.complete.get(cte.name):
            self.debug("Have done this CTE before")
            return False, None

        self.debug(
            f"Checking {cte.name} for predicate pushdown with {len(cte.parent_ctes)} parents"
        )
        if isinstance(cte.condition, BuildConditional):
            candidates = cte.condition.decompose()
        else:
            candidates = [cte.condition]
        self.debug(
            f"Have {len(candidates)} candidates to try to push down from parent {type(cte.condition)}"
        )
        optimized = False
        for candidate in candidates:
            if not is_scalar_condition(candidate):
                self.debug(
                    f"Skipping {candidate} as not a basic [no aggregate, etc] condition"
                )
                continue
            self.debug(
                f"Checking candidate {candidate}, {type(candidate)}, scalar: {is_scalar_condition(candidate)}"
            )
            for parent_cte in cte.parent_ctes:
                local_pushdown = self._check_parent(
                    cte=cte,
                    parent_cte=parent_cte,
                    candidate=candidate,
                    inverse_map=inverse_map,
                )
                optimized = optimized or local_pushdown
                if local_pushdown:
                    # taint a CTE again when something is pushed up to it.
                    self.complete[parent_cte.name] = False
                self.debug(
                    f"Pushed down {candidate} from {cte.name} to {parent_cte.name}"
                )

        self.complete[cte.name] = True
        return optimized, None


class PredicatePushdownRemove(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.complete: dict[str, bool] = {}

    def _atom_sourced_only_from_parents(self, atom, cte: CTE) -> bool:
        """True if every concept referenced by `atom` is materialized in `cte`
        via a parent CTE rather than via one of `cte`'s own joined base
        datasources. Atoms that reference inline-join columns must stay in
        `cte` because no upstream filter constrains those columns."""
        if not isinstance(atom, BuildConceptArgs):
            return False
        parent_names = {p.name for p in cte.parent_ctes}
        for c in atom.row_arguments:
            sources = cte.source_map.get(c.address)
            if not sources:
                return False
            if not all(s in parent_names for s in sources):
                return False
        return True

    def _atom_covered_by_applicable_parents(
        self, atom, cte: CTE, existence_only: set[str]
    ) -> bool:
        """True if every non-existence parent that exposes `atom`'s row
        arguments already enforces `atom`. Parents that don't expose those
        concepts are irrelevant — they can't filter on them."""
        atom_args = {c.address for c in atom.row_arguments}
        relevant_parents = [
            p
            for p in cte.parent_ctes
            if p.name not in existence_only
            and atom_args.issubset({x.address for x in p.output_columns})
        ]
        if not relevant_parents:
            return False
        return all(_parent_covers_condition(p, atom) for p in relevant_parents)

    def _parent_is_nullable_in_cte(self, cte: CTE, parent_name: str) -> bool:
        """True if ``parent_name`` is on the nullable side of any outer join
        on ``cte``. A nullable parent can be NULL-padded by the join, and
        rows whose filter column is NULL pass through a removed predicate
        but would have failed the original WHERE."""
        for j in cte.joins or []:
            if not isinstance(j, Join):
                continue
            if j.jointype == JoinType.INNER:
                continue
            # right side is nullable on FULL/LEFT_OUTER
            if j.jointype in (JoinType.FULL, JoinType.LEFT_OUTER):
                if (
                    isinstance(j.right_cte, (CTE, UnionCTE))
                    and j.right_cte.name == parent_name
                ):
                    return True
            # left side is nullable on FULL/RIGHT_OUTER
            if j.jointype in (JoinType.FULL, JoinType.RIGHT_OUTER):
                if (
                    isinstance(j.left_cte, (CTE, UnionCTE))
                    and j.left_cte.name == parent_name
                ):
                    return True
                # implicit left from joinkey_pairs.cte (inlined left case)
                for pair in j.joinkey_pairs or []:
                    cte_ref = getattr(pair, "cte", None)
                    if cte_ref is not None and cte_ref.name == parent_name:
                        return True
        # BaseJoins on cte.source.joins target datasources, not CTEs by
        # name, so they can't render a parent_name CTE nullable.
        return False

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, UnionCTE):
            return False, None
        optimized = False

        if not cte.parent_ctes:
            self.debug(f"No parent CTEs for {cte.name}")

            return False, None

        if not cte.condition:
            self.debug(f"No CTE condition for {cte.name}")
            return False, None

        parent_filter_status = {
            parent.name: _parent_covers_condition(parent, cte.condition)
            for parent in cte.parent_ctes
        }
        # flatten existnce argument tuples to a list

        flattened_existence = [
            x.address for y in cte.condition.existence_arguments for x in y
        ]

        existence_only = [
            parent.name
            for parent in cte.parent_ctes
            if all([x.address in flattened_existence for x in parent.output_columns])
            and len(flattened_existence) > 0
        ]
        if all(
            [
                value
                for key, value in parent_filter_status.items()
                if key not in existence_only
            ]
        ) and not any([isinstance(x, BuildDatasource) for x in cte.source.datasources]):
            self.log(
                f"All parents of {cte.name} have same filter or are existence only inputs, removing filter from {cte.name}"
            )
            cte.condition = None
            # remove any "parent" CTEs that provided only existence inputs
            if existence_only:
                original = [y.name for y in cte.parent_ctes]
                cte.parent_ctes = [
                    x for x in cte.parent_ctes if x.name not in existence_only
                ]
                self.log(
                    f"new parents for {cte.name} are {[x.name for x in cte.parent_ctes]}, vs {original}"
                )
            return True, None

        # Per-conjunct removal: even when the full condition isn't covered (e.g.
        # cte joins dim tables inline that contribute their own filter atoms),
        # individual atoms whose row_arguments are sourced entirely from parent
        # CTEs and which are covered by every parent that exposes those concepts
        # are still redundant — predicate-pushdown already arranged for them to
        # be enforced upstream. The per-atom outer-join safety check happens
        # inside the loop: an atom whose source parent is on the nullable side
        # of an outer join in `cte` can't be removed (NULL-padding bypasses
        # the parent's filter).
        existence_set = set(existence_only)
        if (
            isinstance(cte.condition, BuildConditional)
            and cte.condition.operator == BooleanOperator.AND
        ):
            atoms = cte.condition.decompose()
        else:
            atoms = [cte.condition]
        if len(atoms) <= 1:
            self.complete[cte.name] = True
            return optimized, None
        surviving: list = []
        removed = 0
        for atom in atoms:
            if not (
                self._atom_sourced_only_from_parents(atom, cte)
                and self._atom_covered_by_applicable_parents(atom, cte, existence_set)
            ):
                surviving.append(atom)
                continue
            # Per-atom outer-join safety: if any source parent for this atom
            # is on the nullable side of an outer join in cte, NULL-padded
            # rows could bypass the predicate. Keep the atom in that case.
            atom_sources: set[str] = set()
            if isinstance(atom, BuildConceptArgs):
                for c in atom.row_arguments:
                    atom_sources.update(cte.source_map.get(c.address, []) or [])
            if any(self._parent_is_nullable_in_cte(cte, src) for src in atom_sources):
                surviving.append(atom)
                continue
            self.log(
                f"Removing redundant atom {atom} from {cte.name}: covered by parent CTE(s) and sourced only from parents"
            )
            removed += 1
        if removed:
            if surviving:
                cte.condition = rebuild_and_condition(surviving)
            else:
                cte.condition = None
            return True, None

        self.complete[cte.name] = True
        return optimized, None
