from trilogy.core.enums import (
    BooleanOperator,
    JoinType,
    SetOperator,
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
from trilogy.core.models.execute import CTE, DatasourceCTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    append_condition,
    condition_contains_atom,
    strip_condition_atom,
)
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    condition_proves_non_null,
    condition_value_implies,
    conditions_mutually_exclusive,
    gather_windows,
    is_scalar_condition,
)
from trilogy.utility import unique


def is_child_of(a, comparison):
    return condition_contains_atom(a, comparison)


def _transitively_depends_on(node: CTE | UnionCTE, target_name: str) -> bool:
    """True if ``node`` reaches ``target_name`` through its dependency chain."""
    seen: set[str] = set()
    stack: list[CTE | UnionCTE] = list(node.dependency_nodes())
    while stack:
        dep = stack.pop()
        if dep.name == target_name:
            return True
        if dep.name in seen:
            continue
        seen.add(dep.name)
        stack.extend(dep.dependency_nodes())
    return False


def _predicate_safe_past_null_extension(
    candidate: BuildComparison | BuildConditional | BuildParenthetical,
    cte: CTE | UnionCTE,
    parent_cte: CTE | UnionCTE,
) -> bool:
    """A predicate that NULL operands can satisfy (`x IS NULL`, null-defaulted
    coalesce forms) may hold at this CTE precisely BECAUSE an outer join
    null-extended the parent's columns — a presence probe's `IS NULL` is true
    exactly on the rows the parent contributed nothing to. Evaluated inside
    the parent it reads the parent's real rows instead and inverts meaning
    (the parent's own probe column is never null, so the pushed filter empties
    the parent and the outer test trivially passes for every row). Only
    null-REJECTING predicates commute with a null-extending join: rows they
    keep must have real parent values, so pre- and post-join filtering agree."""
    if not isinstance(cte, CTE):
        return True
    null_extended = False
    for join in cte.joins:
        if not isinstance(join, Join):
            continue
        sides: list[str] = [join.right_cte.name]
        if join.left_cte is not None:
            sides.append(join.left_cte.name)
        if parent_cte.name not in sides:
            continue
        if join.jointype is JoinType.FULL:
            null_extended = True
        elif (
            join.jointype is JoinType.LEFT_OUTER
            and join.right_cte.name == parent_cte.name
        ):
            null_extended = True
        elif (
            join.jointype is JoinType.RIGHT_OUTER
            and join.left_cte is not None
            and join.left_cte.name == parent_cte.name
        ):
            null_extended = True
    if not null_extended:
        return True
    proven = condition_proves_non_null(candidate)
    return {x.address for x in candidate.row_arguments} <= proven


def _promotion_would_cycle(
    target: CTE | UnionCTE, sources: list[CTE | UnionCTE]
) -> bool:
    """True if making ``target`` depend on any of ``sources`` closes a cycle.

    Promoting an existence source adds a ``target -> source`` edge. If a source
    already (transitively) depends on ``target`` — the symmetric case where two
    existence filters on the same concept push into each other's producers — the
    new edge makes the CTE graph non-acyclic and ``reorder_ctes`` cannot sort it.
    """
    return any(
        s.name == target.name or _transitively_depends_on(s, target.name)
        for s in sources
    )


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


def _parent_materialized_addrs(parent: CTE | UnionCTE) -> set[str]:
    """Addresses a parent CTE exposes as plain output columns.

    For a plain ``CTE``, a non-empty ``source_map`` entry means the concept is
    pulled in from upstream rather than derived inline — i.e. its value lives
    in the CTE's projection as a column, not as an inline aggregate. A
    ``UnionCTE`` exposes whatever appears in its output columns. We hand this
    set to ``is_scalar_condition`` so an aggregate-derived concept that's
    already materialized in the parent counts as scalar in *that* parent's
    context, unblocking WHERE-pushdown of predicates the parent could
    actually evaluate against a column reference.
    """
    if isinstance(parent, UnionCTE):
        return {c.address for c in parent.output_columns}
    return {addr for addr, sources in parent.source_map.items() if sources}


def _parent_nullable_in_cte(cte: CTE, parent_name: str) -> bool:
    """True if ``parent_name`` is on the nullable side of any outer join on
    ``cte``. A nullable parent can be NULL-padded by the join, so rows whose
    filter column is NULL slip through a removed predicate but would have
    failed the original WHERE."""
    for j in cte.joins or []:
        if not isinstance(j, Join):
            continue
        if j.jointype == JoinType.INNER:
            continue
        if j.jointype in (JoinType.FULL, JoinType.LEFT_OUTER):
            if (
                isinstance(j.right_cte, (CTE, UnionCTE))
                and j.right_cte.name == parent_name
            ):
                return True
        if j.jointype in (JoinType.FULL, JoinType.RIGHT_OUTER):
            if (
                isinstance(j.left_cte, (CTE, UnionCTE))
                and j.left_cte.name == parent_name
            ):
                return True
            for pair in j.joinkey_pairs or []:
                if pair.cte.name == parent_name:
                    return True
    return False


def _consumer_may_emit_without_parent(cte: CTE, parent_name: str) -> bool:
    for j in cte.joins or []:
        if not isinstance(j, Join):
            continue
        if j.jointype == JoinType.FULL:
            return True
        if j.jointype != JoinType.RIGHT_OUTER:
            continue
        if isinstance(j.right_cte, (CTE, UnionCTE)) and j.right_cte.name == parent_name:
            continue
        return True
    return False


def _predicate_safe_past_windows(candidate, cte: CTE | UnionCTE) -> bool:
    """True if ``candidate`` can be pushed into/below ``cte`` without changing
    the result of any window function ``cte`` computes.

    A row predicate may only cross a window when it references *only* that
    window's PARTITION BY keys: dropping whole partitions leaves every surviving
    row's window value unchanged. A predicate on an ORDER BY key or any other
    input (e.g. q59's ``year_flag``, which is the lead's order key and feeds the
    partition expression but is not itself a partition key) changes
    lead/lag/rank results, so it must stay above the window. Window virt-concepts
    aren't in ``source_map``, so detection keys off output-column lineage —
    including windows nested inside arithmetic (``sum(x) / lead(sum(x), N)``),
    whose column lineage is a function with the window one level down.
    """
    materialized = _parent_materialized_addrs(cte)
    window_lineages: list[BuildWindowItem] = []
    for col in cte.output_columns:
        window_lineages.extend(gather_windows(col.lineage, materialized))
    if not window_lineages:
        return True
    if not isinstance(candidate, BuildConceptArgs):
        return False
    row_args = {x.address for x in candidate.row_arguments}
    if not row_args:
        return False
    for lineage in window_lineages:
        partition = {p.address for p in lineage.over}
        if not row_args.issubset(partition):
            return False
    return True


def _predicate_safe_past_grouping(candidate, cte: CTE | UnionCTE) -> bool:
    """True if ``candidate`` can be applied inside ``cte`` without changing any
    aggregate it computes.

    A scalar predicate lands in a grouping CTE as WHERE — a pre-aggregation row
    filter — which preserves the aggregates only when the predicate is constant
    within each group, so it drops whole groups: every row argument must be a
    group key (or pseudonym of one), or a property whose keys are all group
    keys. Anything else filters rows *inside* groups and silently changes the
    aggregates (q81: a grain-key membership semijoin pushed into an
    avg-by-partition group averaged only the semijoin-surviving rows).

    This holds for every atom regardless of provenance: pushdown is an
    optimization pass, so a push that changes what a group computes is a
    semantics change, never a legal relocation. Pre-aggregation placement of a
    WHERE is discovery's job, not this pass's.
    """
    if isinstance(cte, UnionCTE) or not cte.group_to_grain:
        return True
    if not isinstance(candidate, BuildConceptArgs):
        return False
    group_addrs: set[str] = set()
    for key in cte.group_concepts:
        group_addrs.add(key.address)
        group_addrs.update(key.pseudonyms)
    for arg in candidate.row_arguments:
        if arg.address in group_addrs or set(arg.pseudonyms) & group_addrs:
            continue
        if arg.keys and set(arg.keys).issubset(group_addrs):
            continue
        return False
    return True


class PredicatePushdown(OptimizationRule):
    def __init__(self, having_alias: bool = False) -> None:
        super().__init__()
        self.complete: dict[str, bool] = {}
        # only relocate aggregate predicates into a group HAVING when the
        # target dialect can reference SELECT aliases there
        self.having_alias = having_alias

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
        # EXCEPT/INTERSECT arms participate in whole-row set comparison;
        # conservatively leave them untouched (pushing a filter on the compared
        # outputs is provably sound, but branch source_map extras are not).
        if parent_cte.operator != SetOperator.UNION_ALL.value:
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
            if not _predicate_safe_past_grouping(candidate, branch):
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
                sources = [p for p in cte.dependency_nodes() if p.name in origin]
                for source in sources:
                    branch.add_dependency(source)
                union_dependencies_changed = True
            self.log(
                f"Pushed {candidate} into union branch {branch.name} of {parent_cte.name}"
            )

        if union_dependencies_changed:
            # Re-derive union.parent_ctes so reorder_ctes sees the new edges.
            parent_cte.parent_ctes = unique(
                [
                    p
                    for branch in parent_cte.internal_ctes
                    for p in branch.dependency_nodes()
                ],
                "name",
            )
        return True

    def _prune_union_parent(
        self,
        parent_cte: UnionCTE,
        candidate: BuildConditional | BuildComparison | BuildParenthetical | None,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> bool:
        if not isinstance(candidate, BuildConceptArgs):
            return False
        # Dropping an arm of EXCEPT/INTERSECT changes the set result (for
        # EXCEPT even a provably-empty-under-filter subtracted arm is only
        # droppable if actually empty). Prune UNION ALL stacks only.
        if parent_cte.operator != SetOperator.UNION_ALL.value:
            return False
        children = inverse_map.get(parent_cte.name, [])
        if not children:
            return False
        if not all(is_child_of(candidate, child.condition) for child in children):
            return False
        row_conditions = {x.address for x in candidate.row_arguments}
        if not row_conditions:
            return False
        union_outputs = {x.address for x in parent_cte.output_columns}
        if not row_conditions.issubset(union_outputs):
            return False

        # Dropping branches changes the rows EVERY consumer sees. Only safe when
        # every consumer of the union already carries this filter — otherwise a
        # co-sourcing consumer that needs the other branches (e.g. an unfiltered
        # ``sum(... ? channel='STORE')`` reading the same union as a
        # ``channel='CATALOG'`` membership feeder) silently loses its rows.
        children = inverse_map.get(parent_cte.name, [])
        if not children or not all(
            is_child_of(candidate, child.condition) for child in children
        ):
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
            [parent for cte in kept for parent in cte.dependency_nodes()],
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
        # Never push a predicate into a row-limited parent: filtering before
        # the LIMIT changes which rows fill it (a rowset body `limit N` is a
        # semantic boundary, not a plan detail).
        if parent_cte.limit is not None:
            return False
        if isinstance(parent_cte, UnionCTE):
            pruned = self._prune_union_parent(parent_cte, candidate, inverse_map)
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
        if not _predicate_safe_past_windows(candidate, parent_cte):
            self.debug(
                f"CTE {parent_cte.name} computes a window whose result a pushed "
                f"{candidate} would change (predicate not on partition keys); not pushing"
            )
            return False
        if not _predicate_safe_past_grouping(candidate, parent_cte):
            self.debug(
                f"CTE {parent_cte.name} groups by keys that do not determine a pushed "
                f"{candidate} (would filter rows inside groups); not pushing"
            )
            return False
        if not _predicate_safe_past_null_extension(candidate, cte, parent_cte):
            self.debug(
                f"CTE {parent_cte.name} is null-extended by {cte.name}'s outer join "
                f"and {candidate} is not null-rejecting; not pushing"
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
                # Existence sources to promote onto the parent (computed before
                # any mutation so the cycle guard can veto the whole push).
                # The consumer may source an existence concept either from a
                # regular dependency CTE or from an already-inlined datasource
                # (datasource inlining runs before this pass) — propagate both
                # so the pushed subselect has a real source to render against
                # instead of a dangling CTE reference. A subselect-only feeder
                # is absent from ``source_map`` entirely and lives in
                # ``existence_source_map``; promote it into the same map.
                promotions: list[
                    tuple[
                        str, list[str], bool, list[CTE | UnionCTE], list[DatasourceCTE]
                    ]
                ] = []
                for x in all_inputs.difference(row_conditions):
                    if (
                        x in parent_cte.source_map
                        or x in parent_cte.existence_source_map
                    ):
                        continue
                    existence_only = x not in cte.source_map
                    if existence_only and x not in cte.existence_source_map:
                        continue
                    source_names = (
                        cte.existence_source_map[x]
                        if existence_only
                        else cte.source_map[x]
                    )
                    dep_sources = [
                        parent
                        for parent in cte.dependency_nodes()
                        if parent.name in source_names
                    ]
                    inlined_sources = (
                        [
                            ip
                            for s in source_names
                            if (ip := cte.inlined_parent_for_source(s)) is not None
                        ]
                        if isinstance(cte, CTE)
                        else []
                    )
                    resolved = {p.name for p in dep_sources} | {
                        ip.datasource.safe_identifier for ip in inlined_sources
                    }
                    # A source name backed by neither a dependency CTE nor an
                    # inlined datasource would render as a phantom table.
                    if any(s not in resolved for s in source_names):
                        self.log(
                            f"Not pushing {candidate} into {parent_cte.name}: "
                            f"existence source {x} ({source_names}) is not resolvable"
                        )
                        return False
                    promotions.append(
                        (x, source_names, existence_only, dep_sources, inlined_sources)
                    )
                if any(
                    _promotion_would_cycle(parent_cte, srcs)
                    for _, _, _, srcs, _ in promotions
                ):
                    self.log(
                        f"Not pushing {candidate} into {parent_cte.name}: existence "
                        "source already depends on it (would create a CTE cycle)"
                    )
                    return False
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
                for (
                    x,
                    source_names,
                    existence_only,
                    sources,
                    inlined_sources,
                ) in promotions:
                    if existence_only:
                        parent_cte.existence_source_map[x] = list(source_names)
                    else:
                        parent_cte.source_map[x] = list(source_names)
                    for source in sources:
                        parent_cte.add_dependency(source)
                    for inlined in inlined_sources:
                        parent_cte.add_inlined_datasource(inlined)
                return True
        self.debug(
            f"conditions {row_conditions} not subset of parent {parent_cte.name} parent has {materialized} "
        )
        return False

    def _push_having_into_group_parent(
        self,
        cte: CTE | UnionCTE,
        parent_cte: CTE | UnionCTE,
        candidate: BuildConditional | BuildComparison | BuildParenthetical | None,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> bool:
        """Relocate a non-scalar (aggregate-result) predicate into a group
        parent's HAVING and strip the redundant copy from safe consumers.

        Filtering inside the group is identical to filtering its output
        downstream, and pushing it *before* the consumers' joins/aggregations
        prunes rows early (e.g. usa_names: filter names in the by-name group so
        the join back to the base table sees only qualifying names). The
        renderer splits a group CTE's condition into WHERE (scalar atoms) and
        HAVING (non-scalar atoms) and emits the HAVING by alias, so the
        relocation neither changes results nor bloats the SQL.

        Safe because: the predicate's columns are produced by ``parent_cte``
        (a real GROUP BY); every consumer already AND-carries the atom; and no
        consumer leaves ``parent_cte`` on the nullable side of an outer join.
        The downstream copy is only redundant when each consumer row must map
        to a surviving HAVING-passed group row.
        """
        if not isinstance(parent_cte, CTE):
            return False
        # Relocating a predicate into a row-limited group applies it before
        # the LIMIT, changing which rows fill it.
        if parent_cte.limit is not None:
            return False
        # A HAVING is only valid where a GROUP BY is emitted.
        if not parent_cte.group_to_grain:
            return False
        if not isinstance(candidate, BuildConceptArgs):
            return False
        # existence/subselect predicates are out of scope for this case
        if any(arg for group in candidate.existence_arguments for arg in group):
            return False
        row_conditions = {x.address for x in candidate.row_arguments}
        if not row_conditions:
            return False
        # the filtered columns must be produced by this group so HAVING can
        # reference them (and so we aren't pushing a row filter past the group)
        output_addresses = {x.address for x in parent_cte.output_columns}
        if not row_conditions.issubset(output_addresses):
            return False
        if is_child_of(candidate, parent_cte.condition):
            return False
        children = inverse_map.get(parent_cte.name, [])
        if not children:
            return False
        for child in children:
            # every consumer must AND-carry the atom (else filtering in the
            # group changes its result) and source the filtered columns from
            # this group, with the group non-nullable in the child (else a
            # NULL-padded row would bypass the now-removed predicate)
            if not isinstance(child, CTE):
                return False
            if not is_child_of(candidate, child.condition):
                return False
            if _parent_nullable_in_cte(child, parent_cte.name):
                return False
            # Relocating the predicate into the group parent applies it *before*
            # any window the consumer computes over the parent's rows (SQL WHERE
            # precedes window evaluation), changing lead/lag/rank results unless
            # it only drops whole partitions. See q59: filtering `year_flag` here
            # strips the year-2 rows the cross-year lead needs.
            if not _predicate_safe_past_windows(candidate, child):
                return False
            for addr in row_conditions:
                if parent_cte.name not in (child.source_map.get(addr) or []):
                    return False
        if parent_cte.condition:
            parent_cte.condition = append_condition(parent_cte.condition, candidate)
        else:
            parent_cte.condition = candidate
        retained_consumers: list[str] = []
        stripped_consumers: list[str] = []
        for child in children:
            assert isinstance(child, CTE)
            if _consumer_may_emit_without_parent(child, parent_cte.name):
                retained_consumers.append(child.name)
                continue
            child.condition = strip_condition_atom(child.condition, candidate)
            stripped_consumers.append(child.name)
        self.log(
            f"Relocated aggregate predicate {candidate} into group parent "
            f"{parent_cte.name} as HAVING; stripped redundant copy from "
            f"{stripped_consumers}; retained copy on {retained_consumers}"
        )
        return True

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        # TODO - pushdown through unions
        if isinstance(cte, UnionCTE):
            return False, None
        optimized = False

        parents = cte.dependency_nodes()
        if not parents:
            self.debug(f"No parent CTEs for {cte.name}")
            return False, None

        if not cte.condition:
            self.debug(f"No CTE condition for {cte.name}")
            return False, None

        if self.complete.get(cte.name):
            self.debug("Have done this CTE before")
            return False, None

        self.debug(
            f"Checking {cte.name} for predicate pushdown with {len(parents)} parents"
        )
        if isinstance(cte.condition, BuildConditional):
            candidates = cte.condition.decompose()
        else:
            candidates = [cte.condition]
        self.debug(
            f"Have {len(candidates)} candidates to try to push down from parent {type(cte.condition)}"
        )
        # CTEs feeding this consumer's existence subselects. Sibling membership
        # atoms of one AND-group are mutually redundant inside each other's
        # feeder sets (the final AND already intersects them), but each push
        # promotes the other feeder as a dependency — with N HAVING-derived
        # semijoins that chains feeder k over feeders 1..k-1: O(N^2) SQL and a
        # combinatorial explosion for the executing engine (q11 25GiB OOM).
        existence_feeders: set[str] = set()
        for sources in cte.existence_source_map.values():
            existence_feeders.update(sources)
        optimized = False
        for candidate in candidates:
            candidate_has_existence = isinstance(candidate, BuildConceptArgs) and any(
                arg for group in candidate.existence_arguments for arg in group
            )
            # Scalarity is *parent-relative*: a candidate like
            # ``manufact_matches > 0`` is non-scalar in the abstract (its concept
            # has aggregate lineage) but becomes scalar inside a parent CTE that
            # materializes ``manufact_matches`` as a plain output column — the
            # aggregate has already been computed there, the parent just
            # filters on its value. Pushing such filters as WHERE into a
            # non-aggregating parent prunes rows earlier and (the case that
            # motivated this) lets ``UpgradeJoinOnGuards`` see a
            # null-rejecting predicate next to the producing outer join.
            for parent_cte in parents:
                if candidate_has_existence and parent_cte.name in existence_feeders:
                    self.debug(
                        f"Not pushing existence predicate {candidate} into "
                        f"{parent_cte.name}: sibling existence feeder of {cte.name}"
                    )
                    continue
                parent_materialized = _parent_materialized_addrs(parent_cte)
                if is_scalar_condition(candidate, materialized=parent_materialized):
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
                elif self.having_alias:
                    # Non-scalar even for this parent: a true aggregate-result
                    # predicate. Only a group parent can carry it as HAVING.
                    local = self._push_having_into_group_parent(
                        cte=cte,
                        parent_cte=parent_cte,
                        candidate=candidate,
                        inverse_map=inverse_map,
                    )
                    optimized = optimized or local
                    if local:
                        self.complete[parent_cte.name] = False
                else:
                    self.debug(
                        f"Skipping non-scalar {candidate} into {parent_cte.name}; "
                        "dialect has no HAVING-by-alias support"
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
        parent_names = {p.name for p in cte.dependency_nodes()}
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
            for p in cte.dependency_nodes()
            if p.name not in existence_only
            and atom_args.issubset({x.address for x in p.output_columns})
        ]
        if not relevant_parents:
            return False
        return all(_parent_covers_condition(p, atom) for p in relevant_parents)

    def _parent_is_nullable_in_cte(self, cte: CTE, parent_name: str) -> bool:
        return _parent_nullable_in_cte(cte, parent_name)

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, UnionCTE):
            return False, None
        optimized = False

        parents = cte.dependency_nodes()
        if not parents:
            self.debug(f"No parent CTEs for {cte.name}")

            return False, None

        if not cte.condition:
            self.debug(f"No CTE condition for {cte.name}")
            return False, None

        parent_filter_status = {
            parent.name: _parent_covers_condition(parent, cte.condition)
            for parent in parents
        }
        # flatten existnce argument tuples to a list

        flattened_existence = [
            x.address for y in cte.condition.existence_arguments for x in y
        ]

        existence_only = [
            parent.name
            for parent in parents
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
                original = [y.name for y in parents]
                cte.parent_ctes = [x for x in parents if x.name not in existence_only]
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
                cte.condition = combine_condition_atoms(surviving)
            else:
                cte.condition = None
            return True, None

        self.complete[cte.name] = True
        return optimized, None
