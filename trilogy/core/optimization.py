from __future__ import annotations

import heapq
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from trilogy.constants import CONFIG, Optimizations, logger
from trilogy.core.domain_graph import DomainGraph
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.optimizations import (
    CollapseSingleParent,
    HideUnusedConcepts,
    InlineDatasource,
    JoinHoist,
    MergeIrrelevantGroupBy,
    OptimizationRule,
    OrderInnerJoinsFirst,
    PredicatePushdown,
    PredicatePushdownRemove,
    PushFilteredAggregateInput,
    PushFilteredCountIntoJoin,
    PushSemiJoinIntoAggregate,
    SimplifyNullSafeJoins,
    StripRedundantNotNull,
    UnionDimPushdown,
    UpgradeJoinOnGuards,
    UpgradeOuterFromKeySetEquivalence,
    optimization_log,
)
from trilogy.core.optimizations.collapse_single_parent import (
    grouped_unbound_passthrough_should_wait,
)
from trilogy.core.optimizations.existence_having_fold import (
    FoldExistenceIntoAggregate,
)
from trilogy.core.optimizations.full_join_lowering import lower_full_joins
from trilogy.core.optimizations.identity_group import DropIdentityGroup
from trilogy.core.optimizations.join_upgrade import PrunePreservedJoinKeys
from trilogy.core.optimizations.reuse_parent_lookup import ReuseParentLookup
from trilogy.core.processing import plan_trace
from trilogy.core.processing.utility import sort_select_output_processed
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement
from trilogy.utility import unique

MAX_OPTIMIZATION_LOOPS = 10
MAX_OPTIMIZATION_PASSES = 10


@dataclass(frozen=True)
class OptimizationRulePlan:
    name: str
    rule_factory: Callable[[], OptimizationRule]
    depends_on: tuple[str, ...] = ()
    refires_after: tuple[str, ...] = ()
    reason: str = ""

    def make_rule(self) -> OptimizationRule:
        return self.rule_factory()


def canonicalize_graph(input: list[CTE]) -> None:
    """Make the CTE graph self-consistent.

    Optimization rules replace or fold CTEs, but other CTEs / joins can
    retain references to the old object. Rewrite every cross-reference to the
    single live instance keyed by name:

    - ``parent_ctes``: dedupe to the live object.
    - join endpoints (``right``/``left``/``pairs[].node``):
      resolve to the live emitted CTE, or to the consumer's folded
      ``inlined_parents`` instance so the render contract stays in sync.
    """
    emitted: dict[str, CTE | UnionCTE] = {c.name: c for c in input}
    inlined: dict[str, CTE | UnionCTE] = {}
    for c in input:
        if isinstance(c, CTE):
            inlined.update(
                {
                    p.name: p
                    for p in c.dependency_nodes(include_inlined=True)
                    if p.name not in emitted
                }
            )

    def resolve(node: CTE | UnionCTE) -> CTE | UnionCTE:
        # Never drop a reference: a missing target means another rule must
        # still resolve it.
        return emitted.get(node.name) or inlined.get(node.name) or node

    for cte in input:
        deduped: list[CTE | UnionCTE] = []
        seen: set[str] = set()
        for p in cte.dependency_nodes():
            live = resolve(p)
            if live.name in seen:
                continue
            seen.add(live.name)
            deduped.append(live)
        cte.parent_ctes = deduped
        joins = cte.joins if isinstance(cte, CTE) else []
        for join in joins:
            if not isinstance(join, Join):
                continue
            join.repoint(resolve)
        if isinstance(cte, UnionCTE):
            new_branches: list[CTE | UnionCTE] = []
            for binding in cte.source_bindings(include_branches=True):
                if not (binding.branch and binding.node is not None):
                    continue
                branch = binding.node
                live = resolve(branch)
                # A union arm renders inline and must project exactly this
                # union's columns. A same-named live CTE with a different
                # projection is a name collision (QDS identity ignores
                # projection), so the arm keeps its own instance.
                if live is not branch and {x.address for x in live.output_columns} != {
                    x.address for x in branch.output_columns
                }:
                    live = branch
                new_branches.append(live)
            cte.internal_ctes = new_branches
            cte.parent_ctes = unique(
                [
                    parent
                    for branch in cte.internal_ctes
                    for parent in branch.dependency_nodes()
                ],
                "name",
            )


def subquery_sources(
    cte: CTE | UnionCTE,
    lookup: dict[str, CTE] | dict[str, CTE | UnionCTE],
) -> list[CTE | UnionCTE]:
    """CTEs `cte` reads only from inside a subquery: existence membership
    feeders and generated semi-join feeders. The rendered SQL names them, but
    they are not row sources, so they never appear in ``dependency_nodes``."""
    if not isinstance(cte, CTE):
        return []
    names: list[str] = [
        source
        for sources in cte.existence_source_map.values()
        for source in sources or []
    ]
    names.extend(semi.feeder for semi in cte.semi_join_filters)
    out: list[CTE | UnionCTE] = []
    seen: set[str] = set()
    for name in names:
        node = lookup.get(name)
        if node is not None and name not in seen:
            seen.add(name)
            out.append(node)
    return out


def reorder_ctes(
    input: list[CTE],
):
    canonicalize_graph(input)
    # Stable topological order: among ready CTEs, earliest input position
    # first, so the result depends only on input order and the dependency
    # edge set, never on hash iteration order. Only nodes in the working set
    # participate; a parent that is not is sourced elsewhere and not emitted.
    position = {cte.name: index for index, cte in enumerate(input)}
    mapping: dict[str, CTE] = {cte.name: cte for cte in input}
    children: dict[str, set[str]] = {name: set() for name in mapping}
    indegree: dict[str, int] = {name: 0 for name in mapping}
    for cte in input:
        # A CTE read only through a subquery still has to be declared first:
        # a WITH list is sequential.
        for parent in [*cte.dependency_nodes(), *subquery_sources(cte, mapping)]:
            if parent.name in mapping and cte.name not in children[parent.name]:
                children[parent.name].add(cte.name)
                indegree[cte.name] += 1
    ready = [(position[name], name) for name, deg in indegree.items() if deg == 0]
    heapq.heapify(ready)
    ordered: list[CTE] = []
    while ready:
        _, name = heapq.heappop(ready)
        ordered.append(mapping[name])
        for child in children[name]:
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, (position[child], child))
    if len(ordered) != len(input):
        logger.error(
            "The graph is not a DAG (contains cycles) and cannot be topologically sorted."
        )
        raise ValueError("CTE dependency graph contains a cycle")
    return ordered


def filter_irrelevant_ctes(
    input: list[CTE | UnionCTE],
    root_cte: CTE | UnionCTE,
):
    relevant_ctes: set[str] = set()
    visited: set[str] = set()
    by_name: dict[str, CTE | UnionCTE] = {c.name: c for c in input}

    def recurse(cte: CTE | UnionCTE, emit: bool = True):
        if cte.name in visited:
            # A CTE first visited as a union branch (emit=False) and later
            # reached as a real parent needs its own WITH entry.
            if emit:
                relevant_ctes.add(cte.name)
            return
        visited.add(cte.name)
        if emit:
            relevant_ctes.add(cte.name)

        for parent in cte.dependency_nodes():
            recurse(parent)
        # Subquery feeders need WITH entries too.
        for node in subquery_sources(cte, by_name):
            recurse(node)
        if isinstance(cte, UnionCTE):
            for binding in cte.source_bindings(include_branches=True):
                if not binding.branch or binding.node is None:
                    continue
                # Branches render inside the union; only their parents need
                # standalone WITH entries.
                recurse(binding.node, emit=False)

    recurse(root_cte)
    final = [cte for cte in input if cte.name in relevant_ctes]
    filtered = [cte for cte in input if cte.name not in relevant_ctes]
    if filtered:
        logger.info(
            optimization_log(
                "FilterIrrelevantCTEs",
                f"Removing redundant CTEs {[x.name for x in filtered]}",
            )
        )
    return final


def gen_inverse_map(input: list[CTE | UnionCTE]) -> dict[str, list[CTE | UnionCTE]]:
    inverse_map: dict[str, list[CTE | UnionCTE]] = {}
    by_name = {c.name: c for c in input}
    for cte in input:
        if isinstance(cte, UnionCTE):
            dependencies = cte.dependency_nodes(include_branches=True)
        else:
            dependencies = cte.dependency_nodes()
        seen = {parent.name for parent in dependencies}
        # A subquery-only reference is a real consumer relationship: a merge
        # that repoints just the row consumers would leave it naming a dead
        # CTE. Counting it lets is_sole_consumer see it and repoint_consumers
        # rewrite it.
        for node in subquery_sources(cte, by_name):
            if node.name not in seen:
                dependencies.append(node)
                seen.add(node.name)
        for parent in dependencies:
            if parent.name not in inverse_map:
                inverse_map[parent.name] = []
            inverse_map[parent.name].append(cte)

    return inverse_map


def carry_root_contract(old_root: CTE | UnionCTE, new_root: CTE | UnionCTE) -> None:
    """The statement's ORDER BY, LIMIT and hidden set belong to whichever CTE
    is the root; a merge rule carries its child's WHERE and existence
    references but only a limited child's ordering."""
    new_root.order_by = old_root.order_by
    new_root.limit = old_root.limit
    new_root.hidden_concepts = new_root.hidden_concepts | old_root.hidden_concepts


@dataclass(frozen=True)
class _PlanContext:
    opts: Optimizations
    having_alias: bool
    domain_graph: DomainGraph | None


@dataclass(frozen=True)
class _Phase:
    name: str
    enabled: Callable[[Optimizations], bool]
    rule: Callable[[_PlanContext], OptimizationRule]
    # Phases this one must follow; a disabled one drops out.
    depends_on: tuple[str, ...] = ()
    refires_after: tuple[str, ...] = ()
    reason: str = ""


JOIN_UPGRADES = ("upgrade_join_on_guards.final", "upgrade_outer_key_set_equivalence")


def _pushdown(c: _PlanContext) -> OptimizationRule:
    return PredicatePushdown(having_alias=c.having_alias)


_PHASES: tuple[_Phase, ...] = (
    _Phase(
        "collapse_single_parent",
        lambda o: o.merge_aggregate,
        lambda c: CollapseSingleParent(domain_graph=c.domain_graph),
    ),
    _Phase(
        "merge_irrelevant_group_by",
        lambda o: o.merge_irrelevant_group_by,
        lambda c: MergeIrrelevantGroupBy(),
    ),
    _Phase(
        "inline_datasource",
        lambda o: o.datasource_inlining,
        lambda c: InlineDatasource(),
    ),
    _Phase(
        "join_hoist",
        lambda o: o.join_hoist,
        lambda c: JoinHoist(),
        depends_on=("inline_datasource",),
        reason=(
            "runs after datasource inlining so joins that target folded "
            "datasources stay folded when hoisted"
        ),
    ),
    _Phase(
        "predicate_pushdown.initial",
        lambda o: o.predicate_pushdown,
        _pushdown,
        depends_on=("inline_datasource", "join_hoist"),
        reason=(
            "runs after datasource inlining and join hoist so filters "
            "on folded raw sources stay local instead of requiring "
            "BuildDatasource pushdown"
        ),
    ),
    _Phase(
        "upgrade_join_on_guards.early",
        lambda o: o.upgrade_condition_joins,
        lambda c: UpgradeJoinOnGuards(left_only=True),
        depends_on=("predicate_pushdown.initial",),
        reason=(
            "makes guarded dim joins INNER before union dim pushdown "
            "tries to match them"
        ),
    ),
    _Phase(
        "union_dim_pushdown",
        lambda o: o.union_dim_pushdown,
        lambda c: UnionDimPushdown(),
        depends_on=("predicate_pushdown.initial", "upgrade_join_on_guards.early"),
        reason="matches settled consumer predicates and INNER dim joins",
    ),
    _Phase(
        "predicate_pushdown.remove",
        lambda o: o.predicate_pushdown,
        lambda c: PredicatePushdownRemove(),
        depends_on=("predicate_pushdown.initial",),
    ),
    _Phase(
        "collapse_single_parent.after_pushdown",
        lambda o: o.merge_aggregate and o.predicate_pushdown,
        # Without the domain graph `produces_unbound_rowset` treats every
        # aliased rowset output as unbound and this phase collapses nothing
        # across a rowset boundary.
        lambda c: CollapseSingleParent(domain_graph=c.domain_graph),
        depends_on=("predicate_pushdown.remove",),
        refires_after=("predicate_pushdown.remove",),
        reason=(
            "a per-contributor projection becomes a bare passthrough once "
            "predicate pushdown relocates its WHERE onto the parent scan, "
            "so re-collapse it then; a later pass is too late, the plan "
            "builds around the passthrough first"
        ),
    ),
    # A bare passthrough is pure noise unrelated to aggregate merging, so it
    # collapses even with merge_aggregate off. No refires_after: this path has
    # no initial collapse phase, so this sole phase must sweep passthroughs
    # left by planning or by any earlier phase, not only pushdown-created ones.
    _Phase(
        "collapse_single_parent.passthrough_after_pushdown",
        lambda o: not o.merge_aggregate and o.predicate_pushdown,
        lambda c: CollapseSingleParent(
            domain_graph=c.domain_graph, passthrough_only=True
        ),
        depends_on=("predicate_pushdown.remove",),
        reason=(
            "collapse bare passthroughs (pushdown residue or otherwise) "
            "when aggregate merging is disabled; sole collapse phase in "
            "this path, so it runs unconditionally"
        ),
    ),
    _Phase(
        "upgrade_join_on_guards.final",
        lambda o: o.upgrade_condition_joins,
        lambda c: UpgradeJoinOnGuards(),
        depends_on=("predicate_pushdown.remove",),
        reason="uses guards moved onto joining CTEs by predicate pushdown",
    ),
    _Phase(
        "predicate_pushdown.after_final_upgrade",
        lambda o: o.predicate_pushdown and o.upgrade_condition_joins,
        _pushdown,
        depends_on=("upgrade_join_on_guards.final",),
        refires_after=("upgrade_join_on_guards.final",),
        reason=(
            "HAVING-into-group push is blocked while a consumer "
            "outer-joins the group (nullable parent); rerun once the "
            "final pass has upgraded CTE-to-CTE outer joins to INNER "
            "so the relocation can fire"
        ),
    ),
    _Phase(
        "predicate_pushdown.remove.after_join_upgrades",
        lambda o: o.predicate_pushdown and o.upgrade_condition_joins,
        lambda c: PredicatePushdownRemove(after_join_upgrades=True),
        depends_on=("predicate_pushdown.after_final_upgrade",),
        refires_after=("predicate_pushdown.after_final_upgrade",),
        reason=(
            "the earlier remove pass runs before the final join "
            "upgrade, so predicates relocated by "
            "predicate_pushdown.after_final_upgrade keep a redundant "
            "copy at their origin; strip those too"
        ),
    ),
    _Phase(
        "upgrade_outer_key_set_equivalence",
        lambda o: o.upgrade_outer_key_set_equivalence,
        lambda c: UpgradeOuterFromKeySetEquivalence(
            domain_graph=c.domain_graph,
            narrow_equal_domain_joins=c.opts.narrow_equal_domain_joins,
        ),
        depends_on=("upgrade_join_on_guards.final",),
        reason=(
            "needs upstream filters in their final position so the "
            "accumulated-filter signatures on each side are stable"
        ),
    ),
    _Phase(
        "inline_datasource.after_join_upgrades",
        lambda o: o.datasource_inlining
        and (o.upgrade_condition_joins or o.upgrade_outer_key_set_equivalence),
        lambda c: InlineDatasource(raw_scope_only=True),
        depends_on=JOIN_UPGRADES,
        refires_after=JOIN_UPGRADES,
        reason=(
            "a raw() scan folds only where every result row carries one "
            "of its rows, which the initial pass has to read off "
            "provisional outer joins; retry once join types are final"
        ),
    ),
    _Phase(
        "push_filtered_count_into_join",
        lambda o: o.push_filtered_count_into_join,
        lambda c: PushFilteredCountIntoJoin(),
        depends_on=JOIN_UPGRADES,
        reason=(
            "runs after join types settle; a sole filtered COUNT over a "
            "left-joined side can move its filter into the join predicate"
        ),
    ),
    _Phase(
        "push_filtered_aggregate_input",
        lambda o: o.push_filtered_aggregate_input,
        lambda c: PushFilteredAggregateInput(),
        depends_on=JOIN_UPGRADES,
        reason=(
            "runs after consumers settle; filtered aggregate input can "
            "move before grouping when all consumers reject empty groups"
        ),
    ),
    _Phase(
        "simplify_null_safe_joins",
        lambda o: o.simplify_null_safe_joins,
        lambda c: SimplifyNullSafeJoins(),
        depends_on=JOIN_UPGRADES,
        reason=(
            "join types and CTE nullability are settled, so redundant "
            "null-safe join keys can be downgraded to ="
        ),
    ),
    _Phase(
        "strip_redundant_not_null",
        lambda o: o.strip_redundant_not_null,
        lambda c: StripRedundantNotNull(),
        depends_on=("simplify_null_safe_joins",),
        reason=(
            "join types and CTE nullability are settled, so an IS NOT "
            "NULL on a column that no outer join can pad is tautological; "
            "runs after null-safe-join simplification, which consumes "
            "those predicates as non-null proofs"
        ),
    ),
    _Phase(
        "drop_identity_group",
        lambda o: o.drop_identity_group,
        lambda c: DropIdentityGroup(),
        depends_on=JOIN_UPGRADES,
        reason=(
            "a GROUP BY a plan-time outer join required is a no-op DISTINCT "
            "once the joins are upgraded, so it runs after join types settle"
        ),
    ),
    _Phase(
        "reuse_parent_lookup",
        lambda o: o.reuse_parent_lookup,
        lambda c: ReuseParentLookup(),
        depends_on=JOIN_UPGRADES,
        reason=(
            "it reads join types and the holder's grouping, so it runs "
            "once both are settled and before output pruning hides the "
            "columns it carries"
        ),
    ),
    _Phase(
        "fold_existence_into_aggregate",
        lambda o: o.fold_existence_into_aggregate,
        lambda c: FoldExistenceIntoAggregate(),
        depends_on=JOIN_UPGRADES,
        reason=(
            "it reads settled INNER joins and the aggregate's final "
            "WHERE, and runs before output pruning hides the dimension "
            "columns the rebased scan keeps"
        ),
    ),
    _Phase(
        "hide_unused_concepts",
        lambda o: o.hide_unused_concepts,
        lambda c: HideUnusedConcepts(),
    ),
    _Phase(
        "push_semi_join_into_aggregate",
        lambda o: o.push_semi_join_into_aggregate,
        lambda c: PushSemiJoinIntoAggregate(),
        depends_on=("hide_unused_concepts", *JOIN_UPGRADES),
        reason=(
            "the mirror is only sound for a settled INNER join, and it "
            "reads the feeder's visible outputs, so it runs after join "
            "types and output pruning are final. Adds no CTE and rewrites "
            "no condition, so nothing downstream needs to re-fire"
        ),
    ),
    _Phase(
        "prune_preserved_join_keys",
        lambda o: o.upgrade_condition_joins or o.upgrade_outer_key_set_equivalence,
        lambda c: PrunePreservedJoinKeys(),
        depends_on=JOIN_UPGRADES,
        reason=(
            "a narrowed outer join preserves a side the planner had to "
            "coalesce, so it runs once join types are final, and before the "
            "INNER reorder the dropped pairs would pin"
        ),
    ),
    _Phase(
        "order_inner_joins_first",
        lambda o: o.order_inner_joins_first,
        lambda c: OrderInnerJoinsFirst(),
        depends_on=JOIN_UPGRADES,
        reason=(
            "runs last so join types are final (INNER<->OUTER upgrades have "
            "settled) before INNER joins are bubbled ahead of LEFT joins"
        ),
    ),
)


def build_optimization_rule_plan(
    having_alias: bool = False,
    domain_graph: DomainGraph | None = None,
) -> list[OptimizationRulePlan]:
    """The enabled phases in order. The plan repeats to a fixpoint, so a rule
    that an earlier phase ran sweeps up later phases' leftovers on the next
    pass without a re-fire phase of its own."""
    opts = CONFIG.optimizations
    context = _PlanContext(opts, having_alias, domain_graph)
    phases = [p for p in _PHASES if p.enabled(opts)]
    names = {p.name for p in phases}
    plan = [
        OptimizationRulePlan(
            name=p.name,
            rule_factory=partial(p.rule, context),
            depends_on=tuple(d for d in p.depends_on if d in names),
            refires_after=tuple(d for d in p.refires_after if d in names),
            reason=p.reason,
        )
        for p in phases
    ]
    validate_optimization_rule_plan(plan)
    return plan


def validate_optimization_rule_plan(plan: list[OptimizationRulePlan]) -> None:
    """Every ``depends_on`` / ``refires_after`` name must be an earlier phase."""
    seen: set[str] = set()
    for phase in plan:
        if phase.name in seen:
            raise ValueError(f"Optimization phase {phase.name!r} is registered twice")
        for dependency in (*phase.depends_on, *phase.refires_after):
            if dependency not in seen:
                raise ValueError(
                    f"Optimization phase {phase.name!r} depends on "
                    f"{dependency!r}, which does not run before it"
                )
        seen.add(phase.name)


def log_optimization_rule_plan(plan: list[OptimizationRulePlan]) -> None:
    if not plan:
        logger.info(optimization_log("RulePlan", "Rule plan is empty"))
        return
    lines = [optimization_log("RulePlan", "Rule plan:")]
    for idx, phase in enumerate(plan, start=1):
        deps = f" after={list(phase.depends_on)}" if phase.depends_on else ""
        refires = (
            f" refires_after={list(phase.refires_after)}" if phase.refires_after else ""
        )
        reason = f" ({phase.reason})" if phase.reason else ""
        lines.append(f"  {idx}. {phase.name}{deps}{refires}{reason}")
    logger.info("\n".join(lines))


def _optimization_visit_order(
    rule: OptimizationRule, ctes: list[CTE | UnionCTE]
) -> list[CTE | UnionCTE]:
    if not isinstance(rule, CollapseSingleParent):
        return ctes
    return sorted(
        ctes,
        key=lambda cte: (
            isinstance(cte, CTE)
            and grouped_unbound_passthrough_should_wait(cte, rule.domain_graph)
        ),
    )


def optimize_ctes(
    input: list[CTE | UnionCTE],
    root_cte: CTE | UnionCTE,
    select: SelectStatement | MultiSelectStatement,
    having_alias: bool = False,
    domain_graph: DomainGraph | None = None,
    supports_full_join: bool = True,
) -> list[CTE | UnionCTE]:
    # Materialize the statement's output contract before demand-driven rules run.
    # Rendering applies the same projection, but doing it only at render time makes
    # carried, non-selected root columns appear live to their parent CTEs.
    sort_select_output_processed(root_cte, select)

    state = _PlanState(input, root_cte)
    rule_plan = build_optimization_rule_plan(
        having_alias=having_alias,
        domain_graph=domain_graph,
    )
    log_optimization_rule_plan(rule_plan)
    # A later phase can leave work for an earlier one (a column pruned, a
    # WHERE moved), so the plan repeats until a pass changes nothing. Each
    # phase already loops to its own fixpoint, so on a later pass it reruns
    # only if some phase changed the plan since it last ran.
    changes = 0
    last_run: dict[str, int] = {}
    for pass_index in range(1, MAX_OPTIMIZATION_PASSES + 1):
        pass_changed: dict[str, bool] = {}
        for phase in rule_plan:
            if _phase_settled(
                phase, pass_index, pass_changed, last_run.get(phase.name), changes
            ):
                pass_changed[phase.name] = False
                continue
            name = phase.name if pass_index == 1 else f"{phase.name}#pass{pass_index}"
            changed = _run_phase(phase, name, state)
            changes += changed
            last_run[phase.name] = changes
            pass_changed[phase.name] = changed
        if not any(pass_changed.values()):
            break
    else:
        logger.warning(
            optimization_log(
                "Driver",
                f"plan still changing after MAX_OPTIMIZATION_PASSES="
                f"{MAX_OPTIMIZATION_PASSES}",
            )
        )

    input, root_cte = state.input, state.root_cte
    before = plan_trace.cte_snapshot([*input, root_cte])
    if not supports_full_join:
        # The rewrite adds CTEs and repoints FROM bases, so every join-type
        # and placement decision must already be final.
        input = lower_full_joins(input, root_cte)

    final = reorder_ctes(filter_irrelevant_ctes(input, root_cte))
    _trace_phase(
        "final sweep", "filter_irrelevant_ctes", 1, before, final, root_cte, {}
    )
    return final


@dataclass
class _PlanState:
    input: list[CTE | UnionCTE]
    root_cte: CTE | UnionCTE
    normalized: bool = False


def _phase_settled(
    phase: OptimizationRulePlan,
    pass_index: int,
    pass_changed: dict[str, bool],
    last_run: int | None,
    changes: int,
) -> bool:
    """A phase nothing has changed since it last ran has nothing new to act
    on. On the first pass a refire also waits for its triggers; a later pass
    sweeps up whatever any phase left, so a refire reruns there like any
    other phase."""
    if (
        pass_index == 1
        and phase.refires_after
        and not any(pass_changed.get(name, False) for name in phase.refires_after)
    ):
        logger.info(
            optimization_log(
                "Driver",
                f"Skipping {phase.name}; refire triggers "
                f"{list(phase.refires_after)} made no changes",
            )
        )
        return True
    return last_run == changes


def _run_phase(phase: OptimizationRulePlan, name: str, state: _PlanState) -> bool:
    rule = phase.make_rule()
    before = plan_trace.cte_snapshot([*state.input, state.root_cte])
    phase_merged: dict[str, str] = {}
    loops = 0
    complete = False
    phase_changed = False
    while not complete and (loops <= MAX_OPTIMIZATION_LOOPS):
        actions_taken = False
        look_at = unique([state.root_cte, *reversed(state.input)], property="name")
        look_at = _optimization_visit_order(rule, look_at)
        inverse_map = gen_inverse_map(look_at)
        for cte in look_at:
            opt, merged = rule.optimize(cte, inverse_map)
            actions_taken = actions_taken or opt
            if merged:
                phase_merged.update(merged)
                _remap_root(state, merged)
                state.input = [c for c in state.input if c.name not in merged]
        complete = not actions_taken
        phase_changed = phase_changed or actions_taken
        loops += 1
    if not complete:
        logger.warning(
            optimization_log(
                "Driver",
                f"{name} hit MAX_OPTIMIZATION_LOOPS={MAX_OPTIMIZATION_LOOPS} "
                "without converging",
            )
        )
    # an unchanged phase leaves an already-normalized list as it was
    if phase_changed or not state.normalized:
        state.input = reorder_ctes(filter_irrelevant_ctes(state.input, state.root_cte))
        state.normalized = True
    _trace_phase(
        name,
        type(rule).__name__,
        loops,
        before,
        state.input,
        state.root_cte,
        phase_merged,
    )
    logger.info(
        optimization_log(
            "Driver",
            f"Finished {name} ({type(rule).__name__}) "
            f"after {loops} loop(s); changed={phase_changed}",
        )
    )
    return phase_changed


def _remap_root(state: _PlanState, merged: dict[str, str]) -> None:
    new_root_name = merged.get(state.root_cte.name)
    if new_root_name is None:
        return
    parent = next((c for c in state.input if c.name == new_root_name), None)
    if parent is None:
        return
    carry_root_contract(state.root_cte, parent)
    state.root_cte = parent
    logger.info(optimization_log("Driver", f"Remapped root_cte to {new_root_name}"))


def _trace_phase(
    phase: str,
    rule: str,
    loops: int,
    before: dict[str, plan_trace.CteTrace],
    after: list[CTE | UnionCTE],
    root_cte: CTE | UnionCTE,
    merged: dict[str, str],
) -> None:
    if not plan_trace.active():
        return
    step = plan_trace.optimizer_step(
        phase, rule, loops, before, plan_trace.cte_snapshot([*after, root_cte]), merged
    )
    plan_trace.note_removed_ctes(phase, rule, {t.name for t in step.removed}, merged)
    plan_trace.record(f"{phase} ({rule})", step)
