import heapq
from collections.abc import Callable
from dataclasses import dataclass

from trilogy.constants import CONFIG, logger
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
from trilogy.core.optimizations.full_join_lowering import lower_full_joins
from trilogy.core.processing.utility import sort_select_output_processed
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement
from trilogy.utility import unique

MAX_OPTIMIZATION_LOOPS = 10


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

    Optimization rules (inline, merge, collapse) replace or fold CTEs, but
    other CTEs / joins can retain references to the *old* object. Those stale
    copies are the source of "table does not exist" / reorder ``KeyError``
    bugs. Rewrite every cross-reference to the single live instance keyed by
    name:

    - ``parent_ctes``: keep only references whose target is still in the
      working set (a folded/merged parent is sourced via inline/merge now);
      dedupe to the live object.
    - join endpoints (``right_cte``/``left_cte``/``joinkey_pairs[].cte``):
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
        # Sync to the single live instance; never drop a reference (a missing
        # target means another rule must still resolve it — dropping it would
        # corrupt reachability).
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
            join.right_cte = resolve(join.right_cte)
            if join.left_cte is not None:
                join.left_cte = resolve(join.left_cte)
            for pair in join.joinkey_pairs or []:
                if pair.cte is not None:
                    pair.cte = resolve(pair.cte)
        if isinstance(cte, UnionCTE):
            new_branches: list[CTE | UnionCTE] = []
            for binding in cte.source_bindings(include_branches=True):
                if not (binding.branch and binding.node is not None):
                    continue
                branch = binding.node
                live = resolve(branch)
                # A union arm renders inline and must project exactly this
                # union's columns. If the same-named live emitted CTE carries a
                # different projection — a name collision between this inline arm
                # and an unrelated standalone CTE over the same source+grain
                # (QDS identity ignores projection) — collapsing onto it would
                # force one projection onto both and corrupt the other consumer.
                # Keep the arm's own instance in that case.
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
    """CTEs `cte` reads only from inside a subquery — existence membership
    feeders and generated semi-join feeders. They are real references (the
    rendered SQL names them) but not row sources, so they never appear in
    ``dependency_nodes``."""
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
    # STABLE topological order: among ready CTEs, earliest input position
    # first. The result is a pure function of the input order and the
    # dependency EDGE SET — never of edge/hash iteration order, which is what
    # let sibling CTEs swap emission order between runs (PYTHONHASHSEED).
    # Only nodes in the working set participate; a parent that isn't is
    # sourced elsewhere (inlined/merged) and not emitted.
    position = {cte.name: index for index, cte in enumerate(input)}
    mapping: dict[str, CTE] = {cte.name: cte for cte in input}
    children: dict[str, set[str]] = {name: set() for name in mapping}
    indegree: dict[str, int] = {name: 0 for name in mapping}
    for cte in input:
        # A CTE read only through a subquery — an existence membership's feeder
        # or a generated semi-join's — still has to be DECLARED first: a WITH
        # list is sequential, so an unordered reference is "table not found".
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
            # Promote to emitted if we now reach this CTE as a real parent.
            # A CTE first visited as a union branch (emit=False) and later
            # reached via parent_ctes of some sibling needs its own WITH entry;
            # without this it gets filtered out while consumers still
            # reference its name. The visited-set still prevents re-traversal.
            if emit:
                relevant_ctes.add(cte.name)
            return
        visited.add(cte.name)
        if emit:
            relevant_ctes.add(cte.name)

        for parent in cte.dependency_nodes():
            recurse(parent)
        # A subquery renders `FROM <feeder>` for CTEs referenced only through
        # existence_source_map or a generated semi-join — those need WITH
        # entries too.
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
        # A source referenced ONLY from inside a subquery (existence membership
        # or generated semi-join) is a real consumer relationship: a merge that
        # repoints just the row consumers leaves that reference naming a dead
        # CTE. Count it so is_sole_consumer sees it and repoint_consumers (via
        # replace_dependency) rewrites it.
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


def _enabled_dependencies(*names: tuple[str, bool]) -> tuple[str, ...]:
    return tuple(name for name, enabled in names if enabled)


def build_optimization_rule_plan(
    having_alias: bool = False,
    domain_graph: DomainGraph | None = None,
) -> list[OptimizationRulePlan]:
    opts = CONFIG.optimizations
    plan: list[OptimizationRulePlan] = []

    if opts.merge_aggregate:
        plan.append(
            OptimizationRulePlan(
                name="collapse_single_parent",
                rule_factory=lambda: CollapseSingleParent(domain_graph=domain_graph),
            )
        )
    if opts.merge_irrelevant_group_by:
        plan.append(
            OptimizationRulePlan(
                name="merge_irrelevant_group_by",
                rule_factory=MergeIrrelevantGroupBy,
            )
        )
    if opts.datasource_inlining:
        plan.append(
            OptimizationRulePlan(
                name="inline_datasource",
                rule_factory=InlineDatasource,
            )
        )
    if opts.join_hoist:
        plan.append(
            OptimizationRulePlan(
                name="join_hoist",
                rule_factory=JoinHoist,
                depends_on=_enabled_dependencies(
                    ("inline_datasource", opts.datasource_inlining),
                ),
                reason=(
                    "runs after datasource inlining so joins that target folded "
                    "datasources stay folded when hoisted"
                ),
            )
        )
    if opts.predicate_pushdown:
        plan.append(
            OptimizationRulePlan(
                name="predicate_pushdown.initial",
                rule_factory=lambda: PredicatePushdown(having_alias=having_alias),
                depends_on=_enabled_dependencies(
                    ("inline_datasource", opts.datasource_inlining),
                    ("join_hoist", opts.join_hoist),
                ),
                reason=(
                    "runs after datasource inlining and join hoist so filters "
                    "on folded raw sources stay local instead of requiring "
                    "BuildDatasource pushdown"
                ),
            )
        )
    if opts.upgrade_condition_joins:
        plan.append(
            OptimizationRulePlan(
                name="upgrade_join_on_guards.base_join_only",
                rule_factory=lambda: UpgradeJoinOnGuards(base_join_only=True),
                depends_on=_enabled_dependencies(
                    ("predicate_pushdown.initial", opts.predicate_pushdown)
                ),
                reason=(
                    "makes guarded dim BaseJoins INNER before union dim pushdown "
                    "tries to match them"
                ),
            )
        )
    if opts.union_dim_pushdown:
        plan.append(
            OptimizationRulePlan(
                name="union_dim_pushdown",
                rule_factory=UnionDimPushdown,
                depends_on=_enabled_dependencies(
                    ("predicate_pushdown.initial", opts.predicate_pushdown),
                    (
                        "upgrade_join_on_guards.base_join_only",
                        opts.upgrade_condition_joins,
                    ),
                ),
                reason="matches settled consumer predicates and INNER dim joins",
            )
        )
    if opts.predicate_pushdown and opts.union_dim_pushdown:
        plan.append(
            OptimizationRulePlan(
                name="predicate_pushdown.after_union_dim",
                rule_factory=lambda: PredicatePushdown(having_alias=having_alias),
                depends_on=("union_dim_pushdown",),
                refires_after=("union_dim_pushdown",),
                reason=(
                    "only refires when union_dim_pushdown adds branch-visible "
                    "dim concepts"
                ),
            )
        )
    if opts.predicate_pushdown:
        plan.append(
            OptimizationRulePlan(
                name="predicate_pushdown.remove",
                rule_factory=PredicatePushdownRemove,
                depends_on=_enabled_dependencies(
                    (
                        "predicate_pushdown.after_union_dim",
                        opts.union_dim_pushdown,
                    ),
                    (
                        "predicate_pushdown.initial",
                        not opts.union_dim_pushdown,
                    ),
                ),
            )
        )
    if opts.merge_irrelevant_group_by and opts.predicate_pushdown:
        plan.append(
            OptimizationRulePlan(
                name="merge_irrelevant_group_by.after_predicate_remove",
                rule_factory=MergeIrrelevantGroupBy,
                depends_on=("predicate_pushdown.remove",),
                refires_after=("predicate_pushdown.remove",),
                reason="uses redundant predicates removed from grouped children",
            )
        )
    if opts.merge_aggregate and opts.predicate_pushdown:
        plan.append(
            OptimizationRulePlan(
                name="collapse_single_parent.after_pushdown",
                # Must carry the domain graph like the other two phases: without
                # it `produces_unbound_rowset` cannot tell a bound rowset key
                # from an unbound one and conservatively treats EVERY aliased
                # rowset output as unbound, so this phase collapses nothing that
                # crosses a rowset boundary — which is most of what it exists to
                # collapse.
                rule_factory=lambda: CollapseSingleParent(domain_graph=domain_graph),
                depends_on=("predicate_pushdown.remove",),
                refires_after=("predicate_pushdown.remove",),
                reason=(
                    "a per-contributor projection becomes a bare passthrough once "
                    "predicate pushdown relocates its WHERE onto the parent scan, "
                    "so re-collapse it then (q81's dim-scan projection)"
                ),
            )
        )
    elif not opts.merge_aggregate and opts.predicate_pushdown:
        # merge_aggregate gates the full CollapseSingleParent rule, but a bare
        # passthrough (single parent, no compute/WHERE/join/regroup) is pure
        # noise unrelated to aggregate merging -- e.g. the semijoin-projection
        # residue left once predicate pushdown relocates its WHERE onto the
        # parent scan. Collapse it even with aggregate
        # merging off. Deliberately UNCONDITIONAL (no refires_after): unlike the
        # merge_aggregate branch above, this path has no initial collapse phase,
        # so this sole phase must run regardless of whether pushdown made changes
        # -- it is positioned after `remove`, so it sweeps any passthrough left by
        # planning OR by an earlier optimization phase, not only pushdown-created
        # ones. Gating on `remove` would leak a passthrough whenever remove is a
        # no-op. It still loops to fixpoint internally like every phase.
        plan.append(
            OptimizationRulePlan(
                name="collapse_single_parent.passthrough_after_pushdown",
                rule_factory=lambda: CollapseSingleParent(
                    domain_graph=domain_graph, passthrough_only=True
                ),
                depends_on=("predicate_pushdown.remove",),
                reason=(
                    "collapse bare passthroughs (pushdown residue or otherwise) "
                    "when aggregate merging is disabled; sole collapse phase in "
                    "this path, so it runs unconditionally"
                ),
            )
        )
    if opts.upgrade_condition_joins:
        plan.append(
            OptimizationRulePlan(
                name="upgrade_join_on_guards.final",
                rule_factory=UpgradeJoinOnGuards,
                depends_on=_enabled_dependencies(
                    ("predicate_pushdown.remove", opts.predicate_pushdown)
                ),
                reason="uses guards moved onto joining CTEs by predicate pushdown",
            )
        )
    if opts.predicate_pushdown and opts.upgrade_condition_joins:
        plan.append(
            OptimizationRulePlan(
                name="predicate_pushdown.after_final_upgrade",
                rule_factory=lambda: PredicatePushdown(having_alias=having_alias),
                depends_on=("upgrade_join_on_guards.final",),
                refires_after=("upgrade_join_on_guards.final",),
                reason=(
                    "HAVING-into-group push is blocked while a consumer "
                    "outer-joins the group (nullable parent); rerun once the "
                    "final pass has upgraded CTE-to-CTE outer joins to INNER "
                    "so the relocation can fire"
                ),
            )
        )
        plan.append(
            OptimizationRulePlan(
                name="predicate_pushdown.remove.after_join_upgrades",
                rule_factory=lambda: PredicatePushdownRemove(after_join_upgrades=True),
                depends_on=("predicate_pushdown.after_final_upgrade",),
                refires_after=("predicate_pushdown.after_final_upgrade",),
                reason=(
                    "the earlier remove pass runs before the final join "
                    "upgrade, so predicates relocated by "
                    "predicate_pushdown.after_final_upgrade keep a redundant "
                    "copy at their origin; strip those too"
                ),
            )
        )
    if opts.upgrade_outer_key_set_equivalence:
        plan.append(
            OptimizationRulePlan(
                name="upgrade_outer_key_set_equivalence",
                rule_factory=lambda: UpgradeOuterFromKeySetEquivalence(
                    domain_graph=domain_graph,
                    narrow_equal_domain_joins=opts.narrow_equal_domain_joins,
                ),
                depends_on=_enabled_dependencies(
                    ("upgrade_join_on_guards.final", opts.upgrade_condition_joins)
                ),
                reason=(
                    "needs upstream filters in their final position so the "
                    "accumulated-filter signatures on each side are stable"
                ),
            )
        )
    if opts.push_filtered_count_into_join:
        plan.append(
            OptimizationRulePlan(
                name="push_filtered_count_into_join",
                rule_factory=PushFilteredCountIntoJoin,
                depends_on=_enabled_dependencies(
                    (
                        "upgrade_outer_key_set_equivalence",
                        opts.upgrade_outer_key_set_equivalence,
                    ),
                    ("upgrade_join_on_guards.final", opts.upgrade_condition_joins),
                ),
                reason=(
                    "runs after join types settle; a sole filtered COUNT over a "
                    "left-joined side can move its filter into the join predicate"
                ),
            )
        )
    if opts.push_filtered_aggregate_input:
        plan.append(
            OptimizationRulePlan(
                name="push_filtered_aggregate_input",
                rule_factory=PushFilteredAggregateInput,
                depends_on=_enabled_dependencies(
                    (
                        "upgrade_outer_key_set_equivalence",
                        opts.upgrade_outer_key_set_equivalence,
                    ),
                    ("upgrade_join_on_guards.final", opts.upgrade_condition_joins),
                ),
                reason=(
                    "runs after consumers settle; filtered aggregate input can "
                    "move before grouping when all consumers reject empty groups"
                ),
            )
        )
    if opts.simplify_null_safe_joins:
        plan.append(
            OptimizationRulePlan(
                name="simplify_null_safe_joins",
                rule_factory=SimplifyNullSafeJoins,
                depends_on=_enabled_dependencies(
                    ("upgrade_join_on_guards.final", opts.upgrade_condition_joins),
                    (
                        "upgrade_outer_key_set_equivalence",
                        opts.upgrade_outer_key_set_equivalence,
                    ),
                ),
                reason=(
                    "join types and CTE nullability are settled, so redundant "
                    "null-safe join keys can be downgraded to ="
                ),
            )
        )
    if opts.strip_redundant_not_null:
        plan.append(
            OptimizationRulePlan(
                name="strip_redundant_not_null",
                rule_factory=StripRedundantNotNull,
                depends_on=_enabled_dependencies(
                    ("simplify_null_safe_joins", opts.simplify_null_safe_joins),
                ),
                reason=(
                    "join types and CTE nullability are settled, so an IS NOT "
                    "NULL on a column that no outer join can pad is tautological; "
                    "runs after null-safe-join simplification, which consumes "
                    "those predicates as non-null proofs"
                ),
            )
        )
    if opts.hide_unused_concepts:
        plan.append(
            OptimizationRulePlan(
                name="hide_unused_concepts",
                rule_factory=HideUnusedConcepts,
            )
        )
    if opts.push_semi_join_into_aggregate:
        plan.append(
            OptimizationRulePlan(
                name="push_semi_join_into_aggregate",
                rule_factory=PushSemiJoinIntoAggregate,
                depends_on=_enabled_dependencies(
                    ("hide_unused_concepts", opts.hide_unused_concepts),
                    ("upgrade_join_on_guards.final", opts.upgrade_condition_joins),
                    (
                        "upgrade_outer_key_set_equivalence",
                        opts.upgrade_outer_key_set_equivalence,
                    ),
                ),
                reason=(
                    "the mirror is only sound for a settled INNER join, and it "
                    "reads the feeder's visible outputs — so it runs after join "
                    "types and output pruning are final. Adds no CTE and rewrites "
                    "no condition, so nothing downstream needs to re-fire"
                ),
            )
        )
    if opts.order_inner_joins_first:
        plan.append(
            OptimizationRulePlan(
                name="order_inner_joins_first",
                rule_factory=OrderInnerJoinsFirst,
                depends_on=_enabled_dependencies(
                    ("upgrade_join_on_guards.final", opts.upgrade_condition_joins),
                    (
                        "upgrade_outer_key_set_equivalence",
                        opts.upgrade_outer_key_set_equivalence,
                    ),
                ),
                reason=(
                    "runs last so join types are final (INNER<->OUTER upgrades have "
                    "settled) before INNER joins are bubbled ahead of LEFT joins"
                ),
            )
        )
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

    cte_lookup: dict[str, CTE | UnionCTE] = {c.name: c for c in input}
    cte_lookup[root_cte.name] = root_cte

    phase_actions: dict[str, bool] = {}
    rule_plan = build_optimization_rule_plan(
        having_alias=having_alias,
        domain_graph=domain_graph,
    )
    log_optimization_rule_plan(rule_plan)
    for phase in rule_plan:
        if phase.refires_after and not any(
            phase_actions.get(name, False) for name in phase.refires_after
        ):
            logger.info(
                optimization_log(
                    "Driver",
                    f"Skipping {phase.name}; refire triggers "
                    f"{list(phase.refires_after)} made no changes",
                )
            )
            phase_actions[phase.name] = False
            continue
        rule = phase.make_rule()
        loops = 0
        complete = False
        phase_changed = False
        while not complete and (loops <= MAX_OPTIMIZATION_LOOPS):
            actions_taken = False
            # assume we go through all CTEs once
            look_at = unique([root_cte, *reversed(input)], property="name")
            look_at = _optimization_visit_order(rule, look_at)
            inverse_map = gen_inverse_map(look_at)
            for cte in look_at:
                opt, merged = rule.optimize(cte, inverse_map)
                actions_taken = actions_taken or opt
                if merged:
                    cte_lookup.update({c.name: c for c in input})
                    cte_lookup[root_cte.name] = root_cte
                    # Remap root_cte if it was merged
                    if root_cte.name in merged:
                        new_root_name = merged[root_cte.name]

                        if new_root_name in cte_lookup:
                            parent = cte_lookup[new_root_name]
                            carry_root_contract(root_cte, parent)
                            root_cte = parent
                            logger.info(
                                optimization_log(
                                    "Driver",
                                    f"Remapped root_cte to {new_root_name}",
                                )
                            )
                    # Filter out merged CTEs from input
                    input = [c for c in input if c.name not in merged]
            complete = not actions_taken
            phase_changed = phase_changed or actions_taken
            loops += 1
        if not complete:
            logger.warning(
                optimization_log(
                    "Driver",
                    f"{phase.name} hit MAX_OPTIMIZATION_LOOPS={MAX_OPTIMIZATION_LOOPS} "
                    "without converging",
                )
            )
        input = reorder_ctes(filter_irrelevant_ctes(input, root_cte))
        phase_actions[phase.name] = phase_changed
        logger.info(
            optimization_log(
                "Driver",
                f"Finished {phase.name} ({type(rule).__name__}) "
                f"after {loops} loop(s); changed={phase_changed}",
            )
        )

    if not supports_full_join:
        # Last: the rewrite adds CTEs and repoints FROM bases, so every
        # join-type and placement decision must already be final.
        input = lower_full_joins(input, root_cte)

    return reorder_ctes(filter_irrelevant_ctes(input, root_cte))
