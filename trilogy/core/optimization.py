from collections.abc import Callable
from dataclasses import dataclass

from trilogy.constants import CONFIG, logger
from trilogy.core.enums import Derivation
from trilogy.core.models.execute import CTE, Join, RecursiveCTE, UnionCTE
from trilogy.core.optimizations import (
    CollapseSingleParent,
    HideUnusedConcepts,
    InlineDatasource,
    JoinHoist,
    MergeIrrelevantGroupBy,
    OptimizationRule,
    PredicatePushdown,
    PredicatePushdownRemove,
    SimplifyNullSafeJoins,
    StripRedundantNotNull,
    UnionDimPushdown,
    UpgradeJoinOnGuards,
    UpgradeOuterFromKeySetEquivalence,
    optimization_log,
)
from trilogy.core.processing.condition_utility import merge_conditions_and_dedup
from trilogy.core.processing.utility import sort_select_output
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement
from trilogy.utility import unique

MAX_OPTIMIZATION_LOOPS = 100


@dataclass(frozen=True)
class OptimizationRulePlan:
    name: str
    rule_factory: Callable[[], OptimizationRule]
    depends_on: tuple[str, ...] = ()
    refires_after: tuple[str, ...] = ()
    reason: str = ""

    def make_rule(self) -> OptimizationRule:
        return self.rule_factory()


# other optimizations may make a CTE a pure passthrough
# remove those
# def is_locally_irrelevant(cte: CTE) -> CTE | bool:
#     if not len(cte.parent_ctes) == 1:
#         return False
#     parent = cte.parent_ctes[0]
#     if not parent.output_columns == cte.output_columns:
#         return False
#     if cte.condition is not None:
#         return False
#     if cte.group_to_grain:
#         return False
#     if len(cte.joins)>1:
#         return False
#     return parent


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
            # UNION ALL arity invariant: every branch must project exactly the
            # union's columns. If a rule over-pruned one branch's
            # ``output_columns`` (its ``source_map`` still carries the data),
            # the branches diverge and the union renders invalid SQL (empty
            # SELECT / column-count mismatch). Re-align divergent branches to
            # the union's projection. Triggers ONLY when branches already
            # disagree (an already-broken union) so consistent unions — every
            # passing query — are untouched.
            plain = [b for b in cte.internal_ctes if isinstance(b, CTE)]
            if len(plain) > 1:

                def visible(b: CTE) -> list[str]:
                    return [
                        x.address
                        for x in b.output_columns
                        if x.address not in b.hidden_concepts
                    ]

                counts = {len(visible(b)) for b in plain}
                # ``union()``-concept branches legitimately project *different*
                # local concepts, but a valid UNION ALL still needs the same
                # column *count* per branch. Only when counts disagree is the
                # union definitively invalid — a rule over-pruned one branch's
                # projection. Re-align the short branch(es) to a healthy
                # sibling's projection (its source_map still carries the data).
                if len(counts) > 1:
                    ref = max(plain, key=lambda b: len(visible(b)))
                    ref_addrs = {x.address for x in ref.output_columns}
                    for b in plain:
                        if b is ref or len(visible(b)) == len(visible(ref)):
                            continue
                        sourceable = set(b.source_map.keys()) | {
                            x.address for x in b.output_columns
                        }
                        if not ref_addrs <= sourceable:
                            continue
                        b.output_columns = list(ref.output_columns)
                        b.hidden_concepts = set(ref.hidden_concepts)


def reorder_ctes(
    input: list[CTE],
):
    from trilogy.core import graph as nx

    canonicalize_graph(input)
    # Create a directed graph
    G = nx.DiGraph()
    mapping: dict[str, CTE] = {}
    for cte in input:
        mapping[cte.name] = cte
    for cte in input:
        for parent in cte.dependency_nodes():
            # Only order nodes that are in the working set; a parent that
            # isn't is sourced elsewhere (inlined/merged) and not emitted.
            if parent.name in mapping:
                G.add_edge(parent.name, cte.name)
    # Perform topological sort (only works for DAGs)
    try:
        topological_order = list(nx.topological_sort(G))
        if not topological_order:
            return input
        ordered = [mapping[x] for x in topological_order if x in mapping]
        # never silently drop a working-set CTE that had no graph edges
        ordered_names = {c.name for c in ordered}
        ordered.extend(c for c in input if c.name not in ordered_names)
        return ordered
    except nx.NetworkXUnfeasible as e:
        logger.error(
            "The graph is not a DAG (contains cycles) and cannot be topologically sorted."
        )
        raise e


def filter_irrelevant_ctes(
    input: list[CTE | UnionCTE],
    root_cte: CTE | UnionCTE,
):
    relevant_ctes: set[str] = set()
    visited: set[str] = set()

    def recurse(
        cte: CTE | UnionCTE,
        inverse_map: dict[str, list[CTE | UnionCTE]],
        emit: bool = True,
    ):
        # TODO: revisit this
        # if parent := is_locally_irrelevant(cte):
        #     logger.info(
        #         optimization_log("FilterIrrelevantCTEs", f"Removing redundant CTE {cte.name} and replacing with {parent.name}")
        #     )
        #     for child in inverse_map.get(cte.name, []):
        #         child.parent_ctes = [
        #             x for x in child.parent_ctes if x.name != cte.name
        #         ] + [parent]
        #         for x in child.source_map:
        #             if cte.name in child.source_map[x]:
        #                 child.source_map[x].remove(cte.name)
        #                 child.source_map[x].append(parent.name)
        #         for x2 in child.existence_source_map:
        #             if cte.name in child.existence_source_map[x2]:
        #                 child.existence_source_map[x2].remove(cte.name)
        #                 child.existence_source_map[x2].append(parent.name)
        # else:
        if cte.name in visited:
            # Promote to emitted if we now reach this CTE as a real parent.
            # A CTE first visited as a union branch (emit=False) and later
            # reached via parent_ctes of some sibling needs its own WITH entry
            # — without this it gets filtered out while consumers still
            # reference its name. The visited-set still prevents re-traversal.
            if emit:
                relevant_ctes.add(cte.name)
            return
        visited.add(cte.name)
        if emit:
            relevant_ctes.add(cte.name)

        for parent in cte.dependency_nodes():
            recurse(parent, inverse_map)
        if isinstance(cte, UnionCTE):
            for binding in cte.source_bindings(include_branches=True):
                if not binding.branch or binding.node is None:
                    continue
                # Branches render inside the union; only their parents need
                # standalone WITH entries.
                recurse(binding.node, inverse_map, emit=False)

    inverse_map = gen_inverse_map(input)
    recurse(root_cte, inverse_map)
    final = [cte for cte in input if cte.name in relevant_ctes]
    filtered = [cte for cte in input if cte.name not in relevant_ctes]
    if filtered:
        logger.info(
            optimization_log(
                "FilterIrrelevantCTEs",
                f"Removing redundant CTEs {[x.name for x in filtered]}",
            )
        )
    if len(final) == len(input):
        return input
    return filter_irrelevant_ctes(final, root_cte)


def gen_inverse_map(input: list[CTE | UnionCTE]) -> dict[str, list[CTE | UnionCTE]]:
    inverse_map: dict[str, list[CTE | UnionCTE]] = {}
    for cte in input:
        if isinstance(cte, UnionCTE):
            dependencies = cte.dependency_nodes(include_branches=True)
        else:
            dependencies = cte.dependency_nodes()
        for parent in dependencies:
            if parent.name not in inverse_map:
                inverse_map[parent.name] = []
            inverse_map[parent.name].append(cte)

    return inverse_map


SENSITIVE_DERIVATIONS = [
    Derivation.UNNEST,
    Derivation.WINDOW,
    Derivation.RECURSIVE,
]


def _grains_equivalent(cte: CTE | UnionCTE, direct_parent: CTE | UnionCTE) -> bool:
    """Strict grain equality, with a pseudonym-aware fallback.

    A pure projection that only renames or duplicates grain columns (e.g.
    q39 selecting one key twice as ``isk1``/``isk2``) has a grain whose
    component *addresses* differ from the parent's but which covers the same
    underlying concepts. Treat those as equal so the redundant projection
    CTE can still collapse. Additive: never rejects a previously-equal pair.
    """
    if direct_parent.grain == cte.grain:
        return True
    g1 = direct_parent.grain.components
    g2 = cte.grain.components
    if not g1 or not g2:
        return False
    concepts = {
        c.address: c
        for c in list(cte.source.output_concepts)
        + list(direct_parent.source.output_concepts)
    }

    def equiv_sets(components: set[str]) -> list[set[str]] | None:
        out: list[set[str]] = []
        for addr in components:
            concept = concepts.get(addr)
            if concept is None:
                return None
            out.append(concept.equivalent_addresses)
        return out

    e1 = equiv_sets(g1)
    e2 = equiv_sets(g2)
    if e1 is None or e2 is None:
        return False
    return all(any(s1 & s2 for s2 in e2) for s1 in e1) and all(
        any(s2 & s1 for s1 in e1) for s2 in e2
    )


def is_direct_return_eligible(cte: CTE | UnionCTE) -> CTE | UnionCTE | None:
    # if isinstance(select, (PersistStatement, MultiSelectStatement)):
    #     return False
    parents = cte.dependency_nodes()
    if len(parents) != 1:
        return None
    direct_parent = parents[0]
    if isinstance(direct_parent, (UnionCTE, RecursiveCTE)):
        return None

    if cte.group_to_grain:
        return None

    output_addresses = set([x.address for x in cte.output_columns])
    parent_output_addresses = set([x.address for x in direct_parent.output_columns])
    if not output_addresses.issubset(parent_output_addresses):
        return None
    if not _grains_equivalent(cte, direct_parent):
        logger.info(
            optimization_log("DirectReturn", "grain mismatch, cannot early exit")
        )
        return None

    assert isinstance(cte, CTE)
    derived_concepts = [
        c for c in cte.source.output_concepts if c not in cte.source.input_concepts
    ]

    parent_derived_concepts = [
        c
        for c in direct_parent.source.output_concepts
        if c not in direct_parent.source.input_concepts
    ]
    condition_arguments = cte.condition.row_arguments if cte.condition else []
    for x in derived_concepts:
        if x.derivation in SENSITIVE_DERIVATIONS:
            return None
    # `cte`'s condition collapses into the parent's SELECT scope. If the parent
    # derives a window (or other sensitive derivation), the predicate would then
    # evaluate in the same scope as that window — and SQL applies WHERE before
    # window functions, so a lead/lag/rank would see only the surviving rows
    # instead of the full series. Keep the scopes separate so the predicate
    # filters the window's OUTPUT (a HAVING applies after the window), whether or
    # not the predicate references the window itself.
    if cte.condition is not None:
        for x in parent_derived_concepts:
            if x.derivation in SENSITIVE_DERIVATIONS:
                return None
    for x in condition_arguments:
        # if it's derived in the parent
        if x.address in parent_derived_concepts:
            if x.derivation in SENSITIVE_DERIVATIONS:
                return None
            # this maybe needs to be recursive if we flatten a ton of derivation
            # into one CTE
            if not x.lineage:
                continue
            for z in x.lineage.concept_arguments:
                # if it was preexisting in the parent, it's safe
                if z.address in direct_parent.source.input_concepts:
                    continue
                # otherwise if it's dangerous, play it safe.
                if z.derivation in SENSITIVE_DERIVATIONS:
                    return None
    logger.info(
        optimization_log(
            "DirectReturn",
            f"Removing redundant output CTE {cte.name} with derived_concepts {[x.address for x in derived_concepts]}",
        )
    )
    return direct_parent


def pass_up_metadata(downstream: CTE | UnionCTE, upstream: CTE | UnionCTE):
    upstream.order_by = downstream.order_by
    upstream.limit = downstream.limit
    upstream.hidden_concepts = downstream.hidden_concepts.union(
        upstream.hidden_concepts
    )
    if downstream.condition:
        if upstream.condition:
            # Dedup on AND-atoms: a root remap can re-pass a predicate the
            # upstream CTE already carries, and stacking it raw compounds into
            # `H AND H AND H` across optimizer loops (q31's HAVING).
            upstream.condition = merge_conditions_and_dedup(
                downstream.condition, upstream.condition
            )
        else:
            upstream.condition = downstream.condition


def _enabled_dependencies(*names: tuple[str, bool]) -> tuple[str, ...]:
    return tuple(name for name, enabled in names if enabled)


def build_optimization_rule_plan(
    having_alias: bool = False,
    full_join_keys: set[str] | None = None,
) -> list[OptimizationRulePlan]:
    opts = CONFIG.optimizations
    full_join_keys = full_join_keys or set()
    plan: list[OptimizationRulePlan] = []

    if opts.merge_aggregate:
        plan.append(
            OptimizationRulePlan(
                name="collapse_single_parent",
                rule_factory=CollapseSingleParent,
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
                    ("inline_datasource", opts.datasource_inlining)
                ),
                reason=(
                    "runs after datasource inlining so joins that target folded "
                    "datasources stay folded when hoisted"
                ),
            )
        )
    if opts.merge_irrelevant_group_by and opts.join_hoist:
        plan.append(
            OptimizationRulePlan(
                name="merge_irrelevant_group_by.after_join_hoist",
                rule_factory=MergeIrrelevantGroupBy,
                depends_on=("join_hoist",),
                refires_after=("join_hoist",),
                reason="uses joins and predicates stripped by join hoist",
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
                rule_factory=CollapseSingleParent,
                depends_on=("predicate_pushdown.remove",),
                refires_after=("predicate_pushdown.remove",),
                reason=(
                    "a per-contributor projection becomes a bare passthrough once "
                    "predicate pushdown relocates its WHERE onto the parent scan, "
                    "so re-collapse it then (q81's dim-scan projection)"
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
    if opts.upgrade_outer_key_set_equivalence:
        plan.append(
            OptimizationRulePlan(
                name="upgrade_outer_key_set_equivalence",
                rule_factory=lambda: UpgradeOuterFromKeySetEquivalence(
                    full_join_keys=full_join_keys
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
    return plan


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


def optimize_ctes(
    input: list[CTE | UnionCTE],
    root_cte: CTE | UnionCTE,
    select: SelectStatement | MultiSelectStatement,
    having_alias: bool = False,
    full_join_keys: set[str] | None = None,
) -> list[CTE | UnionCTE]:
    direct_parent: CTE | UnionCTE | None = root_cte
    while CONFIG.optimizations.direct_return and (
        direct_parent := is_direct_return_eligible(root_cte)
    ):
        pass_up_metadata(root_cte, direct_parent)
        root_cte = direct_parent

        sort_select_output(root_cte, select)

    cte_lookup: dict[str, CTE | UnionCTE] = {c.name: c for c in input}
    cte_lookup[root_cte.name] = root_cte

    phase_actions: dict[str, bool] = {}
    rule_plan = build_optimization_rule_plan(
        having_alias=having_alias,
        full_join_keys=full_join_keys,
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
                            pass_up_metadata(root_cte, parent)
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
        input = reorder_ctes(filter_irrelevant_ctes(input, root_cte))
        phase_actions[phase.name] = phase_changed
        logger.info(
            optimization_log(
                "Driver",
                f"Finished {phase.name} ({type(rule).__name__}) "
                f"after {loops} loop(s); changed={phase_changed}",
            )
        )

    return reorder_ctes(filter_irrelevant_ctes(input, root_cte))
