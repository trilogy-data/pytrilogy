from collections.abc import Callable
from dataclasses import dataclass

from trilogy.constants import CONFIG, logger
from trilogy.core.enums import BooleanOperator, Derivation
from trilogy.core.models.build import (
    BuildConditional,
)
from trilogy.core.models.execute import CTE, RecursiveCTE, UnionCTE
from trilogy.core.optimizations import (
    CollapseSingleParent,
    HideUnusedConcepts,
    InlineDatasource,
    JoinHoist,
    MergeIrrelevantGroupBy,
    OptimizationRule,
    PredicatePushdown,
    PredicatePushdownRemove,
    UnionDimPushdown,
    UpgradeJoinOnGuards,
)
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


def reorder_ctes(
    input: list[CTE],
):
    from trilogy.core import graph as nx

    # Create a directed graph
    G = nx.DiGraph()
    mapping: dict[str, CTE] = {}
    for cte in input:
        mapping[cte.name] = cte
        for parent in cte.parent_ctes:
            G.add_edge(parent.name, cte.name)
    # Perform topological sort (only works for DAGs)
    try:
        topological_order = list(nx.topological_sort(G))
        if not topological_order:
            return input
        return [mapping[x] for x in topological_order]
    except nx.NetworkXUnfeasible as e:
        logger.error(
            "The graph is not a DAG (contains cycles) and cannot be topologically sorted."
        )
        raise e


def filter_irrelevant_ctes(
    input: list[CTE | UnionCTE],
    root_cte: CTE | UnionCTE,
):
    relevant_ctes = set()

    def recurse(cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]):
        # TODO: revisit this
        # if parent := is_locally_irrelevant(cte):
        #     logger.info(
        #         f"[Optimization][Irrelevent CTE filtering] Removing redundant CTE {cte.name} and replacing with {parent.name}"
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
        relevant_ctes.add(cte.name)

        for parent in cte.parent_ctes:
            if parent.name in relevant_ctes:
                logger.info(
                    f"[Optimization][Irrelevent CTE filtering] Already visited {parent.name} when visting {cte.name}, potential recursive dag"
                )
                continue

            recurse(parent, inverse_map)
        if isinstance(cte, UnionCTE):
            for internal in cte.internal_ctes:
                recurse(internal, inverse_map)

    inverse_map = gen_inverse_map(input)
    recurse(root_cte, inverse_map)
    final = [cte for cte in input if cte.name in relevant_ctes]
    filtered = [cte for cte in input if cte.name not in relevant_ctes]
    if filtered:
        logger.info(
            f"[Optimization][Irrelevent CTE filtering] Removing redundant CTEs {[x.name for x in filtered]}"
        )
    if len(final) == len(input):
        return input
    return filter_irrelevant_ctes(final, root_cte)


def gen_inverse_map(input: list[CTE | UnionCTE]) -> dict[str, list[CTE | UnionCTE]]:
    inverse_map: dict[str, list[CTE | UnionCTE]] = {}
    for cte in input:
        if isinstance(cte, UnionCTE):
            for internal in cte.internal_ctes:
                if internal.name not in inverse_map:
                    inverse_map[internal.name] = []
                inverse_map[internal.name].append(cte)
        else:
            for parent in cte.parent_ctes:
                if parent.name not in inverse_map:
                    inverse_map[parent.name] = []
                inverse_map[parent.name].append(cte)

    return inverse_map


SENSITIVE_DERIVATIONS = [
    Derivation.UNNEST,
    Derivation.WINDOW,
    Derivation.RECURSIVE,
]


def is_direct_return_eligible(cte: CTE | UnionCTE) -> CTE | UnionCTE | None:
    # if isinstance(select, (PersistStatement, MultiSelectStatement)):
    #     return False
    if len(cte.parent_ctes) != 1:
        return None
    direct_parent = cte.parent_ctes[0]
    if isinstance(direct_parent, (UnionCTE, RecursiveCTE)):
        return None

    if cte.group_to_grain:
        return None

    output_addresses = set([x.address for x in cte.output_columns])
    parent_output_addresses = set([x.address for x in direct_parent.output_columns])
    if not output_addresses.issubset(parent_output_addresses):
        return None
    if not direct_parent.grain == cte.grain:
        logger.info("[Direct Return] grain mismatch, cannot early exit")
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
    for x in parent_derived_concepts:
        if x.address not in condition_arguments:
            continue
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
        f"[Optimization][EarlyReturn] Removing redundant output CTE {cte.name} with derived_concepts {[x.address for x in derived_concepts]}"
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
            upstream.condition = BuildConditional(
                left=upstream.condition,
                operator=BooleanOperator.AND,
                right=downstream.condition,
            )
        else:
            upstream.condition = downstream.condition


def _enabled_dependencies(*names: tuple[str, bool]) -> tuple[str, ...]:
    return tuple(name for name, enabled in names if enabled)


def build_optimization_rule_plan() -> list[OptimizationRulePlan]:
    opts = CONFIG.optimizations
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
    if opts.join_hoist:
        plan.append(
            OptimizationRulePlan(
                name="join_hoist",
                rule_factory=JoinHoist,
                reason=(
                    "runs before datasource inlining so moved joins remain "
                    "visible as CTE joins"
                ),
            )
        )
    if opts.datasource_inlining:
        plan.append(
            OptimizationRulePlan(
                name="inline_datasource",
                rule_factory=InlineDatasource,
                depends_on=_enabled_dependencies(("join_hoist", opts.join_hoist)),
            )
        )
    if opts.predicate_pushdown:
        plan.append(
            OptimizationRulePlan(
                name="predicate_pushdown.initial",
                rule_factory=PredicatePushdown,
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
                rule_factory=PredicatePushdown,
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
        logger.info("[Optimization] Rule plan is empty")
        return
    lines = ["[Optimization] Rule plan:"]
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
    rule_plan = build_optimization_rule_plan()
    log_optimization_rule_plan(rule_plan)
    for phase in rule_plan:
        if phase.refires_after and not any(
            phase_actions.get(name, False) for name in phase.refires_after
        ):
            logger.info(
                f"[Optimization] Skipping {phase.name}; refire triggers "
                f"{list(phase.refires_after)} made no changes"
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
                                f"[Optimization] Remapped root_cte to {new_root_name}"
                            )
                    # Filter out merged CTEs from input
                    input = [c for c in input if c.name not in merged]
            complete = not actions_taken
            phase_changed = phase_changed or actions_taken
            loops += 1
        input = reorder_ctes(filter_irrelevant_ctes(input, root_cte))
        phase_actions[phase.name] = phase_changed
        logger.info(
            f"[Optimization] Finished {phase.name} ({type(rule).__name__}) "
            f"after {loops} loop(s); changed={phase_changed}"
        )

    return reorder_ctes(filter_irrelevant_ctes(input, root_cte))
