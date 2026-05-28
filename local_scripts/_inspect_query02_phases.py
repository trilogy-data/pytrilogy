"""Snapshot the CTEs at every stage of compile_sql for query02 to see when
the channel filter disappears."""

import sys
from pathlib import Path

sys.path.insert(0, "local_scripts")

from discovery_v4 import run_tpcds_query
from trilogy.core.enums import BooleanOperator
from trilogy.core.models.build import BuildConditional
from trilogy.core.optimization import optimize_ctes
from trilogy.core.processing.nodes import SelectNode
from trilogy.core.query_processor import datasource_to_cte, flatten_ctes
import trilogy.core.optimization as opt_mod


def dump_ctes(label: str, ctes, root_cte):
    print(f"\n=== {label} ===")
    seen = {c.name for c in ctes}
    seen.add(root_cte.name)
    all_ctes = list(ctes)
    if root_cte.name not in {c.name for c in all_ctes}:
        all_ctes.append(root_cte)
    for cte in all_ctes:
        kind = type(cte).__name__
        outs = sorted(c.address for c in cte.output_columns)
        hidden = sorted(cte.hidden_concepts) if cte.hidden_concepts else []
        cond = getattr(cte, "condition", None)
        cond_str = ""
        if cond:
            cond_str = str(cond)
            if len(cond_str) > 120:
                cond_str = cond_str[:117] + "..."
        parents = [p.name for p in cte.parent_ctes]
        print(f"  {kind} {cte.name}")
        print(f"    outputs ({len(outs)}): {outs}")
        if hidden:
            print(f"    hidden: {hidden}")
        if cond_str:
            print(f"    condition: {cond_str}")
        if parents:
            print(f"    parents: {parents}")
        if isinstance(cte, opt_mod.UnionCTE):
            for b in cte.internal_ctes:
                bouts = sorted(c.address for c in b.output_columns)
                bhide = sorted(b.hidden_concepts) if b.hidden_concepts else []
                bcond = getattr(b, "condition", None)
                bcond_str = str(bcond) if bcond else ""
                print(f"    branch {b.name}: outs={bouts}")
                if bhide:
                    print(f"      hidden: {bhide}")
                if bcond_str:
                    print(f"      condition: {bcond_str[:120]}")


# Monkey-patch optimize_ctes to dump per-phase
orig_optimize = opt_mod.optimize_ctes

def patched_optimize(input, root_cte, select, having_alias=False):
    # Copy of orig with phase-by-phase snapshot
    from trilogy.core.optimization import (
        CONFIG, MAX_OPTIMIZATION_LOOPS, build_optimization_rule_plan,
        filter_irrelevant_ctes, gen_inverse_map, is_direct_return_eligible,
        log_optimization_rule_plan, optimization_log, pass_up_metadata,
        reorder_ctes, sort_select_output, unique, logger,
    )
    direct_parent = root_cte
    while CONFIG.optimizations.direct_return and (
        direct_parent := is_direct_return_eligible(root_cte)
    ):
        pass_up_metadata(root_cte, direct_parent)
        root_cte = direct_parent
        sort_select_output(root_cte, select)
    dump_ctes("AFTER direct_return", input, root_cte)
    cte_lookup = {c.name: c for c in input}
    cte_lookup[root_cte.name] = root_cte
    phase_actions = {}
    rule_plan = build_optimization_rule_plan(having_alias=having_alias)
    for phase in rule_plan:
        if phase.refires_after and not any(
            phase_actions.get(name, False) for name in phase.refires_after
        ):
            phase_actions[phase.name] = False
            continue
        rule = phase.make_rule()
        loops = 0
        complete = False
        phase_changed = False
        while not complete and (loops <= MAX_OPTIMIZATION_LOOPS):
            actions_taken = False
            look_at = unique([root_cte, *reversed(input)], property="name")
            inverse_map = gen_inverse_map(look_at)
            for cte in look_at:
                opt, merged = rule.optimize(cte, inverse_map)
                actions_taken = actions_taken or opt
                if merged:
                    cte_lookup.update({c.name: c for c in input})
                    cte_lookup[root_cte.name] = root_cte
                    if root_cte.name in merged:
                        new_root_name = merged[root_cte.name]
                        if new_root_name in cte_lookup:
                            parent = cte_lookup[new_root_name]
                            pass_up_metadata(root_cte, parent)
                            root_cte = parent
                    input = [c for c in input if c.name not in merged]
            complete = not actions_taken
            phase_changed = phase_changed or actions_taken
            loops += 1
        input = reorder_ctes(filter_irrelevant_ctes(input, root_cte))
        phase_actions[phase.name] = phase_changed
        if phase_changed:
            dump_ctes(f"AFTER {phase.name}", input, root_cte)
    return reorder_ctes(filter_irrelevant_ctes(input, root_cte))


opt_mod.optimize_ctes = patched_optimize

# Now invoke the compile pipeline (copy of compile_sql logic so we hit our patch)
info, build_env, _, build_stmt = run_tpcds_query("02")
node = info.strategy_node.copy()
if build_stmt is not None and getattr(build_stmt, "having_clause", None):
    having = build_stmt.having_clause.conditional
    combined = BuildConditional(left=node.conditions, right=having, operator=BooleanOperator.AND) if node.conditions else having
    node = SelectNode(output_concepts=list(build_stmt.output_components), input_concepts=list(node.usable_outputs), parents=[node], environment=node.environment, partial_concepts=list(node.partial_concepts), conditions=combined)
node.hidden_concepts = set(build_stmt.hidden_components) if build_stmt else set()
node.ordering = build_stmt.order_by if build_stmt else None
node.rebuild_cache()
qds = node.resolve()
root_cte = datasource_to_cte(qds, build_env.cte_name_map)
raw_ctes = list(reversed(flatten_ctes(root_cte)))
seen = {}
for cte in raw_ctes:
    if cte.name not in seen:
        seen[cte.name] = cte
    else:
        seen[cte.name] = seen[cte.name] + cte
for cte in raw_ctes:
    cte.parent_ctes = [seen[x.name] for x in cte.parent_ctes]
deduped = list(seen.values())
if build_stmt is not None:
    root_cte.limit = build_stmt.limit
    root_cte.hidden_concepts = set(build_stmt.hidden_components)

dump_ctes("PRE-OPTIMIZE", deduped, root_cte)
deduped = patched_optimize(deduped, root_cte, build_stmt)
dump_ctes("FINAL", deduped, root_cte)
