# Bug: RecursionError when an outer aggregate over a union output column is aliased to that column's own name (q05)

**Status:** FIXED 2026-06-25 — the **named** form now *works* (no error). Root
cause: the named `with combined as union(...) -> (k, v)` registered its aligned
outputs in the bare local namespace (`local.k`), so a later `select ... as k`
(or `sum(combined.v) as v`) wrote the same address it read back through the
rowset wrapper → build cycle. Regular rowsets never hit this because their inner
SELECT aliases are already mangled to the hidden per-rowset name (`local._r_x`)
via `rowset_alias_scope`; the union's aligned outputs were the one rowset output
that bypassed that mangling. Fix: in `_lower_union` (`trilogy/parsing/v2/rules/
tvf_rules.py`), when lowering inside a rowset scope (the named form), mangle the
align output names to `local._{rowset}_{name}` so the user-facing `local.k` stays
free; the rowset wrapper unmangles back to `combined.k`. The **inline** form
(`from union(...) -> (k,v) select ...`) has no rowset scope and keeps bare names,
so a genuine self-alias there still raises cleanly (pre-existing guards in
`common.py`). Tests: `tests/engine/test_duckdb_rowset.py::
test_tvf_union_named_output_self_alias_resolves` (named works) and
`test_tvf_union_inline_output_self_alias_errors` (inline still errors).

Note: renaming a union output to *any* name (`combined.k as kk`) groups it to the
select grain and drops UNION ALL duplicates — pre-existing rename-grain behavior,
orthogonal to this fix; only the no-rename projection preserves arm duplicates.

---

(original report)

**Status:** OPEN — confirmed, deterministic repro on the current tree (after the union
cyclic-grain fix and the grouping-without-rollup fix).
**Surfaced by:** TPC-DS q05 enriched eval (agent message ~84/85).
**Severity:** HIGH — a valid-looking query stack-overflows `generate_sql` ("circular reference").

This is the **fourth distinct** q05 recursion and is NOT covered by the prior fixes
(`project_union_derived_output_recursion_bug`, `project_grouping_in_derived_concept_recursion_bug`,
`project_union_output_alias_self_reference`).

## Trigger (isolated)

An **outer aggregate over a `union(...)` output column, aliased back to that column's own name**:

```trilogy
import raw.all_sales as all_sales;
with stacked as union(
  (where all_sales.date.year = 2001 select all_sales.channel as channel, sum(all_sales.net_profit) as np),
  (where all_sales.date.year = 2002 select all_sales.channel as channel, sum(all_sales.return_amount) as np)
) -> (channel, np);
select stacked.channel, sum(stacked.np) as np limit 10;   -- (!) alias `np` == the column being summed
```

| Variant | Result |
|---|---|
| D: outer `sum(stacked.np) as np` (alias == summed output column) | **RECURSE** |
| E: outer `sum(stacked.np) as np2` (distinct alias) | OK |
| A: arm aliases `all_sales.channel as channel` (base-leaf name), distinct outer alias | OK |

So the recursion is driven by the **outer `as <name>` colliding with the union output column
`<name>` it aggregates** — NOT by the arm aliases (A builds fine). In the full q05 query the
collisions are `sum(stacked.total_returns) as total_returns` and `sum(stacked.net_profit) as
net_profit`; `channel`/`entity_id` are passed through (not aggregated) so they don't collide.

## Symptom (full query)

```
Recursion error building concept local.channel with grain Grain<Abstract> and lineage
UnionSelectLineage(selects=[SelectLineage(... ___tvf_arm_0_* ...), ...],
align=AlignClause(items=[AlignItem(alias='channel', ...), AlignItem(alias='net_profit', ...)]))
. This is likely due to a circular reference.
```

(The named concept in the message can be any output; the cycle is seeded by the self-aliased
aggregate output, e.g. `local.net_profit <- sum(stacked.net_profit)` where `stacked.net_profit`
resolves back through the alias.)

## Root cause (family)

Same family as the self-referential SELECT alias (`reference_planner_keys_by_address`: "`<expr> as
foo` reading `foo` can't compute → recursive-reference error") and
`project_union_output_alias_self_reference` (`rs.channel as channel`). Here the self-reference is
an **aggregate** (`sum(stacked.np) as np`) over a **union TVF output column**, and instead of a
clean recursive-reference error it **stack-overflows** in concept build — so the existing guard
(which fires for a bare alias whose source resolves to its own address) does not catch the
aggregate-wrapped form over a union output.

## Suggested fix

Extend the self-referential-alias guard to the union-output case: reject (clean error) or break
the cycle when an output `<name>` is defined by an expression (including an aggregate) that reads
the union output column also named `<name>`. The clean-error message should tell the agent to
alias to a distinct name (`as np2`). Defense in depth: a visiting guard in the concept-build walk
so a union output dimension can never stack-overflow (the same guard recommended for the other q05
recursions).

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260625-164230/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())   # RecursionError (variant D)
```
