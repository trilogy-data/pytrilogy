# q16 — enum "always true, should be removed" hard-error drops a join-load-bearing predicate (SILENT wrong result)

> **RESOLVED 2026-07-20** — removed the enum constant-satisfiability check entirely
> (comparison + membership + LIKE + BETWEEN) rather than attempt to prove transitive
> nullability through joins. Enum domains are *sampled* by auto-ingest, so "always
> true/false over the domain" reasoning was unsound to begin with. Enum comparisons
> now parse and generate SQL like any other comparison, so the load-bearing join
> null-rejection is preserved. Check removed from
> `trilogy/parsing/v2/rules/expression_rules.py` (+ `function_rules.py` `like()` call
> site); tests updated in `tests/engine/test_enum_unions.py`.

- **Run:** `evals/tpcds_agent/results/20260720-140600` (INGEST leg)
- **q16:** 1,626,984 tokens (baseline 720,301 — >2x), **FAIL** ("result set differs from reference"), 1 row both sides.
- **Classification:** FRAMEWORK bug. Silent wrong-result + token cascade.

## Symptom

q16 needs catalog sales "via a call center located in 'Williamson County'". In the
auto-ingested model the `call_center.county` column was sampled to a **single-value
non-nullable enum**: `county enum<string>['Williamson County']`
(`workspace/raw/call_center.preql:31`).

When the agent wrote the correct predicate `cs.call_center.county = 'Williamson County'`,
the parser raised a **hard blocking error** (exit_code 1, `agent_log.q16.conversation.txt`
msg 30):

> Syntax error: Comparison `cs.call_center.county = 'Williamson County'` matches every
> value of enum field 'cs.call_center.county' ... It is always true and **should be removed.**

The agent obeyed (msg 31: "removing it won't change the result") and deleted the **only
reference to `call_center`** from the query. Its final answer
(`workspace/query16.preql`) has **no call_center filter at all** — yet its own summary to
the user falsely claims it applied "call_center in Williamson County (enum constraint)".

## Why removal is wrong (the silent divergence)

`cs.call_center.county` is a **nested/joined dimension attribute**, reached through a
**nullable foreign key**: `cs_warehouse_sk`/`cs_call_center_sk: ?call_center.call_center_sk`
(`workspace/raw/catalog_sales.preql:53`). The reference SQL inner-joins
`catalog_sales -> call_center` (`cs1.cs_call_center_sk = cc_call_center_sk AND cc_county='Williamson County'`),
which **null-rejects** catalog_sales rows with a NULL `cs_call_center_sk`.

Removing the predicate drops that join entirely, so NULL-call_center rows leak into the
result. The enum value is "always true" only as a *local scalar*; as a *traversal* it also
enforces join existence. The framework advises removal without accounting for the
nullable-join path, so the answer silently changes.

## Minimal repro (against the run's `workspace/`, DuckDB)

`.venv/Scripts/python.exe -m trilogy.scripts.trilogy run <file>`

| Variant | Predicate on call_center | order_count | ext_ship_cost | net_profit | Result |
|---|---|---|---|---|---|
| **A** agent final | *(none — removed)* | 233 | **1100865.19** | **-140905.60** | parses, WRONG |
| **B** authored | `cs.call_center.county = 'Williamson County'` | — | — | — | **HARD PARSE ERROR** (blocked) |
| **C** retain join | `cs.call_center.call_center_sk is not null` | 233 | **1095837.99** | **-143869.23** | parses, matches ref join semantics |
| D isolate | `... call_center_sk is null` (leaked rows) | 3 | 5027.20 | 2963.63 | the exact delta |

A − C = 5027.20 ext_ship_cost and 2963.63 net_profit = **exactly** the 3 NULL-call_center
rows (variant D). Those 3 rows are what the reference's inner join excludes and the agent's
query wrongly includes. (`count(order_number)` is unchanged at 233 because those rows reuse
already-counted order numbers, so the divergence is invisible in the count column and only
shows in the sums — a genuinely silent failure.)

Scope probe (GA + ship-date window, pre set-filters): 611 orders, 607 with call_center, **4
NULL** — the null-rejection is real and load-bearing at this SF.

## Root cause (file:line)

`trilogy/parsing/v2/rules/expression_rules.py`

- `_enum_field` (line 231-240): derives nullability **solely** from the concept's own
  modifiers — `nullable = ... Modifier.NULLABLE in concept.modifiers` (line 238). It does
  **not** consider that `cs.call_center.county` is reached via a **nullable FK**
  (`?call_center.call_center_sk`), so a joined attribute looks "non-nullable / local".
- `_enum_violation` (line 268-276): with `nullable=False` it classifies a
  match-every-member comparison as `"tautology"` (not `"nullable_tautology"`).
- `_raise_enum_comparison` (line 279-300): for `"tautology"` raises `InvalidComparison`
  ("It is always true and should be removed") — a **hard error**, via
  `_validate_enum_comparison` (line 342) called from `comparison` (line 338).

The check treats an enum comparison as a pure scalar boolean and never accounts for the
**join/existence semantics** a comparison on a nested dimension attribute carries. For a
truly local non-null enum, "remove it" is sound; for a joined attribute (especially through
a nullable FK) it is not — removal silently drops inner-join null-rejection.

Note the irony: the **nullable** branch (line 294-299) would have told the agent to
"simplify it to '<field> is not null'" — which *preserves* the join and would have produced
the correct result. Because the ingested `county` enum is non-nullable, the agent instead
got the unsound "remove it" branch. A joined-attribute comparison should route to the
null-rejection branch (or not error) regardless of the leaf concept's own modifier.

## Secondary token driver (not the failure, but padded the 1.6M tokens)

Before hitting the enum error (~msg 30 of ~40), the agent burned most of its budget doubting
whether `warehouse` exists on `catalog_sales`: `explore` output does not surface the
imported/nullable-FK dimension `warehouse` (`catalog_sales.preql:18,56` — it IS there via
`?warehouse.warehouse_sk`), so the agent repeatedly re-explored web_sales/store_sales/inventory
(conversation lines ~2625-3837, ~5147-5173). This matches the known explore-hides-imported-dim-FK
guidance issue (memory q95). The enum hard-error then triggered further rewrite churn.

## Verdict

FRAMEWORK bug (silent wrong result). The enum-tautology satisfiability check should not
hard-error-and-advise-removal for a comparison on a **joined dimension attribute reached
through a nullable relationship**; that predicate carries load-bearing null-rejection. Do
NOT fix here (read-only task).
