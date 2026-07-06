# q75 churn regression 155k→605k (run 20260706-222300) — NOT a framework regression

Run: `evals/tpcds_agent/results/20260706-222300` (enriched leg). Trilogy engine at HEAD
(commit `4e69c5547` `more_fixes`). All probes via `evals/common/scoring` +
`generate_sql`/`execute_raw_sql` against the run's own `workspace/tpcds.duckdb`.

## Headline verdict

**No framework bug, no regression, no silent codegen fault.** The agent's final
`workspace/query75.preql` **resolves, renders faithfully, and runs cleanly (exit 0, 100
rows)** on the current engine. The `fail` is a pure **correctness** miss from ONE authored
semantic delta — coalescing the *sale* side of the net measures to 0 — the SAME agent
mistake already documented in `diag_q75_enriched_20260706.md`. The +450k token jump is
**agent/model exploration variance**, not a framework obstacle: this run the model took 23
iterations (5 benign syntax-guidance errors) to hand-derive the query; the prior enriched
run took 9 iterations with 0 errors and landed essentially the same query.

## Metrics (target comparison is enriched→enriched, both use `all_sales`)

| run | leg | iters | tool_errors | tokens | outcome |
|---|---|---|---|---|---|
| 20260706-135542_enriched | enriched | 9 | 0 | 155k | fail (values) |
| 20260706-222300 | enriched | 23 | 5 | **605k** | fail (values) |
| 20260706-135542_ingest | ingest | 52 | 21 | 2.89M | fail (the old "disconnect" story) |

The prior "discovery-disconnect" deep-dive (`bug_q75_discovery_disconnect_deepdive.md`) was
about the **ingest** leg (no `all_sales` datasource → hand-rolled union + bare-agg
disconnects). This target is the **enriched** leg — it has `all_sales`, the agent used it,
and **hit ZERO disconnects this run**. The disconnect narrative does not apply here.

## The 5 errors this run (all benign, all correctly-rejected guidance friction, none new)

1. ran `all_sales.preql` directly → "Nothing was executed" (correct: no final select)
2. `count(*)` → Syntax [223] (correct: no `*` row-marker)
3. `group by` → Syntax [103] (correct: grouping is implicit)
4. `import raw/all_sales` (slash) → parse error `expected IMPORT_DOT`
5. file-write refused for the same slash import

**Zero Binder / Catalog / "Unexpected error" from generated SQL** — the class MEMORY.md
flags as always-framework. Per that same criterion, this run is exploration variance, not a
regression. The extra tokens are simply 23 iterations × a large constant context (the
~845-line model catalog re-sent every turn ≈ 27k/turn × 23 ≈ 605k). The iteration count,
not any engine fault, is the multiplier.

## Correctness root cause (unchanged from prior diag) + proof

Agent net measures (query75.preql:13-14):
`coalesce(all.quantity,0) - coalesce(all.return_quantity,0)` and same for
`ext_sales_price`/`return_amount`. Reference keeps the sale side **bare**:
`sales.quantity - coalesce(sales.return_quantity,0)` (`tests/modeling/tpc_ds_duckdb/query75.preql:13-14`).
NULL-quantity / NULL-price sale lines should drop from the SUM; coalescing them to 0 lets a
matched return push net *negative*.

Trigger matrix (agent structure, toggle only the sale-side coalesce; multiset symdiff vs
canonical `query75.sql`, 100 ref rows):

| variant | sale-side coalesce | rows | symdiff vs ref |
|---|---|---|---|
| agent as-authored | yes | 100 | **78** |
| agent, sale side bare | no | 100 | **0 (exact)** |

Removing only the two `coalesce(sale,0)` wrappers makes the agent's output **byte-set
identical** to the reference. That transitively proves: dedup was done correctly this run
(the `lines` rowset's 7-tuple GROUP BY == reference `deduped`), the `union join`→FULL JOIN +
both-sides-`is not null` filter is inner-equivalent to the reference LEFT-join+ratio-HAVING,
and codegen is faithful. The entire delta is the authored sale-side coalesce.

## Rule-outs

- FRAMEWORK codegen: fixed variant == reference exactly (symdiff 0) → no silent wrong-rows.
- Canonical `query75.preql` `generate_sql` succeeds on current engine (1 statement); its
  execute error under the probe is only a model-dir `memory.*` schema-attach mismatch, not a
  build fault. Reference `.sql` runs (100 rows).
- Commit `4e69c5547` added `except(...)`/`intersect(...)` TVFs + guidance (new feature). It
  did NOT alter the count(*)/group-by/import guardrails the agent tripped on. Its
  always-loaded overview now lists `intersect(...)` for "present in every source" (q75's
  "both years") — a *possible* future nudge, but the transcript shows the agent never
  fetched the `except-intersect-setops` example and never attempted a set-op form, so it did
  not drive this run's thrash.

## Classification

**Agent semantic error (correctness), proven.** Not a framework bug, not a regression, not a
disconnect. The token jump is model exploration variance.

## DX / guidance follow-up (would save the recurring ~450k, no code fix)

1. The dominant, repeated failure across both enriched runs is uninstructed **sale-side
   coalesce**. A model/prompt guidance line — "'treat missing X as zero' means coalesce ONLY
   X; never coalesce the sale-side measure (a NULL sale is meant to drop from the sum)" —
   directly targets the only wrong ingredient. (Same recommendation as
   `diag_q75_enriched_20260706.md`.)
2. Exploration friction (import-path slash, count(*), group-by) is benign but recurring;
   nothing framework-actionable.

## File:line references

- Defect (authored): `results/20260706-222300/workspace/query75.preql:13-14` (sale-side coalesce)
- Reference (correct): `tests/modeling/tpc_ds_duckdb/query75.preql:13-14`, `query75.sql`
- No `trilogy/` file is at fault.
