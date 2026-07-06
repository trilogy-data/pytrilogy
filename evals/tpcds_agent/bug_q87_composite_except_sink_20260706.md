# q87 token sink 443k→918k — `except(...)`/`intersect(...)` set-op arms are column-pruned (SILENT wrong result)

Run: `evals/tpcds_agent/results/20260706-222300`, q87 **FAIL, 917,645 tokens**
(prior enriched leg `20260706-135542_enriched` = 443k). `ref_rows=1 cand_rows=1`,
`detail="result set differs from reference"` — no hard error, silent wrong count.

## What caused the +475k jump: a NEW construct the guidance now STEERS agents into

Commit `4e69c5547` ("more_fixes") added the `except(...)` / `intersect(...)` relational
TVFs (new `SetOperator` enum, `UnionSelectLineage.operator`, `UnionNode.set_operator`,
`UnionCTE` render via `SET_OPERATOR_MAP`) **and** rewrote the AI guidance to *prefer* them:
- `trilogy/ai/constants.py`: new table rows "Rows in A but never in B (set difference) | `except(...)`",
  and "**Prefer except over multi-column `not in`**".
- `trilogy/ai/syntax_examples.py`: new `except-intersect-setops` example whose NOTES literally
  say "customers who bought in store but never online → `from except(...) select count(x)`" — i.e.
  exactly q87, with a single-column count consumer.

The two composite-membership bugs from the prior report (`bug_q87_composite_membership.md`)
**are still fixed** — they did not regress. The jump is a *different, new* bug: the agent
followed the new guidance, wrote the natural `except((...store combos...), (...catalog...),
(...web...)) -> (last_name, first_name, sale_date)` then `count(...)`, and got a silently wrong
answer (337 in the run). It burned ~918k tokens noticing "the except only compares last_name",
retrying, re-reading the example, and finally retreating to the same concat `not in` form as
before (`workspace/query87.preql`, → 45,689, still FAIL vs ref 47,298).

## The framework bug (reproduced + minimized on current engine)

A multi-output `except(...)`/`intersect(...)` whose downstream consumer references only ONE of the
declared output columns has its **set-op arms pruned to just that one column**, so the EXCEPT/INTERSECT
compares on a single column instead of the full declared tuple — silently changing row identity.

Minimal repro (workspace `raw` model, store-except-catalog on the (last, first, date) key):
```
with store_only as except(
  (where ss.date.year=2000 select ss.customer.last_name as last_name,
        ss.customer.first_name as first_name, ss.date.date as sale_date),
  (where cs.sold_date.year=2000 select cs.billing_customer.last_name as last_name,
        cs.billing_customer.first_name as first_name, cs.sold_date.date as sale_date)
) -> (last_name, first_name, sale_date);
select count(store_only.sale_date) as cnt;     -- RESULT: 0   (WRONG)
```
Generated SQL: both arms `GROUP BY` all three columns (grain intact) but `SELECT` only
`___tvf_arm_N_sale_date`; the EXCEPT CTE reads one column → `store.sale_date EXCEPT catalog.sale_date`
→ every store date also occurs in catalog → 0.

### Trigger matrix (all on current engine, `4e69c5547`+working tree)
| case | consumer references | arm cols projected | result | correct? |
|------|--------------------|--------------------|--------|----------|
| `except` 3-col, `count(sale_date)` | 1 of 3 | only `sale_date` | **0** | NO |
| `except` 3-col, `select last,first,date` (all 3) | 3 of 3 | all 3 | 47,318 rows, incl. `('Monahan', NULL, …)` | YES (NULL-safe) |
| `except` 3-col, `count_distinct(concat(all 3))` | all 3 | all 3 | 47,270 | YES* |
| `except` 1-col | 1 of 1 | 1 | fine | YES |
| **`intersect` 3-col, `count(sale_date)`** | 1 of 3 | only `sale_date` | pruned | NO (same bug) |
| `union` 3-col, `count(sale_date)` | 1 of 3 | only `sale_date` | pruned but **harmless** (bag, unused col can't change a `sale_date` count) | n/a |

\* full 3-arm store-except-catalog-except-web via all-columns → **47,270**; reference (PRAGMA
`tpcds(87)` EXCEPT-chain) = **47,298**. The 28-row residual is the concat NULL/empty-name
conflation noted previously (authored-spelling artifact), NOT this bug. With true whole-tuple
projection the EXCEPT is NULL-safe and lands on the reference family — so `except(...)` is the
*correct* construct for q87, if only the arms weren't pruned.

## Root cause — file:line

`trilogy/core/optimizations/hide_unused_concept.py` — `HideUnusedConcepts.optimize` (lines 52-132):
- lines 58-75 compute `used` = the concepts a downstream child actually renders from this CTE
  (for `count(sale_date)`, `used = {sale_date}`);
- lines 87-110 set `cte.hidden_concepts = {last_name, first_name}` on the `UnionCTE`;
- **lines 118-128 propagate that hide INTO each `branch.hidden_concepts`**, so every EXCEPT/INTERSECT
  arm SELECT drops the "unused" columns.

The pass is written for the original `union(...)` (UNION ALL), where pruning a column no consumer
reads is a valid projection narrowing. It has **no awareness of `cte.set_operator`**: for
`SetOperator.EXCEPT` / `INTERSECT`, the arms' DISTINCT + set-difference/intersection use the entire
projected row as the identity key, so *every declared output column is load-bearing* and must not be
hidden. Commit `4e69c5547` added the set operators and the guidance that funnels agents to them, but
did not teach this pruning pass (or `_hide_branch_only_outputs`, same file, lines 13-50) to leave
non-UNION_ALL set-op columns alone.

Contributing: `gen_union_select_node` (`trilogy/core/processing/node_generators/union_select_node.py`)
correctly emits all `ordered_outputs` on the UnionNode/MergeNode — the loss happens *only* in the
optimizer pass above, which is why it is demand-driven (prune vs keep flips with what the final
SELECT references).

## Classification
**Real framework bug** (silent wrong result), aggravated into a 918k sink by a **guidance change that
actively steers agents into the broken construct** (prefer `except` over `not in`, with a q87-shaped
example). Not agent error: the agent's spelling is the one the docs prescribe, and it correctly
diagnosed "the except only compares one column" but had no in-language way to force full-tuple
projection through an aggregate consumer. The prior composite-membership fixes did NOT regress.

## Fix direction (DO NOT FIX — noted only)
In `HideUnusedConcepts`, skip hiding/propagating output columns for any `UnionCTE` whose
`set_operator is not SetOperator.UNION_ALL` (every declared output is part of set-op row identity).
Same guard for `_hide_branch_only_outputs`. Add a guard test: multi-column `except`/`intersect` whose
consumer references a strict subset of outputs must still project ALL declared columns into the arms.

Verified on a scratch copy of the workspace DB (`.cache/tpcds_sf1.duckdb`); read-only, no run-dir DB opened.
