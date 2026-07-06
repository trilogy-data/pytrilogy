# q14 still failing — run 20260706-222300 (631,289 tok, FAIL, silent wrong rows)

Re-verified on today's engine against the failing run's own workspace DB
(`results/20260706-222300/workspace`, opened read-only after killing a stale
python process that held the file lock). q14 exit 0, 100 rows,
`"result set differs from reference"` — **silent wrong rows**.

## Headline: NO live framework wrong-row bug for q14. Failure is agent modeling.

The submitted `workspace/query14.preql` uses the modern idiom — an `intersect(...)`
rowset for the 3-channel combo set + **composite row membership**
`(brand_id, class_id, category_id) in (qc.brand_id, qc.class_id, qc.category_id)`
+ `by rollup` + `having sum > rowset_scalar`. **All of these constructs work
correctly on the current engine.** A variant that keeps the agent's exact
`intersect` + composite-membership shape and fixes only the agent's four modeling
mistakes **passes 100/100** (proof below). The canonical
`tests/modeling/tpc_ds_duckdb/query14.preql` also **matches reference 100/100**.

The prior q14 framework bugs are all resolved/avoided on this run:
- multi-key **subset-join-onto-rowset** BinderException (recheck doc): **FIXED**;
  the agent never used `subset join` this run.
- **union-reproject composite subset join** "could not resolve union/multiselect
  output" (`bug_q14_union_multiselect_output_resolution.md`, the 2.58M enriched
  sink): **AVOIDED** — the agent expressed the intersection as
  `intersect(...)` + composite `(a,b,c) in (...)`, which now lowers correctly
  (row-tuple `IN (SELECT ...)`), i.e. the q87 composite-membership fix covers it.
- grainless BASIC-scalar HAVING misroute: **not triggered** — the agent used a
  `rowset avg` scalar (broadcasts via `1=1`, correct).
- scoped `inner join` → FULL: **moot** (syntax removed).

## Why the submitted file returns wrong rows — 4 independent agent-side causes

Diffing candidate vs reference (`query14.sql`): leaf **counts** match but leaf
**sums** are wrong, and the grand-total/channel-subtotal rows are missing.

1. **PRIMARY — wrong measure.** Candidate sums `sales.ext_list_price`; reference
   (and canonical) sum `quantity * list_price`. In this TPC-DS data these are
   **not equal**: for catalog leaf (1002001,4,3) Nov-2001,
   `sum(cs_ext_list_price)=16636.16` but `sum(cs_quantity*cs_list_price)=23863.10`
   (both count 7). The generated SQL is correct — it faithfully sums
   `CS_EXT_LIST_PRICE`. The agent's own farewell states the false belief
   *"overall average `ext_list_price` (qty × list_price per line)"*. This alone
   makes every leaf/subtotal wrong. **Agent modeling** (guidance contributor).
2. **UPPERCASE channel.** Projects raw `sales.channel` (`STORE/CATALOG/WEB`);
   reference emits lowercase (`store/catalog/web`). Every row's label mismatches.
   **Agent/guidance.**
3. **HAVING at rollup grain, not leaf grain.** Reference applies
   `having sum(...) > avg` **per (channel,brand,class,category) leaf before the
   ROLLUP**; the candidate applies it after the rollup (`WHERE scrawny.total_sales
   > avg` on the rolled-up output). Sub-threshold leaves are dropped from output
   but still contribute to the subtotals → **inflated subtotals/grand total**.
   **Agent modeling.**
4. **`order by _level` + `limit 100` drops the rollup subtotal rows.** Line 31 is
   `--grouping(sales.channel)+…+… as _level` and `order by _level asc, …` on
   line 35. The agent used `--` believing it a SQL comment; **Trilogy `--` is
   double unary negation, not a comment** (the harness even states "`--` is NOT a
   comment"), so `_level` is a **live** concept equal to the positive
   grouping-level sum, and the generated SQL orders by it: leaf rows (level 0)
   first, subtotals/grand-total (level>0) last → `LIMIT 100` truncates every
   subtotal. Removing that line yields `UndefinedConceptException: _level`,
   confirming the line defines the concept. (This is the same phenomenon prior
   docs labeled a "commented-alias masking bug"; it is actually `--`
   double-negation — the framework behaves as designed, the agent's SQL habit
   misfires.) **Agent** (ordering choice + `--` misuse).

## Trigger matrix (current engine, workspace DB, ref = `query14.sql`)

| variant | result |
|---|---|
| submitted `query14.preql` | 100 rows, **FAIL** (all 4 causes) |
| canonical `tests/.../query14.preql` (import remapped) | **PASS** 100/100 |
| M0: catalog Nov-2001 leaf, `sum(ext_list_price)`, **no membership** | 16636.16/7 (wrong measure alone) |
| M1: same + `intersect` composite membership | 16636.16/7 (membership adds nothing wrong) |
| raw SQL `sum(cs_quantity*cs_list_price)` same leaf | 23863.10/7 (correct) |
| PROOF: agent's `intersect`+composite membership, but `qty*list_price` + lowercase + leaf-grain HAVING + order-by-channel | **PASS** 100/100 |

Isolated conclusion: `intersect(...)`, composite tuple membership, `by rollup`,
and `having vs rowset-scalar` are all sound. Toggling in the correct measure +
lowercase + leaf-grain HAVING + order-by-key is exactly what turns the agent's
own structure from FAIL to PASS.

## What drove the 631k churn (down from 2.58M)

Moderate: 22 iterations, 4 tool errors. Prompt tokens dominated by **6 large
`agent-info` model dumps re-sent across growing context** (the `all_sales`/`item`
dumps are tens of KB). Two minor parse-error frictions, each a clear actionable
error costing ~1 iteration:
- `rowset qc as intersect(...)` → parse error "expected EOI/block". The set-op
  rowset binder is `rowset qc <- …` (or `with qc as intersect(...)`); the agent
  used `rowset … as …` (wrong keyword). `rowset X <- intersect(...)` and
  `with X as intersect(...)` both build fine.
- `by rollup (...)` placed after `having` → parse error (the clause moved before
  `having`); the error hint is correct.

Neither is a framework wall. The file validated and was submitted; the wrong
answer is invisible because scoring is offline.

## Classification

- **Framework bug: NONE live for q14.** All constructs the agent used compile and
  produce correct SQL; canonical + a fixed-modeling variant of the agent's own
  shape both match reference.
- **Agent modeling (primary driver):** (1) `ext_list_price` instead of
  `quantity*list_price` — dominant; (2) uppercase channel; (3) HAVING at rollup
  grain not leaf grain; (4) `order by --…as _level` leaf-first + limit drops
  subtotals (`--` is double-negation, not a comment).
- **Guidance contributors:** the model comment for `ext_list_price`
  ("Line-extended list price") does **not** warn that it ≠ `quantity*list_price`,
  and nothing steers the agent to `quantity*list_price` for "sales value" (the
  agent explicitly conflated them — highest-leverage guidance fix); no steer to
  lowercase the uppercase `channel` enum to the reference's labels; `rowset X as
  intersect` / `by rollup` clause-order frictions.

## Root cause (file:line)

No `trilogy/` defect. The wrong values originate in the candidate `.preql`
(`results/20260706-222300/workspace/query14.preql`): measure on line 29
(`sum(sales.ext_list_price)`), raw channel on line 25, rollup-grain HAVING on
line 33, and the `--…as _level` double-negation define + `order by _level` on
lines 31/35. Correct engine codegen verified via `generate_sql` (leaf SQL sums
`CS_EXT_LIST_PRICE` faithfully). Guidance loci for a fix: the `ext_list_price`
property comment in the ingest model `raw/all_sales.preql` (line 44) and the
`channel` enum handling.
