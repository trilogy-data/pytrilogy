# q29 churn (run 20260629-030015_enriched) — ~636k tokens, SILENT failure

## Outcome
- Scoring: `id 29, status fail, ref_rows 1, cand_rows 100, detail "result set differs from reference"`.
- 6 `trilogy run` attempts, all `exit_code: 0`, all returning 100 rows. No errors, no exhaustion — pure silent wrong-result thrash. Final submitted query returns 100 rows; correct answer is **1 row**.

## What q29 actually is
Store sales in Sep 1999, correlated **at the line/ticket grain** to a store return of the
same (customer, item, ticket) returned Sep–Dec 1999, AND to catalog sales of the same
(billed customer, item) in years 1999–2001; sum three quantities per
(item_code, item_desc, store_code, store_name). The three-way correlation is so selective
that exactly **one** (item, store) survives.

Reference row: `('AAAAAAAAOBCDAAAA', 'Very public months…', 'AAAAAAAAEAAAAAAA', 'ese', 71, 41, 23)`.

## The framework is NOT the obstacle — q29 is fully expressible and correct
Verified against the run's own `workspace/tpcds.duckdb` via the scoring engine
(`generate_sql` + `execute_raw_sql`):

1. **Reference SQL** → 1 row `(…,71,41,23)`.
2. **Canonical `tests/modeling/tpc_ds_duckdb/query29.preql`** (only `import` paths
   retargeted to `raw.*`) → **1 row, exact match**. It uses a `rowset correlated`
   doing the fine-grain join, then a *separate* select that re-aggregates
   `sum(correlated.store_sales.quantity)` etc. grouped by the 4 output columns.
3. **Natural single-query formulation** I wrote — three `with` stages (ss/sr/cs base,
   keys including cust_id/item_id/ticket but at fine grain), then ONE final select that
   `inner join`s on (ticket,item,cust) and (cust,item), does **not** select the join keys,
   and wraps each measure in `sum(...)` → **1 row, exact match `(…,71,41,23)`**.

Repro scripts: `/tmp/repro29c.py` (ref vs canonical vs agent), `/tmp/repro29e.py`
(natural single-query, returns the correct 1 row).

## Why the agent burned 636k tokens (root cause = agent modeling, not a code bug)
Every one of the agent's 6 versions made the same structural error: it **pre-aggregated
each fact independently to the coarse (item_code, …) grain first, then joined the
aggregates on business keys** (item_code / store_code). That throws away the row-level
correlation the question requires:
- store returns get summed to (item,store) before being tied to the *specific* Sep-99 sale;
- catalog qty becomes an item-level total over *all* matching customers (the agent's
  `2531` / `3491` values), spread across every store, instead of the per-correlated
  subset (`23`).
- It used `left join` between the coarse aggregates, so every Sep-99 store sale survives →
  100 rows. Swapping `left`→`inner` still gives **100 wrong rows** (`/tmp/repro29f.py`):
  the structure is wrong, not just the join type.

The agent's recurring on-screen complaint (log #31/#37): *"the join key cust_id and item_id
become part of the output grain automatically, even though they're not explicitly
selected."* This is a **misdiagnosis** — in its WRITE#1/#2 it *did* explicitly select
`cust_id`/`item_id` into the intermediate rowset and then did **not** re-`sum` in the
outer select, so the finer grain (and its duplicate item/store rows: 98/58/24) is exactly
what it asked for. When the join keys are simply omitted from the SELECT and measures are
summed at the top, Trilogy aggregates to the coarse grain correctly — proven minimally in
`/tmp/repro29d.py` (2-fact inner join, keys not selected → 20 distinct (item,store) rows,
no fan-out). The agent never tried that one move (omit keys + sum at the end) on a
fine-grain correlated join; it instead retreated to the broken pre-aggregate-then-join
shape.

Secondary discoverability factor: the agent never noticed that `raw/store_sales.preql`
already carries inline return concepts (`is_returned`, `return_quantity`, `return_date`,
`return_customer` — identical to the canonical model). The canonical solves q29 almost
trivially with those + a `correlated` rowset; the agent hand-rolled a separate
`store_returns` import and a multi-stage join instead.

## Classification
**Agent issue (not a framework bug).** Concrete proof: the canonical preql and an
independent natural single-query formulation both `generate_sql` cleanly and execute to the
exact reference row against the run's own database. No silent miscompilation, no fan-out in
the framework, no error swallowed. The token burn comes from the agent (a) modeling the
correlation as coarse-grain pre-aggregation + business-key joins, (b) misreading its own
key-selection as an automatic grain leak, and (c) not discovering the inline return
concepts / `rowset` re-aggregation idiom.

### Soft follow-up (optional, not a bug)
The one genuinely subtle bit is the scoped-join grain rule: *selecting* a join key pins it
into the output grain, and re-aggregating to a coarser grain requires omitting the keys and
summing at the final select. This is correct behavior but cost the agent most of its
iterations. A targeted `agent-info` example ("correlate facts at fine grain, then
re-aggregate — do NOT select the correlation keys in the final projection") would likely
have prevented the churn. No code change indicated.
