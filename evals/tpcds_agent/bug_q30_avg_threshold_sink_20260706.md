# q30 token sink (203k → 790k, +587k) — SILENT wrong-result driven by a misleading undefined-concept suggestion

**Verdict:** FRAMEWORK BUG (diagnostic/guidance defect that causes a silent wrong result).
Not agent error, not deepseek variance, not the avg-threshold gate. The correlated per-state
average / `>1.2×avg` gate is a red herring — it compiles and runs fine. The real bug is the
**suggestion ranking in `_find_similar_concepts`**, which steered the agent off the correct
returning-customer path onto the *original web-sale's* billing customer.

## Symptom
- Business q30: returning customers (WR_RETURNING_CUSTOMER_SK) whose 2002 web-return total for a
  return-origin state exceeds 1.2× that state's average, home state = GA.
- Agent naturally wrote the grouping key as the bare entity `by web_returns.billing_customer`
  (the import alias of `customer` = the returning customer, declared + bound in the ingest model
  as `WR_RETURNING_CUSTOMER_SK: ?billing_customer.id`).
- Bare `web_returns.billing_customer` is a namespace, not a concept, so it is (correctly)
  undefined — BUT the error's `Suggestions:` list contained only
  `web_returns.web_sales.billing_customer.demographics.{id,gender,...}` and **never the obvious
  `web_returns.billing_customer.id`**. The agent (log msg 16: *"So the billing_customer is under
  web_sales in the web_returns model"*) trusted the suggestion and rewrote everything through
  `web_returns.web_sales.billing_customer` — WS_BILL_CUSTOMER_SK, a DIFFERENT customer (the
  bill-to of the original order, not the person who made the return). Then it thrashed ~15 more
  turns over the same wrong path before submitting a clean-running but wrong query.

## Silent wrong-result (scored, not theory)
Engine harness over the run's workspace (`raw.web_returns` ingest model), scale factor 1,
reference `tests/modeling/tpc_ds_duckdb/query30.sql` (builds/executes fine, 100 rows):

| form | grouping/select customer | rows | distinct ids | overlap w/ ref (99 ids) |
|---|---|---|---|---|
| reference `query30.sql` | wr_returning_customer_sk | 100 | 99 | — |
| **submitted candidate** | `web_returns.web_sales.billing_customer` (WS_BILL) | 100 | 93 | **2** |
| corrected (`web_returns.billing_customer.id` direct) | wr_returning (WR) | 100 | 99 | **94** |

Swapping the buried-but-correct path back in flips overlap from 2/93 → 94/99. Confirms the path
swap — caused by the suggestion — is the wrong-result driver.

## Minimal repro (trigger matrix)
`import raw.web_returns as web_returns;` then:
- `select web_returns.billing_customer limit 3;` → **UndefinedConcept**, suggestions = only
  `web_sales.billing_customer.demographics.*` (BUG: no `web_returns.billing_customer.id`)
- `... by web_returns.billing_customer, ...` (agent's actual construct) → same
- `... by web_returns.billing_customer.id, ...` → **OK** (correct path, but never suggested)
- `select web_returns.billing_customer.id limit 3;` → OK (resolves to `customer.C_CUSTOMER_SK`)

Toggle isolation: the avg-gate, the state join, and the year filter all compile independently;
removing them does not change the suggestion behavior. The single failing ingredient is the bare
`web_returns.billing_customer` reference + its misranked suggestions.

## Root cause (file:line)
`trilogy/core/models/environment.py:390` `EnvironmentConceptDict._find_similar_concepts`.
For query `web_returns.billing_customer` (2 segments), `path_matches` (lines 416–425) collects
every key of which the query is an ordered subsequence. Two families qualify:
- `web_returns.billing_customer.id` (extra depth 1 — the intended answer), and
- `web_returns.web_sales.billing_customer.demographics.id` (extra depth 3 — unrelated).

`path_matches` is built by iterating `self.keys()` in **dict-insertion order with no relevance
sort**, then the merged list is de-duped and hard-capped at 6 (lines 459–463). `web_sales` is
imported before the direct `customer as billing_customer` alias, so the deep
`web_sales.billing_customer.demographics.*` keys sit at dict index ~107 while the shallow direct
children start at ~index 554 — the 6-cap is fully consumed by the deep, wrong-namespace matches
and the correct shallow child is dropped. Verified live:
`web_returns.billing_customer.id` IS present (index 554) yet absent from the returned 6.

**Fix locus (do NOT apply here):** rank `path_matches` by closeness before capping — prefer the
fewest extra segments beyond the query (`len(key_segs) - len(q_segs)`) and/or the query as a
contiguous prefix, so `...billing_customer.id` (extra 1) outranks
`...web_sales.billing_customer.demographics.id` (extra 3). Optionally also emit an
alias-is-a-namespace hint ("did you mean its key `web_returns.billing_customer.id`?") when a bare
import-alias entity is referenced.

## Classification
FRAMEWORK bug — suggestion/diagnostic ranking. Silent class (query runs clean, wrong rows);
the >500k token bar was the only detector. Agent behaved reasonably (followed the framework's own
suggestion). Secondary contributor: the ingest model exposes two `billing_customer` aliases
(direct + via `web_sales`), which the ranking bug then resolves in favor of the wrong one.
