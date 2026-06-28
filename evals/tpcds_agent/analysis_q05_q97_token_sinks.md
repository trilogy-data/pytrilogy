# Token-sink analysis: q05 (~3.2M tok, FAILED) & q97 (~2.9M tok, PASSED)

Run: evals/tpcds_agent/results/20260628-042638_enriched (model: deepseek-chat, max_iter 75).
Neither query crashed the framework — both burned tokens via long iteration loops
(history replay × turns), not exceptions. q05: 114 msgs / 59 trilogy calls / 6 trivial
syntax errors. q97: 161 msgs / 4 errors. Cost is iteration count, not tool failures.

## q05 — channel/entity rollup (sales + returns) — FAILED
Agent built a 4-stage pipeline: two date-filtered `union` arms (sales/returns) →
`combined` union rowset → `formatted` relabel rowset (CASE channel labels + concat ids) →
final `by rollup`. Canonical does ONE pass over `all_sales` with a `windowed(...)` macro
and `coalesce(channel_dim_text_id, return_channel_dim_text_id)` — no rowsets.

Two independent failure causes (reproduced against the DB):

### Cause 1 (dominant): `order by _level asc` evicted the rollup rows past LIMIT 100
Task: "sort by channel and entity, nulls first, first 100 rows." Agent prepended
`order by _level asc` (level = grouping()+grouping()), so leaf rows (level 0) fill the
top-100 and the grand-total + 3 channel subtotals are cut off. Agent's top-100 has 0
subtotal/total rows; reference (nulls-first) has all 4. At msg 72–74 the agent diagnosed
this exactly but mis-fixed it (`_level asc` puts totals LAST). Likely induced by the
agent-info rollup example's hidden-`grouping()` level-sort.
Classification: (d) agent difficulty + (b) guidance.

### Cause 2 (also fatal — framework bug): nested-rowset aggregate fan-out
With the sort fixed, grand total = 119,387,633 vs reference 112,458,734, localized
entirely to WEB (agent 26.57M vs correct 19.64M; store/catalog match). web_site is the
only SCD dim (up to 3 `channel_dim_id` per `channel_dim_text_id`).
- `select sum(combined.gross_sales)` over the single union rowset → 1 row, 112.5M (correct).
- `select sum(formatted.gross_sales), count(...)` over the relabel rowset reading from
  `combined` → returns 861 rows instead of 1; a global aggregate fails to collapse to grain.
- Repro shape: union(...) rowset → passthrough rowset selecting its measure → bare
  sum(measure) does not group to one row → fans out on SCD keys.
Classification: (a) framework correctness bug. Avoidable (canonical uses no rowsets).
See bug_q05_nested_rowset_aggregate_fanout.md.

Recommended (q05): file the nested-rowset aggregate-grain bug; add a rollup-example caution
about level-sort vs explicit nulls-first + LIMIT. No model data change.

## q97 — count (customer,item) combos store-only / catalog-only / both — PASSED
Agent ended on the correct idiom (one `union` of both channels' combos + per-combo presence
flags via sum(case when src=…), count by flag) ≈ canonical `pair_presence`. It passed. Thrash
came from getting there:
- First tried SQL FULL OUTER JOIN of two distinct sets and `(a,b) not in (select …)` —
  Trilogy rejects subqueries (clear hint); FULL JOIN across two independent fact models hits
  "disconnected subgraphs."
- Discovered by trial that `with x as where… select a,b` does NOT dedup (returned 547,758
  rows), derailing its full-join row-count reasoning for many turns.
- Imported store_sales + catalog_sales separately, never used `all_sales`, despite agent-info
  saying "prefer all_sales for cross-channel" and listing a "presence-flags / multi-column
  INTERSECT/EXCEPT" example (terse list entries it didn't fetch/apply early).
No framework bug; errors were clear.
Classification: (b) guidance gap + (d) difficulty (mild (a): full-outer-join-two-sets isn't
expressible across two models, by design).

Recommended (q97): promote the presence-flags / only-in-A / only-in-B / both pattern to a
worked, discoverable agent-info example; cross-link from the FULL-OUTER-JOIN docs; optionally
have the disconnected-subgraphs error on a set-comparison count suggest the union+presence
idiom. No correctness work warranted.
