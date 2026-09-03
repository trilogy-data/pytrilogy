# Handoff: summary-table selection - size-aware tie-break, sum linearity, aggregates inside expressions

**Status: OPEN. Re-filed 2026-08-20 from the tail of the deleted
`handoff_aggregate_selection_gap.md`,** whose items 1-3 landed on 2026-08-12 (the
file was deleted in `facdd161c` on 2026-08-19 while these three items were still
open). The landed half is covered by `tests/discovery/test_aggregate_rollup_matching.py`;
every cell there asserts both "the summary IS used" and "the answer still matches the
raw fact", because this is an optimization that otherwise degrades silently. Keep that
property for anything added here.

What already works: an agent-authored inline aggregate that is lineage-equivalent to a
hidden `fact_agg_*` binding compiles to the summary table, in any namespace, at exact /
coarser / grand-total grain, with the binding never exposed to the agent.

## 1. Size-aware tie-break for key-existence subqueries

For a q10-style `customer.sk in (fact filtered by date)`, both the raw fact and the
summary bind the needed keys by address, so the summary IS enumerated as a candidate -
it just loses the ranking. `score_datasource_node`
(`trilogy/core/processing/v4_helper/source_scoring.py`) ranks by
`(mat_score, grain_score, ...)` where `grain_score` counts grain components *not*
requested, so a raw fact's 2-key grain beats a summary's 8-key grain.
`get_materialization_score` knows address *type*, not table size. **No notion of row
count or cost exists anywhere in datasource selection.**

Fix shape: a materialization preference or a declared row-count hint, so an 8-key
summary can beat a 2-key raw fact when both merely supply keys.

Gate carefully - this changes plans for existing models, so it needs a corpus A/B with
the byte-diff harness, not just a green suite.

## 2. Aggregates inside an enclosing expression

Only a *bare* aggregate is matched today. `sum(x) + 0` demands the enclosing BASIC and
the summary search never looks inside its lineage, so it falls back to the raw fact
(right answer, wrong scan). Pinned by
`test_aggregate_inside_an_expression_is_a_known_gap` in the file above - the day that
assertion flips should be a deliberate one.

This is the same lineage-descent the linearity rewrite below needs, so the two are
worth doing together.

## 3. Sum linearity and per-measure non-null counts

- **Linearity rewrite** - `sum(a - b + c)` -> `sum(a) - sum(b) + sum(c)` when every
  column is bound by the summary. Worth +2 of the saved TPC-DS candidates (q04, q11).
- **`avg` unlock** - summaries carry no per-measure non-null counts, and
  `avg = sum / count(non-null)`, so every `avg` is unservable. Adding those counts to
  the aggregate DDL is worth roughly +1 (q07).

## Ceiling this is chasing

Of 19 saved candidates: 9 strictly servable as-is, +2 with linearity, ~12-13 with the
`avg` unlock, and 6 principled non-matches (row-level measure-value band filters,
measure products, count-distinct grains). So roughly half to two-thirds of real
candidates are summary-servable. The eval-side acceptance metric already exists:
`used_aggregate` in `report.json` / `agg used` in the funnel, which reads compiled
datasource selection without exposing anything to the agent.

## Guardrails

`tests/join_matrix` gets cells first; corpus A/B byte-diff to prove zero plan changes
outside summary-bound models; and count rule firings - a green suite says nothing about
a matcher that never fires.
