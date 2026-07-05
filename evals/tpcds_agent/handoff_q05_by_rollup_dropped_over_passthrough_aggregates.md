# HANDOFF — q05: `by rollup (…)` silently discarded when the SELECT has no fresh aggregate (passthrough of pre-aggregated columns)

**Status:** ✅ FIXED 2026-07-05 (option #1 + #2 floor). `_propagate_select_grouping` now calls
`_apply_grouping_to_passthroughs` (`trilogy/parsing/v2/select_finalize.py`): a SUM/COUNT-derived
rowset passthrough measure is re-aggregated with an implicit `sum(...)` carrying the spec —
passthrough and fresh-aggregate forms produce identical rows (parity asserted) — and this also
fixes the mixed shape (passthrough next to a fresh aggregate emitted a bare ungrouped column).
Exempt: grouping keys, key-derived dims (q80 fold family), single-row scalars (broadcast),
plain other-grain dims (join-back). Never silent: a non-additive (avg/…) passthrough or a
keys-only select with no carrier raises `InvalidSyntaxException` with the explicit-form fix.
Guards: `tests/engine/test_duckdb_rollup_passthrough.py`.

Original report below for context.
**Full diagnosis:** `evals/tpcds_agent/bug_q05_by_rollup_silently_dropped_over_passthrough_aggregates.md`
**Classification:** REAL framework bug, **SILENT** (wrong rows — subtotal/grand-total rows missing,
no error). Not the same as the (now-fixed) q05 float32 issue; this is the still-latent codegen hole
the agent side-steps by using fresh `sum(...)`.

## Symptom
q05 = channel roll-up (store/catalog/web) with `rollup(channel, entity_id)` → per-entity leaves +
per-channel subtotals + grand total. A natural Trilogy shape pre-aggregates in rowsets (`sum(…)`),
then the final SELECT **passes through** those pre-aggregated columns (e.g.
`coalesce(sale_agg.ext_sales, 0) as total`) with `by rollup (sale_agg.channel, sale_agg.entity_id)`.
The query runs clean and returns only leaf rows — **zero subtotal/grand-total rows**; the `by
rollup` clause was silently discarded (no ROLLUP in the generated SQL, no diagnostic).

## Minimal repro (reconfirmed)
```python
import sys; sys.path.insert(0,'evals'); from common import scoring; from pathlib import Path
ws=Path('evals/tpcds_agent/results/20260705-142435/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
gen=lambda b: eng.generate_sql(b)[-1]
```
```
import raw.all_sales as sales;
rowset agg <- where sales.date.year = 2001
  select sales.channel as channel, sales.item.id as eid, sum(sales.ext_list_price) as ext;
select agg.channel as channel, agg.eid as eid, agg.ext as total
by rollup (agg.channel, agg.eid)
limit 50;
```
→ `'ROLLUP' in gen(body).upper()` is **False** (bug). Contrast: a select whose projections are
fresh `sum(...)` aggregates emits `GROUP BY ROLLUP(...)` correctly.

## Root cause (file:line)
`trilogy/parsing/v2/select_finalize.py`:
- `_propagate_select_grouping` (L995) — for a non-None grouping `spec`, loops the select items and
  calls `_collect_ungrouped_aggregates_deep`, stamping `wrapper.grouping = spec.mode` /
  `.grouping_sets` onto each hit (L1042-1045). **If the loop finds zero un-grouped aggregates it
  stamps nothing and returns — the rollup spec is silently discarded.**
- `_is_ungrouped_aggregate` (L929) — requires a bare `AggregateWrapper` with `not node.by`. A
  passthrough of a rowset/`auto` measure (already summed at its own grain) is not one, so it is
  never collected.

Downstream, `query_processor.py` builds a plain GROUP BY (no `rollup_concepts`), so DuckDB gets
`GROUP BY 1,2,…` with no subtotal rows.

## Fix direction
`by rollup`/`by cube`/`by grouping sets` describe the GROUPING of the SELECT's output grain — they
must apply whenever the select groups, regardless of whether the aggregation is a fresh function or
a passed-through pre-aggregate. Two options:
1. **Apply the grouping spec at the query/grain level, not by stamping individual aggregate
   wrappers.** The rollup grouping-set should be attached to the SELECT's group node (the thing that
   emits GROUP BY) so it holds even when every measure is a passthrough. This is the robust fix —
   the spec should never depend on finding a bare `AggregateWrapper`.
2. **At minimum, never silently discard.** If `_propagate_select_grouping` finds no un-grouped
   aggregate to stamp, either apply the grouping at the group node anyway (option 1) or raise a
   clear error ("`by rollup` requires the select to group; found only passthrough projections") —
   never drop it with no diagnostic.

Prefer #1 (make passthrough rollups work — it's a valid, idiomatic shape). #2 is the floor.

## Test to add
DuckDB codegen+execute test:
- The passthrough repro above must emit `GROUP BY ROLLUP(...)` and return subtotal + grand-total
  rows (null-key rows present).
- Keep the fresh-`sum()` rollup path green.
- Assert parity: the passthrough form and an equivalent fresh-aggregate form produce the same
  subtotal/grand-total row set.

## Acceptance criteria
- `by rollup` over passthrough/pre-aggregated projections emits ROLLUP and yields subtotal +
  grand-total rows.
- Fresh-aggregate rollup unchanged.
- `by cube` / `by grouping sets` over passthroughs behave consistently (same root path).
- No regression in the rollup / select-finalize suites.
- `ruff check . --fix && mypy trilogy && black .` clean.

## Do NOT
- Do NOT require a fresh aggregate for `by rollup` to work — the passthrough shape is legitimate.
- Related but SEPARATE: `handoff_q05_q80_rollup_label_via_join.md` covers the rollup *label*
  (rendering subtotal rows with a channel label) idiom/model gap — not this codegen drop.
