# Silent token sinks, ingest leg: q72 (FAIL) + q59/q77 presence-probe no-ops + q49 explore volume

Found in run `20260820-031800_ingest_deepseek_deepseek-v4-flash` (ingest leg, auto-ingested
`root/*.preql`). Re-verified against the working tree on 2026-08-20: every repro below was
re-executed through `evals/common/scoring.make_scoring_engine` on a copy of the run
workspace and reproduces. Three of the four queries hide real framework bugs; all engine
findings are SILENT (exit 0, no warnings, wrong or phantom rows).

Ranking within this file: q72 Bug A (P1, graded FAIL, provably wrong rows, one-line-class
fix) > q72 Bug B (P1, internally inconsistent aggregates) > q59/q77 presence-probe no-op
(P2, silently vacuous filter, and inconsistent between plans of the same clause) > q49
(no engine bug, tooling cost with receipts).

---

## q72 (1.0M tokens, FAIL "result set differs from reference") - TWO engine bugs

### STATUS 2026-08-20: Bug A FIXED. Bug B fixed except one residual (details at the
### end of the q72 section).

### Bug A (FIXED): NULL-valued group rows silently dropped when split aggregate branches rejoin

**This is the entire scoring failure.** The agent's final answer
(`results/.../workspace/query72.preql`, staged rowsets `sales`/`inv_rows`/`matched`, then
one select with `count(grain(...))` plus two filtered counts) is semantically correct.
The reference has 2008 groups; 9 have NULL `item_desc` and 397 have NULL
`warehouse_name` (TPC-DS seeds NULLs into dimension attributes). The generated SQL
computes total_cnt and the filtered counts in two branch CTEs (`busy`, `divergent`),
each of which contains all 2008 groups including the NULL ones, then recombines:

```sql
INNER JOIN "divergent" on "busy"."matched_item_desc" = "divergent"."matched_item_desc"
  AND "busy"."matched_warehouse_name" = "divergent"."matched_warehouse_name"
  AND "busy"."matched_week_seq" is not distinct from "divergent"."matched_week_seq"
```

`week_seq` (a scoped-join key) got null-safe pairing; the two intrinsically nullable
dims (`string?` columns bound with `?` in `root/item.preql` / `root/warehouse.preql`)
got plain `=`. Every NULL-dim group fails the equality: 2008 -> 1604 groups, the 404
missing rows are exactly the reference's NULL-dim rows. Patching just those two
predicates to `is not distinct from` makes the run's answer **exactly equal** the
reference (`rows_equal_tolerant` = True, verified).

Root cause chain:

- `trilogy/core/processing/v4_node_generators/rowset.py:403-460`: the rowset boundary
  propagates `nullable_concepts` only for KEY-LIKE handles (boundary grain +
  scoped-join key groups). The comment says a nullable non-key property "stays
  unstamped" to avoid hash-join-defeating `is not distinct from` in the FINAL
  re-pairing join. That optimization is unsound the moment the property becomes a
  GROUP BY key of split aggregate branches.
- `trilogy/core/processing/join_resolution.py:611-628` (`get_modifiers`): null-safe
  equality requires `side_nullable` on BOTH sides; with the stamp stripped it returns
  no modifier.
- `trilogy/dialect/duckdb.py:38-46` (`null_wrapper`): no `Modifier.NULLABLE`, renders `=`.

Minimal repro (run workspace, no union join needed, 1 group lost):

```trilogy
import root.catalog_sales as cs;
rowset s <- select
    cs.order_number as o, cs.item.item_sk as sk,
    cs.item.item_desc as d, cs.quantity as q;
select
    s.d,
    count(grain(s.o, s.sk)) as total,
    count(grain(s.o, s.sk) ? s.q > 10) as hi;
-- 13442 groups; the NULL item_desc group is dropped by
--   INNER JOIN ... on "uneven"."s_d" = "cooperative"."s_d"
-- The identical select WITHOUT the rowset returns 13443 incl. the NULL group.
```

Fix direction: the recombining join of aggregate branches over the SAME parent joins on
group-by keys, whose NULL values are group labels by construction; that join must be
null-safe regardless of nullability stamps (or the rowset boundary must keep intrinsic
nullability on any handle that can become a downstream group key).

### Bug B (defect 1 FIXED, defect 2 PARTLY FIXED): aggregate directly over a
### query-scoped union join - flat WHERE skips the unfiltered count branch, and
### output never collapses to the authored grain

This is what burned the tokens (21 writes). The agent's probe6/probe11 wrote the
NATURAL formulation - the same two rowsets, then one select with the three counts and
`union join sales.item_sk = inv_rows.item_sk and sales.week_seq = inv_rows.week_seq`
plus flat `where inv_rows.quantity_on_hand < sales.quantity`. Result: 29148 rows for a
question whose promised grain has 2008, including internally impossible rows like
`(NULL, NULL, NULL, no_promo=0, promo=0, total=7)`. The agent correctly called it
"impossible", spent probes 6 through 14 bisecting, and invented the staged-rowset
workaround (which then hit Bug A).

Two distinct defects visible in the generated SQL (reproduced from the log's probe11
body):

1. **The flat WHERE is dropped from the plain `count(grain(...))` branch.** The
   filtered counts aggregate `sweltering` (has `WHERE qoh < quantity`); total_cnt
   aggregates `kaput`/`divergent`, built straight off the FULL-join stream with no
   filter, and `kaput` does not even carry `quantity_on_hand`, so the branch node was
   constructed without the condition's inputs (demand-side omission, not a rendering
   miss). Hence promo + no_promo != total.
2. **Branches aggregate at authored-dims + scoped-join-keys grain and never collapse
   back.** `_final_merge_grain` and neighbors
   (`trilogy/core/processing/v4_helper/group_graph.py:1614-1657`) force the scoped-join
   relation addresses (`item_sk`, `week_seq`) into the FINAL merge grain, so every
   branch groups by e.g. `(warehouse_name, week_seq, item_desc, item_sk)`; the final
   SELECT projects the authored three columns but groups by all outputs, emitting one
   row per finer-grain combination (29148) instead of re-aggregating to the authored
   grain (2008).

Minimal repro (both defects, single non-key dim):

```trilogy
import root.catalog_sales as cs;
import root.inventory as inv;
rowset sales <- where cs.sold_date.year = 1999
  select cs.order_number as order_number, cs.item.item_sk as item_sk,
         cs.sold_date.week_seq as week_seq, cs.quantity as quantity,
         cs.promotion.promo_sk as promo_sk;
rowset inv_rows <- select inv.item.item_sk as item_sk, inv.date_dim.week_seq as week_seq,
         inv.date_dim.date_sk as date_sk, inv.warehouse.warehouse_sk as warehouse_sk,
         inv.quantity_on_hand as quantity_on_hand;
where inv_rows.quantity_on_hand < sales.quantity
select
    sales.week_seq,
    count(grain(sales.order_number, sales.item_sk, inv_rows.date_sk, inv_rows.warehouse_sk) ? sales.promo_sk is null) as no_promo_cnt,
    count(grain(sales.order_number, sales.item_sk, inv_rows.date_sk, inv_rows.warehouse_sk)) as total_cnt
union join sales.item_sk = inv_rows.item_sk and sales.week_seq = inv_rows.week_seq
limit 10;
-- Emits per-(week_seq, item_sk) rows (week_seq duplicated, totals 5-15) while ground
-- truth per week_seq is in the hundreds; total_cnt branch ("sweltering"/"late" CTEs)
-- carries no qoh filter.
```

**Verdict: silent framework bugs, both.** The final divergence is engine (Bug A), not
model or question. The 1.0M spend is engine (Bug B forcing discovery + workaround).

### What landed for q72, and what is left

Fixed (each gated by a row-asserting test; whole-corpus render is byte-identical
except q64, which gains four null-safe predicates from Bug A):

- `trilogy/core/processing/v4_node_generators/rowset.py` - the rowset boundary
  stamps nullability on EVERY handle, not just key-like ones. Bug A.
  Test: `tests/engine/test_duckdb_rowset_null_group_rejoin.py`.
- `condition_placement._uncovered_grouping_placements` - an UPSTREAM_MOST row
  atom is copied onto every select-phase aggregate the elected host does not
  feed. Bug B defect 1: `count(grain(a, b) ? p)` routes through the grain
  projection while the plain `count(grain(a, b))` wires the grain args
  directly, so a host elected on that projection left the plain count
  unfiltered.
- `condition_placement._group_in_active_relation` - an AGGREGATE whose relation
  mate is hosted only by its own lineage ancestors already contains the
  completion merge, so it keeps local hosting instead of deferring the atom to
  FINAL (where it filtered aggregated rows, not input rows).
- `merge_node._splits_aggregate_groups` - `_inject_scoped_join_key_exposure` no
  longer surfaces a relation member onto an aggregating parent when the member
  would become a new GROUP BY key.
- `strategy_builder._scoped_join_mates` / `_add_relation_axis_contributors` -
  contributor selection counts a scoped-join mate as covering the concept, and
  an axis provider already merged below a chosen contributor is skipped (it
  re-entered the FINAL merge re-admitting the rows the WHERE dropped).

Tests: `tests/engine/test_duckdb_union_join_aggregate_population.py` (four
cells: both counts / total only, filtered / unfiltered).

RESIDUAL, still open. `concept_graph._aggregate_axis_members` widens an
aggregate's grouping grain by every statement-scoped relation member its INPUT
GRAIN rides. When the counted row identity itself contains a relation member -
`count(grain(sales.order_number, sales.item_sk, ...))` under `union join
sales.item_sk = inv_rows.item_sk` - the branch groups by that member and the
outer select dedups rather than re-aggregating, so `week_seq` still repeats.
The minimal repro in this file (which counts a grain tuple holding no relation
member) is fixed; the q72 formulation above is not. Narrowing the candidate set
to the aggregate's DIRECT function arguments collapses the grain correctly but
breaks the q17 composite-union-join family (7 cells in
`tests/engine/test_duckdb_rowset.py`), which depends on the widening. Splitting
"member is the measure" from "member is part of the counted row identity" is
the piece of work that closes this.

---

## q59 (507k tokens, pass) - presence-probe rewrite lands post-merge and no-ops; first answer already passed

The FIRST full answer (write 2 of 9) scores PASS against `query59.sql` (verified by
rescoring the logged body). Everything after it, 6 more tool calls and most of the
tokens, was spent reconciling a real engine inconsistency the agent's verification
probe surfaced:

- write_02 (the answer): projects `this_year.store_name`/`store_code` plus ratios,
  `having this_year.store_key is not null and next_year.store_key is not null`
  after `union join this_year.store_key = next_year.store_key and this_year.week_seq
  = next_year.week_seq - 52`. Returns 312 rows (correct matched pairs).
- write_03 (verification probe): SAME rowsets, SAME union join, SAME having, fewer
  projected columns. Returns 318 rows: 6 phantom rows for 2002-week-5375 (next-side
  only, no 2001 counterpart), displayed with the coalesced read-through key.

Both reproduce today (312 vs 318). The language HAS machinery intended to make this
having work: `Factory._coalescing_presence_probe`
(`trilogy/core/models/build.py:3916`) rewrites `member is [not] null` on a coalescing
join-key member to a per-side probe, and its docstring cites this exact TPC-DS q59
subset-side no-op as the motivating case. In write_02's plan both probes materialize
pre-merge inside the side CTEs and work. In write_03's plan the next-side probe
materializes correctly but the this-side probe renders as
`coalesce("abhorrent"."this_year_store_key") is not null` where `abhorrent` is the
POST-merge CTE and `this_year_store_key` is already aliased to
`coalesce(next_year_store_key, this_year_store_key)`: the probe evaluates over the
fused column and self-defeats, exactly the failure the docstring warns about
("collapse the probe back onto the member").

**Verdict: silent framework bug** (plan-shape-dependent probe placement; the same
authored clause filters in one projection and no-ops in another). The token sink is
the inconsistency itself: with stable semantics either way, the agent would have
stopped at write_02.

---

## q77 (520k tokens, pass) - same presence-probe family, expression key, probe never minted

First answer leaked return-only outlets (54 rows instead of 43) through
`having store_sales_agg.outlet is not null` (and the catalog/web twins), where
`outlet` is a rowset member defined as a CAST of the join key
(`cs.call_center.call_center_sk::bigint as outlet`) and the union join is on
`catalog_returns_agg.outlet = catalog_sales_agg.outlet`. Reproduced from the logged
probe3 body: the generated SQL contains NO `_virt_presence` probe at all; the having
renders as a plain null test on the post-merge fused column
(`"young"."catalog_sales_agg_outlet" is not null`), which is vacuous, and the
return-only outlet 4 (sales NULL) survives. The `_coalescing_presence_probe`
eligibility gate (`build.py:3939-3949`, membership in
`coalescing_relation_members() | subset_sources()` plus derivation ROWSET/ROOT)
apparently does not admit this cast-expression member.

Four probes (writes 2-5) went to discovering the trap and the workaround
(`having <side>.sales is not null` on a non-key measure), which is what the passing
final uses. Secondary cost, shared with q49: the six fact-model explores at 12-20k
chars each (about 94k chars) get re-sent every iteration.

**Verdict: silent framework bug**, same family as q59 (null test on a coalescing join
key member silently no-ops); here it is at least consistent, but the syntax example
(`trilogy/ai/syntax_examples.py:515,606-608`) actively recommends "restrict rows with
explicit `is not null` filters" without warning that the join-key member itself only
works through the probe rewrite, which has these holes.

---

## q49 (529k tokens, pass) - no engine bug; the sink is explore payload times iterations

The write timeline is clean: one probe (worked first try, rank-in-HAVING already
correct), one answer (34 rows, PASS, first attempt). There is no rewrite loop and no
wrong result anywhere in the log. The 529k decomposes as:

- 143,616 chars of tool results total; 6 model explores (store/catalog/web sales +
  returns, 12-20k chars each) are about 93k of it, plus `agent-info query` (16.6k) and
  4 more syntax examples. All of it rides in context for each of 23 iterations
  (506,818 prompt tokens, 458,496 cached, so the spend is mostly cache-read rebilling).
- The "3 explores of the same file" are: full `store_sales` explore, then
  `--regex date`, then `--regex ticket|item_sk|net_profit|net_paid|quantity`. What it
  was hunting, plus the 3 subsequent denied `file read` attempts on the raw model
  files, was the sales-to-returns linkage (join keys / grain) which the full explore
  output does not surface compactly enough to retain. The 5 syntax lookups are
  deliberation over the per-channel rank idiom (scoped-join, union-stack,
  query-structure, window-period-over-period, rank-over-rollup) before a first-try
  correct 3-channel union answer.

**Verdict: agent-with-proof / tooling cost, no framework bug.** Actionables live in
existing docs (`handoff_enriched_token_reduction.md`, `plan_close_ingest_gap.md`):
a compact cross-fact join/grain view in `explore` output would have removed the
re-explores and the blocked `file read` attempts; this query is otherwise the
benign shape of a 500k spend, i.e. wide exploration for a genuinely 6-model question.

---

## Notes for the fixer

- All repros were run through `evals/common/scoring.make_scoring_engine` on a COPY of
  the run workspace (`warehouse.duckdb` + `trilogy.toml` + `root/`); the run dirs are
  shared and DuckDB locks exclusively, so copy first.
- q72 Bug A gives a crisp acceptance test: with the two dim keys null-safe, the run's
  own `query72.preql` matches `tests/modeling/tpc_ds_duckdb/query72.sql` exactly.
- The q59 312/318 pair (two logged bodies differing only in projected columns) is a
  ready-made A/B for the probe-placement fix.
- Known verdicts respected: q77's prior-years findings are unrelated; the
  WHERE-does-not-cross-filter-inline-aggregates design is not implicated anywhere
  above (q72 Bug B is a FLAT where dropped from a sibling branch, a different
  contract).
