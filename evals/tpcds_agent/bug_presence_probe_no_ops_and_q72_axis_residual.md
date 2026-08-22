# Presence-probe no-ops (q59/q77) and the q72 aggregate-axis residual

Rewritten 2026-08-21 from `bug_silent_ingest_sinks_q49_q59_q72_q77.md` (filed
2026-08-20 over run `20260820-031800_ingest_deepseek_deepseek-v4-flash`), keeping
only what is still open. Both items are SILENT: exit 0, no warning, wrong rows.

Closed out of the old file, no action left:

- **q72 Bug A** (rowset boundary stripped non-key nullability, 404 NULL groups
  dropped by a plain `=` rejoin) and most of **Bug B**. Five fixes landed
  2026-08-20, gated by `tests/engine/test_duckdb_rowset_null_group_rejoin.py`
  and `tests/engine/test_duckdb_union_join_aggregate_population.py`. Section 2
  below is the one piece that did not land.
- **q49** (529k tokens, pass). Never an engine bug: the spend was explore
  payload times iterations. `docs/explore_compact_output_design.md` (explore v3,
  outline by default) landed the same day and is the fix.

Both items below reproduce on committed models, so neither needs a copy of the
run workspace any more.

---

## 1. A null test on a coalescing join-key member no-ops on one side

q59 (507k tokens, pass) and q77 (520k tokens, pass) both burned their tokens
discovering this and working around it. The first answer in each case was
already correct; everything after it was the agent reconciling a filter that
silently did nothing.

### Self-contained repro (no workspace, verified 2026-08-21)

```trilogy
key store_key int;
key week_seq int;
property <store_key, week_seq>.amt float;
datasource f (s: store_key, w: week_seq, a: amt)
grain (store_key, week_seq)
query '''
select 1 as s, 1 as w, 10.0 as a
union all select 2 as s, 1 as w, 20.0 as a
union all select 2 as s, 2 as w, 30.0 as a
union all select 3 as s, 2 as w, 40.0 as a
''';

rowset this_year <- where week_seq = 1 select store_key as sk, sum(amt) as total;
rowset next_year <- where week_seq = 2 select store_key as sk, sum(amt) as total;

select this_year.sk, this_year.total, next_year.total
union join this_year.sk = next_year.sk
having this_year.sk is not null and next_year.sk is not null;
```

Store 1 is this-year only, store 3 is next-year only, store 2 is on both sides,
so the intersection the `having` asks for is store 2 alone.

| | rows |
|---|---|
| want | `[(2, 20.0, 30.0)]` |
| got | `[(2, 20.0, 30.0), (3, NULL, 40.0)]` |

The `next_year.sk is not null` half works (store 1 is gone). The
`this_year.sk is not null` half does nothing. Swapping the null test onto a
non-key measure (`having this_year.total is not null and next_year.total is not
null`) returns the correct single row, which is the workaround both q59 and q77
eventually found.

### Root cause

`Factory._coalescing_presence_probe` (`trilogy/core/models/build.py:3916`)
exists precisely for this: it rewrites `member is [not] null` on a coalescing
join-key member into a single-arg `COALESCE` passthrough that must materialize
on the member's OWN side, before the merge, so it is NULL exactly when that
side is absent. Its docstring cites this TPC-DS q59 shape as the motivating
case.

Both probes are minted here (the rendered SQL carries two `_virt_presence`
references), but only one lands below the merge. From the repro's SQL:

```sql
cheerful as (            -- next_year side, PRE-merge: correct
SELECT ..., coalesce("quizzical"."_next_year_sk") as "_virt_presence_4641148367766825" ...),
yummy as (               -- the merge
SELECT ..., coalesce("cheerful"."next_year_sk","cooperative"."this_year_sk") as "this_year_sk"
FROM "cooperative" RIGHT OUTER JOIN "cheerful" on ...)
SELECT ... FROM "yummy"
WHERE coalesce("yummy"."this_year_sk") is not null   -- POST-merge: self-defeating
```

The this-side probe is applied to the merge output, where `this_year_sk` is
already `coalesce(next_year_sk, this_year_sk)`. Wrapping a fused column in
`coalesce` cannot observe that this side was absent, which is the exact failure
the docstring warns about ("collapse the probe back onto the member"). Placement
is the bug, not eligibility.

### Two run-observed variants this reduced repro does NOT cover

Keep them in mind when fixing; both bodies are recoverable from the run logs
(`agent_log.q59.jsonl` writes 2 and 3, `agent_log.q77.jsonl` probe 3).

- **Plan-shape dependence (q59).** Two bodies differing ONLY in their projected
  column list returned 312 and 318 rows from the same rowsets, same union join,
  same `having`. In the 312 plan both probes materialized pre-merge; in the 318
  plan the this-side probe collapsed as above. The same authored clause
  filtering in one projection and no-opping in another is what cost the tokens:
  with either behaviour held stable the agent would have stopped at its first,
  already-passing answer. The reduced repro is stable-wrong in both projection
  widths, so this pair remains the sharper A/B.
- **Expression key (q77).** There the member is a CAST of the join key
  (`cs.call_center.call_center_sk::bigint as outlet`) and the generated SQL
  carried NO `_virt_presence` at all: the `having` rendered as a plain null test
  on the fused column, and a return-only outlet survived (54 rows instead of
  43). Note that the reduced repro's cast variant (`store_key::int as outlet`)
  DOES mint both probes and fails the same way as the bare member, so a cast
  alone is not the discriminator. Something else in that shape (rowset of
  aggregates keyed by the cast) misses the eligibility gate at
  `build.py:3938-3949` (membership in `coalescing_relation_members() |
  subset_sources()`, derivation in `ROWSET`/`ROOT`).

### Docs half

`trilogy/ai/syntax_examples.py:515` tells the reader joins never drop rows and
to "restrict rows with explicit `is not null` filters", and the checklist at
:618 spells that as `where <side_a attr> is not null and <side_b attr> is not
null`. Neither says that a null test on the JOIN KEY member itself only works
through the probe rewrite. The worked example at :581 quietly uses a measure
(`having y2021.cnt is not null`), which is the spelling that actually holds. Say
so: test a non-key measure or attribute of the side, not the key.

---

## 2. q72 residual: the aggregate axis keeps a relation member in the branch grain

### What landed 2026-08-20 (context for the residual)

- `v4_node_generators/rowset.py`: the rowset boundary stamps nullability on
  every handle, not just key-like ones (Bug A, the graded FAIL).
- `condition_placement._uncovered_grouping_placements`: an UPSTREAM_MOST row
  atom is copied onto every select-phase aggregate the elected host does not
  feed, so a flat WHERE no longer skips the unfiltered count branch.
- `condition_placement._group_in_active_relation`: an AGGREGATE whose relation
  mate is hosted only by its own lineage ancestors keeps local hosting.
- `merge_node._splits_aggregate_groups`: `_inject_scoped_join_key_exposure` no
  longer surfaces a relation member onto an aggregating parent when it would
  become a new GROUP BY key.
- `strategy_builder._scoped_join_mates` / `_add_relation_axis_contributors`:
  a scoped-join mate counts as covering the concept, and an axis provider
  already merged below a chosen contributor is skipped.

That fixed grain collapse for every shape whose counted row identity holds no
relation member. It did not fix the q72 formulation.

### Repro (committed tpc-ds test model, verified 2026-08-21)

Rendered against `tests/modeling/tpc_ds_duckdb` (generation only, no data
needed):

```trilogy
import catalog_sales as cs;
import inventory as inv;

rowset sales <- where cs.sale_date.year = 1999
  select cs.order_number as order_number, cs.item.sk as item_sk,
         cs.sale_date.week_seq as week_seq, cs.quantity as quantity,
         cs.promotion.sk as promo_sk;
rowset inv_rows <- select inv.item.sk as item_sk, inv.date.week_seq as week_seq,
         inv.date.sk as date_sk, inv.warehouse.sk as warehouse_sk,
         inv.quantity_on_hand as quantity_on_hand;

where inv_rows.quantity_on_hand < sales.quantity
select
    sales.week_seq,
    count(grain(sales.order_number, sales.item_sk, inv_rows.date_sk, inv_rows.warehouse_sk) ? sales.promo_sk is null) as no_promo_cnt,
    count(grain(sales.order_number, sales.item_sk, inv_rows.date_sk, inv_rows.warehouse_sk)) as total_cnt
union join sales.item_sk = inv_rows.item_sk and sales.week_seq = inv_rows.week_seq
limit 10;
```

The authored grain is `week_seq`. Every branch instead groups by
`(week_seq, item_sk)`, the branches re-pair on both columns, and the outer
select merely dedups:

```sql
sparkling as (
SELECT "young"."inv_rows_week_seq", "young"."sales_item_sk",
       count("young"."_virt_filter_...") as "no_promo_cnt"
FROM "young" GROUP BY 1, 2),
late as (
SELECT coalesce("sparkling"."inv_rows_week_seq","sweltering"."inv_rows_week_seq") as "sales_week_seq", ...
FROM "sweltering" FULL JOIN "sparkling"
  on "sweltering"."inv_rows_week_seq" is not distinct from "sparkling"."inv_rows_week_seq"
 AND "sweltering"."sales_item_sk" is not distinct from "sparkling"."sales_item_sk")
SELECT "late"."sales_week_seq", "late"."no_promo_cnt", "late"."total_cnt"
FROM "late" GROUP BY 1, 2, 3
```

So `week_seq` repeats once per item, and the counts are per-item slivers rather
than the authored per-week totals.

Control, the same query with `sales.item_sk` dropped from both counted grain
tuples: branches group by `week_seq` alone, the merge pairs on the single
column, and the shape is correct. That is exactly the boundary the 08-20 fixes
reached.

### Root cause

`concept_graph._aggregate_axis_members` (`trilogy/core/processing/v4_helper/
concept_graph.py:461`, consumed at :1246) widens an aggregate's grouping grain
by every statement-scoped relation member its INPUT GRAIN rides. When the
counted row identity itself contains a relation member (`sales.item_sk`, a key
of the `union join`), the widening makes that member a branch GROUP BY key and
nothing re-aggregates it away.

The obvious narrowing, restricting the candidate set to the aggregate's DIRECT
function arguments, collapses this grain correctly but breaks the q17
composite-union-join family (7 cells in `tests/engine/test_duckdb_rowset.py`),
which depends on the widening. Closing this means separating "the member is the
measure's own axis" from "the member is part of the counted row identity", not
tuning the candidate set.

---

## Notes for the fixer

- The run dirs under `results/` hold only logs now; no workspace copy survives.
  Both repros above run on committed models, and the run logs
  (`agent_log.q59.jsonl`, `agent_log.q77.jsonl`, `agent_log.q72.jsonl`) still
  carry the original bodies if you want the full-size shapes.
- Gates that must stay green for section 2:
  `tests/engine/test_duckdb_union_join_aggregate_population.py` (the four cells
  the 08-20 fixes bought), `tests/engine/test_duckdb_rowset.py` (the q17 family
  the naive narrowing breaks), and the tpc-ds modeling corpus.
- Section 1 needs a row-asserting gate of its own; the repro above is ready to
  drop into `tests/engine/` as-is, with the measure-spelling variant as the
  control.
- Known verdicts respected: the WHERE-does-not-cross-filter-inline-aggregates
  design is not implicated in either item.
