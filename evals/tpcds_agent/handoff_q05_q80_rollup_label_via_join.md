# Handoff: q05/q80 rollup-with-subtotals — NOT a framework bug, an idiom/model gap

**Status:** diagnosed 2026-06-22 against enriched rebaseline run
`evals/tpcds_agent/results/20260622-174304` (58/99 pass). q05 and q80 are the two
most expensive *failing* traces (q05: 68 turns / 5.28M tokens; q80: 42 turns /
2.06M). Both are "channel report with per-channel subtotals + grand-total row".

## TL;DR — the rollup machinery is correct; verified

The earlier suspicion ("rollup + enrichment-join drops the NULL-key subtotal
rows") does **not** reproduce. The `by rollup` planner emits the grand-total and
subtotal grouping-set rows correctly today, **including when the grouping key is a
CASE/coalesce-derived dimension label**. The canonical reference query for q05
(`tests/modeling/tpc_ds_duckdb/query05.preql`) runs and produces the rollup rows.

This is an **agent-idiom / model-discoverability** problem (eval "Model Problem"
bucket), not a planner defect. Do not change the planner. The fix is in the model
+ guidance so the agent reaches for the working idiom instead of a join.

## What actually happens

The agent (q05) understood the task and had correct per-entity values by ~turn 35,
then burned ~30 more turns trying to add subtotals. It kept reaching for the wrong
shape: **pre-aggregate with rollup, then JOIN a dimension lookup to get the display
label/text-id.** That join is where it lost the rows — the rolled-up rows have a
NULL grouping key, and joining a dimension onto them either drops them (INNER join,
NULL key matches nothing) or fails to resolve. It never tried deriving the label
*inline*, so it never found the path that works. At turn 66 it gave up and
submitted a no-rollup version (`workspace/query05.preql` has no `rollup` at all) →
fails scoring for missing the subtotal/grand-total rows.

## Repros (run from repo root)

```python
import sys; sys.path.insert(0,'evals')
from pathlib import Path
from common import scoring
ws=Path('evals/tpcds_agent/results/20260622-174304/workspace')   # any enriched workspace
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
```

### ✅ Works — label derived inline (CASE on the in-fact enum), rollup over it
```sql
import raw.all_sales as s;
auto lbl <- case when s.channel='STORE' then 'store' when s.channel='CATALOG'
  then 'catalog' when s.channel='WEB' then 'web' else null end;
where s.date.year = 2000
select lbl, sum(s.ext_sales_price) by rollup lbl -> amt
order by lbl nulls first limit 10;
```
Yields the grand total `(None, 2131980422.48)` **plus** all three channel
subtotals `('store',…) ('catalog',…) ('web',…)`. Subtotals present. This is the
shape the reference uses (label via CASE, measures gated by their own dim,
`by rollup` inside a `def windowed(...)`, `coalesce(…,0)`).

### ✅ Works — rollup read straight out, no enrichment join
```sql
import raw.physical_sales as s;
auto sid <- s.store.id;
with rolled as
  select sid, sum(s.ext_sales_price ? s.date.year=2000) by rollup sid -> amt;
select rolled.sid, rolled.amt order by rolled.sid nulls first limit 8;
```
Grand-total `(None, …)` row present. (Note: a second NULL-`sid` row also appears —
that is the *legitimate* null-store group: 6,317 store-sale rows in 2000 have an
unmatched store FK. `grouping()` distinguishes it from the rollup total. This dual
NULL is expected, not a bug.)

### ❌ The trap the agent fell into — enrich the rolled-up grain by joining a dim
```sql
import raw.physical_sales as s;
auto sid <- s.store.id;
with rolled as
  select sid, sum(s.ext_sales_price ? s.date.year=2000) by rollup sid -> amt;
select rolled.sid, rolled.amt, s.store.name      -- joining a dim onto rollup output
  where rolled.sid = s.store.id
order by rolled.sid nulls first limit 8;
```
→ `UnresolvableQueryException: Could not resolve connections …`. (In the agent's
hand-written SQL the equivalent shape rendered as an `INNER JOIN` that silently
dropped the NULL-key rollup rows.) **Lesson: never enrich a rolled-up grain via a
join — derive every displayed dimension inline from fact-native columns.**

## Recommended fix (model + guidance, no planner change)

1. **Surface the labels as concepts in `raw/all_sales.preql`** so the agent doesn't
   reinvent them via a join. The pieces are all fact-native there (`channel`,
   `channel_dim_text_id`, `return_channel_dim_text_id`):
   ```
   auto channel_label <- case when channel='STORE' then 'store channel'
     when channel='CATALOG' then 'catalog channel'
     when channel='WEB' then 'web channel' else null end;
   auto entity_text <- coalesce(channel_dim_text_id, return_channel_dim_text_id);
   ```
   Add a one-line comment pointing at the rollup idiom (subtotals via
   `sum(x) by rollup channel_label, entity_id`, label derived inline).

2. **Add a CLI/agent-guidance note** (the rollup section): to produce
   subtotal/grand-total rows, put `by rollup <dims>` on the aggregate and derive
   the displayed dims **inline** (CASE/coalesce on fact columns). Do **not**
   pre-aggregate then join a dimension table for the label — the rollup's NULL
   grouping keys won't survive the join.

## Validate the fix

Re-run the two queries 10× each and compare `pass_rate` (single runs are noisy):
```bash
python evals/tpcds_agent/repeat_query.py --query-id 5  --repeats 10 --scale-factor 1
python evals/tpcds_agent/repeat_query.py --query-id 80 --repeats 10 --scale-factor 1
```
Success = agent emits a `by rollup` with an inline-derived label and the scored
output includes the subtotal + grand-total rows.

## Related (don't conflate)

- `bug_grouping_id_over_rollup_no_groups.md`, `bug_rollup_inferred_grain_recurses_in_def.md`
  (the `by rollup ()` grain-inference recursion — why the reference names rollup
  keys explicitly inside the `def`), `bug_composite_rollup_agg_union_drops_groupby.md`.
  Those are real planner items; this one is not.
- `bug_outer_scoped_join_two_rowset_measures.md` — a real planner bug found while
  diagnosing q05: the agent split `all_sales` into two aggregate rowsets and tried
  to `full join` them; INNER blends two rowset measures fine but LEFT/FULL break.
  Separate from this idiom note (which is about not splitting `all_sales` at all).
