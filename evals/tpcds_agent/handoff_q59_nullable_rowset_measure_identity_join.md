# Handoff: q59 planner joins rowsets through nullable measure outputs

## Summary

TPC-DS q59 is a silent wrong-result framework bug. The authored query joins two
aggregate rowsets only by store ID and offset calendar week. During planning,
Trilogy adds internal joins on all of the rowsets' projected daily measures.

Those generated measure equalities are ordinary SQL `=` predicates. When a
daily aggregate is NULL on both sides of an internal stitch, `NULL = NULL` does
not match and the otherwise valid store/week row disappears.

There is nothing in the authored query that relates one measure output to
another. The measure joins are planner-generated identity reconstruction.

## Artifact

Latest enriched candidate:

```text
evals/tpcds_agent/results/20260709-105517_enriched/workspace/query59.preql
```

Reference:

```text
tests/modeling/tpc_ds_duckdb/query59.sql
```

The run reports 100 candidate rows and 100 reference rows because both outputs
are limited, but their ordered contents diverge when the first valid nullable
row is dropped.

## Authored query shape

The candidate creates one aggregate rowset for 2001 and another for 2002. Each
has grain `(store ID, store name, week sequence)` and seven filtered sums:

```preql
with this_year as
where ss.date.year = 2001
select
    ss.store.id,
    ss.store.name,
    ss.date.week_seq,
    sum(ss.sales_price ? ss.date.day_of_week = 0) as sun,
    # ... Monday through Saturday
;

with next_year as
where ss.date.year = 2002
select
    ss.store.id,
    ss.store.name,
    ss.date.week_seq,
    sum(ss.sales_price ? ss.date.day_of_week = 0) as sun,
    # ... Monday through Saturday
;
```

The final query declares exactly two relationships:

```preql
union join this_year.id = next_year.id
union join this_year.week_seq = next_year.week_seq - 52
```

It then requires both store IDs to be present and calculates the seven ratios.
No predicate compares `sun`, `mon`, etc. to any other measure.

## Actual generated shape

The initial aggregate CTEs are correct. They group by store/week and render each
daily total as a filtered aggregate such as:

```sql
sum(CASE WHEN date_dim.D_DOW = 5
         THEN store_sales.SS_SALES_PRICE
         ELSE NULL END) AS next_year_fri
```

Later planner-generated CTEs stitch projections back together with an INNER
JOIN containing predicates like:

```sql
INNER JOIN questionable ON
    young.next_year_mon = questionable.next_year_mon
AND young.next_year_sat = questionable.next_year_sat
AND young.next_year_sun = questionable.next_year_sun
AND young.next_year_thu = questionable.next_year_thu
AND young.next_year_tue = questionable.next_year_tue
AND young.next_year_wed = questionable.next_year_wed
AND young.this_year_week_seq = questionable.next_year_week_seq_minus_52
AND young.this_year_store_id = questionable.next_year_store_id
```

The exact generated CTE and virtual-expression names are unstable, but the
important shape is stable: nullable aggregate values are promoted into INNER
JOIN identity predicates in addition to the authored store/week keys.

## Wrong-result evidence

Removing only the output limit gives:

```text
reference rows: 312
candidate rows: 306
```

The candidate has no extra keys. It is missing exactly week 5279 for all six
stores:

```text
('able',  'AAAAAAAACAAAAAAA', 5279)
('ation', 'AAAAAAAAHAAAAAAA', 5279)
('bar',   'AAAAAAAAKAAAAAAA', 5279)
('eing',  'AAAAAAAAIAAAAAAA', 5279)
('ese',   'AAAAAAAAEAAAAAAA', 5279)
('ought', 'AAAAAAAABAAAAAAA', 5279)
```

Every missing reference row has valid Sunday-through-Thursday ratios and NULL
Friday/Saturday ratios. For example:

```text
('able', 'AAAAAAAACAAAAAAA', 5279,
 0.5051258044, 0.9560347769, 0.8167519813,
 0.5935324500, 0.7565868861, NULL, NULL)
```

The source data simply has no Friday/Saturday sales for one side of that paired
week. That is valid and explicitly required to remain NULL. The internal
measure equality evaluates `NULL = NULL` as unknown and the INNER JOIN removes
the row.

With `LIMIT 100`, the first mismatch is therefore positional: the reference's
week 5279 row is followed by candidate week 5280 because 5279 was erased.

## Expected planner contract

A rowset's identity is its declared/projected grain, not all of its output
values. Two materializations of the same aggregate rowset should be stitched by
stable grain keys only.

Projected measures are payload. They must not become join keys merely because
downstream expressions consume them. In particular, nullable aggregate values
cannot safely participate in ordinary equality joins.

If a planner path genuinely must compare nullable identity values, it must use
null-safe equality (`IS NOT DISTINCT FROM` or the dialect equivalent). That is
a defensive fallback, not a substitute for fixing the overly broad identity.

## Likely root area

Investigate the planner path that reconstructs or co-sources rowset projections
for the final ratio expressions. It appears to derive stitch keys from the full
set of projected concepts rather than the rowset grain/native key set.

Useful questions:

1. Why do `next_year.sun` through `next_year.sat` enter the join-key concept
   set when the authored scoped joins mention only store and week?
2. Is a rowset projection being treated as a unique tuple of all outputs rather
   than a keyed relation with measure payload?
3. Are separate ratio CASE expressions causing repeated partial
   materializations that are then stitched through their shared dependencies?
4. Does the stitch-join renderer have a null-safe path that is bypassed here,
   or are these payload concepts incorrectly marked non-null?

This may be related to other projection/stitch identity failures, but its
signature is distinct: there is no authored measure relationship, no rollup,
and no many-to-many fanout. The result loses only rows whose nullable measure
payload reaches a generated equality join.

## Required fix

1. Build internal rowset stitch joins from the rowset grain/key concepts, not
   every shared projected output.
2. Keep aggregate measures as payload even when several downstream expressions
   depend on them.
3. Render null-safe equality for any nullable concept that legitimately remains
   part of an internal identity join.
4. Add a planner invariant or diagnostic that explains why every generated
   stitch key is required; authored scoped joins must not silently expand to
   unrelated measures.
5. Preserve rows carrying NULL filtered aggregates through downstream scalar
   expressions and CASE guards.

## Regression coverage

Add a small execution test with two keyed aggregate rowsets:

- grain `(entity_id, period)`;
- two or more filtered sums, with one category absent so one sum is NULL;
- join only on `entity_id` and an offset `period` expression;
- select guarded ratios from the rowsets.

Assert:

```text
- the nullable-period row remains present;
- its affected ratio is NULL;
- generated stitch joins do not use measure outputs as keys;
- no generated ordinary equality compares nullable measures;
- results equal a hand-written SQL oracle.
```

Include controls for:

- all measures non-null;
- one nullable measure versus several;
- direct division versus CASE-guarded division;
- one downstream expression versus several expressions sharing the measures;
- nullable grain keys, which should use null-safe identity where supported.

