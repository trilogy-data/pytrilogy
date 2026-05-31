# Bug handoff: window function (`partition by`) over `sum(...) by rollup` drops/duplicates rollup rows

## Summary

A window function with a `partition by` clause computed over a `sum(...) by rollup`
aggregate produces an **incorrect result set**: the ROLLUP-generated subtotal /
grand-total rows (the ones with NULL grouping keys) are dropped, duplicated, or
cross-joined into NULL-total rows. The Trilogy parses and resolves fine — this is a
**SQL-generation / planner** bug, not a modeling error.

The rollup itself is correct. A partition-less `rank(...) over (order by ...)` that
ranks by **all** the grouping keys is also correct. The breakage appears specifically
when a window with `partition by` (or one that ranks a *subset* of the rollup keys) is
layered on the rollup result — the window node is planned against the pre-rollup grain
and loses/mismatches the rollup grouping-set rows.

Surfaced in the TPC-DS agent eval (q70, and the same family q77/q80 —
`20260531-035151_enriched`). It is the single biggest blocker for the
rollup-with-rank-within-parent query family; agents reliably write the natural form and
get malformed output, while the hand-authored reference only works via a fragile
rowset-materialization + NULL-pattern workaround (see below).

## Minimal, self-contained repro

```trilogy
key id int;
property id.g1 string;
property id.g2 string;
property id.v int;
datasource t (id, g1, g2, v)
  grain (id)
  query '''select 1 as id,'a' as g1,'x' as g2,10 as v
           union all select 2,'a','y',20
           union all select 3,'b','x',5''';

auto total <- sum(v) by rollup g1, g2;
auto rnk   <- rank(g1) over (partition by g1 order by total desc);

select g1 as r_g1, g2 as r_g2, total, rnk
order by total desc nulls first;
```

`trilogy run repro.preql duckdb`

**Expected** (correct ROLLUP(g1,g2) is 6 rows — see control below): the 3 leaf rows,
the 2 g1-subtotal rows `(a,NULL,30)`/`(b,NULL,5)`, and the grand total `(NULL,NULL,35)`,
each with a rank.

**Actual**: a malformed set — duplicated `(b,x,5)` and `(b,NULL,5)` rows, **no grand
total**, 7 rows. (Exact symptom shifts with the window shape: `partition by g1` →
3 rows, all leaves; `rank(g1) over (partition by <grouping-level>)` → 5 rows, grand
total dropped; q70's `rank() over (partition by hierarchy_level)` → NULL-total
cross-product. All wrong.)

### Control — the rollup alone is correct (remove the window)

```trilogy
auto total <- sum(v) by rollup g1, g2;
select g1 as r_g1, g2 as r_g2, total order by total desc nulls first;
```
→ 6 correct rows: `(NULL,NULL,35)`, `(a,NULL,30)`, `(a,y,20)`, `(a,x,10)`, `(b,x,5)`,
`(b,NULL,5)`.

### Also correct — partition-less rank over ALL the rollup keys

```trilogy
auto total <- sum(v) by rollup g1, g2;
auto rnk   <- rank(g1, g2) over (order by total desc);
select g1 as r_g1, g2 as r_g2, total, rnk order by total desc nulls first;
```
→ 6 rows, NULL rollup rows preserved. So the window machinery is not wholesale broken —
it is the **`partition by` (or a subset-key rank) over the rollup output** that mis-plans.

## What narrows it

- `sum() by rollup` + NO window → correct.
- `sum() by rollup` + `rank(<all keys>) over (order by ...)` (no partition) → correct.
- `sum() by rollup` + `rank(...) over (PARTITION BY ...)` → WRONG (rows dropped/dup'd).
- Adding `grouping(k) by rollup ...` concepts and a `level <- grouping+grouping`, then a
  window `partition by level`, makes it worse — separate rollup CTEs for the sum vs the
  grouping/level don't align on the NULL rollup rows (the q70 cross-product symptom:
  duplicate `('TN', None)` rows, one with `total=NULL level=1`, one with
  `total=-444M level=NULL`).

## Real-world failure (q70)

Generated `query70.preql` (agent):
`total_profit <- sum(net_profit) by rollup state, county`,
`g_state/g_county <- grouping(...) by rollup ...`, `hierarchy_level <- g_state+g_county`,
`state_rank <- rank(state) over (partition by hierarchy_level order by total_profit desc)`.
Executing it yields 6 rows, of which 5 have a NULL `level` or NULL `total` — a
cross-product of a `(state,county,total)` rollup CTE and a `(state,county,level)` rollup
CTE that fail to join on the NULL-keyed rows.

## Current workaround (fragile; reference only)

`tests/modeling/tpc_ds_duckdb/query70.preql` documents and dodges this: it materialises
the rollup into a `rowset` carrying `total_sum, r_state, r_county`, then derives the
hierarchy level from the **NULL pattern** of the rollup output
(`loc_level(state,county) = case when county is not null then 0 when state is not null
then 1 else 2 end`) instead of `grouping()`, and partitions the rank by
`@loc_level(...)`/`@partition_state(...)` expressions over the rowset. This is
non-obvious and agents do not discover it. (Note: even a naive `rowset rolled <- select
sum(v) by rollup ...; select ..., rank(...) over (partition by rolled.r_g1 ...)` still
mis-plans in the minimal case — the working reference relies on the specific
NULL-pattern-derived partition expressions, which suggests the rowset boundary alone is
not a reliable fix.)

## Desired fix

A window function (with or without `partition by`, ranking any subset of keys) applied
to a `sum(...) by rollup` result should operate on the **materialised rollup output**
(all grouping-set rows, NULL-keyed subtotals/total included), not be re-planned against
the pre-rollup source grain. Equivalently: emit the rollup as a CTE and run the window
over that CTE's rows, joining the sum and any `grouping()` columns from the **same**
rollup CTE so the NULL-keyed rows stay aligned.

## Repro assets

Minimal repros used above are inline (no DB needed — the `query '''...'''` datasource
supplies the rows). The live failure is reproducible via the scoring engine:

```python
import sys; sys.path.insert(0, 'evals'); from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260531-035151_enriched/workspace')
eng = scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
gen = (ws/'query70.preql').read_text()
rows = list(eng.execute_raw_sql(eng.generate_sql(gen)[-1]).fetchall())   # 6 rows, 5 malformed
```
