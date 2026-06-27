# Bug: expression scoped-join key + cross-rowset filtered-output division + membership WHERE → uncaught `INVALID_REFERENCE_BUG` crash (q2)

**Status:** OPEN — minimal deterministic repro + bisection. Same `INVALID_REFERENCE_BUG` sentinel
family as the (fixed) q64/q14 crashes, different trigger.
**Surfaced by:** TPC-DS q2 (run `20260627-120644`). Surfaces as `Unexpected error: Invalid reference
string found in query` (uncaught `ValueError`), so the agent gets no actionable message and loops
(q2 burned 1.99M tokens / 46 iterations this run).
**Severity:** HIGH — uncaught crash; generates SQL with the `INVALID_REFERENCE_BUG` sentinel where a
CTE alias belongs (dialect/base.py:247 guard at :2386).

## Trigger (precisely bisected) — needs ALL THREE

Two rowsets aggregating the same fact, joined and divided:

```trilogy
import raw.all_sales as all_sales;
auto weeks_in_2001 <- all_sales.date.week_seq ? all_sales.date.year = 2001;

with cur_sales as where all_sales.channel in ('WEB','CATALOG')
  select all_sales.date.week_seq as ws, all_sales.date.day_of_week as dow,
         sum(all_sales.ext_sales_price) as sales_amt;
with ftr_sales as where all_sales.channel in ('WEB','CATALOG')
  select all_sales.date.week_seq as ws, all_sales.date.day_of_week as dow,
         sum(all_sales.ext_sales_price) as sales_amt;

select cur_sales.ws as wk,
    round(cur_sales.sales_amt ? cur_sales.dow = 0          -- (1) filtered output of rowset A
        / nullif(ftr_sales.sales_amt ? ftr_sales.dow = 0, 0), 2) as sunday   -- ...divided by filtered output of rowset B
inner join cur_sales.ws = ftr_sales.ws - 53                -- (2) EXPRESSION scoped-join key
where cur_sales.ws in weeks_in_2001;                       -- (3) membership WHERE
```
→ `ValueError: Invalid reference string found in query`.

| variant | result |
|---|---|
| full (all three) | **ERR** |
| drop membership `where` (1+2) | OK |
| drop the filtered division, project raw cols (2+3) | OK |
| **plain** join key `cur.ws = ftr.ws` instead of `- 53` (1+3, no expr key) | **OK** |
| single-rowset division, no cross-rowset join (1+3, no join) | OK |

So each is load-bearing: (1) a division of **filtered outputs from two different rowsets**, (2) an
**EXPRESSION** scoped-join key (a plain key does NOT crash), and (3) a **membership WHERE**. Drop any
one and it compiles. One day-column is enough (the 7-column pivot is not required).

## Likely fix area

Same `INVALID_REFERENCE_BUG` dangling-CTE family as q64/q14. The expression join key
(`ftr.ws - 53`) mints an anonymous join concept (`add_virtual_concept`); under the membership
existence filter + the cross-rowset filtered-aggregate division, one operand's source CTE is never
materialized and renders as the sentinel. Inspect how the expression-keyed scoped join's anonymous
key interacts with existence-source planning when both rowsets contribute filtered aggregates to one
divided projection. Cf the FIXED expression-join-key work
([[project_join_expression_keys]]) and the existence-sourcing fixes
([[project_q64_nested_membership_two_source_agg_invalid_ref]],
[[project_q14_having_rollup_vs_scalar_invalid_ref]]). At minimum it must become a clean error, not an
uncaught `ValueError`.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-010809/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('q2.preql').read())   # ValueError: Invalid reference string found in query
```
