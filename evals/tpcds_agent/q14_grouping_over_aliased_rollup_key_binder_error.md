# Bug: `grouping(<source concept>)` over a rollup whose key is that concept under an ALIAS → DuckDB BinderException "GROUPING child must be a grouping column" (q14)

**Status:** FIXED 2026-06-27 — `_normalize_grouping_args_to_rollup_keys` in `select_finalize.py`
(called from `_propagate_select_grouping`) rewrites a `grouping(<source>)` argument to the rollup-key
alias when the key is projected under one. Reuses the existing `_alias_rename_map`, filtered to
renames whose alias is an actual rollup key (`spec.by`). Test:
`tests/engine/test_duckdb.py::test_grouping_over_aliased_rollup_key_builds`.
Same `GROUPING child must be a grouping column` family as the FIXED q5 case, but the q5
validator did NOT cover this one (q5 only rejects non-`ConceptRef` args; here the arg is a concept).
**Surfaced by:** TPC-DS q14 (run `20260627-174648`). Uncaught `(_duckdb.BinderException)` →
`Unexpected error`, no clean Trilogy message.
**Severity:** HIGH — invalid SQL DuckDB rejects; the agent gets a raw binder error.
**Note:** this was the ONLY crash-type error in an otherwise crash-free run (4.91M tokens, no
>1M-token queries) — the last domino in the rollup/grouping family.

## Symptom

```
(_duckdb.BinderException) Binder Error: GROUPING child "sales_channel" must be a grouping column
LINE: ( ( grouping("juicy"."sales_channel") + grouping("juicy"."sales_item_brand_id") + ... )
```

## Root cause

The rollup key is a concept referenced under an **alias** — `sales.channel as channel`, then
`by rollup (channel, brand_id, ...)`. The generated GROUP BY ROLLUP groups by the **materialized
aliased column**, but `grouping(sales.channel)` renders against the **source column** `sales_channel`,
which is not (under that name) a GROUP BY key in the CTE (`juicy`) → DuckDB rejects it.

This is the same shape as the FIXED q5 binder bug
([[project_q5_grouping_over_rollup_expression_key_binder]]) — grouping()'s argument not matching the
materialized rollup-key column — but there the arg was an **inline expression** and the fix
(`_validate_grouping_args_are_concepts`) only rejects non-`ConceptRef` args. Here the arg IS a concept
(`sales.channel`), so it passes the validator, yet still mis-renders because the rollup key is the
**aliased** form. The q5 fix's sibling — *normalizing* the grouping() arg to the same alias the rollup
key materializes — is what's missing.

## Reproducing query (deterministic; q14 as written)

```trilogy
import raw.all_sales as sales;
auto overall_avg <- avg(sales.ext_list_price) by *;
auto combo_key <- concat(sales.item.brand_id::string,'-',sales.item.class_id::string,'-',sales.item.category_id::string);
rowset qualifying_combos <-
  where sales.date.year between 1999 and 2001
  select combo_key as combo_key, --count_distinct(sales.channel) as channel_count
  having channel_count = 3;

where sales.date.year = 2001 and sales.date.month_of_year = 11
  and combo_key in qualifying_combos.combo_key
select
  sales.channel as channel, sales.item.brand_id as brand_id,
  sales.item.class_id as class_id, sales.item.category_id as category_id,
  sum(sales.ext_list_price) as total_sales, count(sales.ext_list_price) as num_sales,
  --grouping(sales.channel) + grouping(sales.item.brand_id) + grouping(sales.item.class_id) + grouping(sales.item.category_id) as _level
having total_sales > overall_avg
by rollup (channel, brand_id, class_id, category_id)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;
-- generate_sql OK; execute -> BinderException: GROUPING child "sales_channel" must be a grouping column
```

(A trimmed `grouping(sales.channel)+... by rollup (channel, brand_id)` alone compiles+executes — the
full shape with the membership rowset + HAVING is needed to land grouping() in a CTE that aliases the
key; the membership/HAVING are load-bearing for that CTE structure, so a one-liner isolation wasn't
extracted.)

## Likely fix area

In the rollup grouping() render, normalize the `grouping(<concept>)` argument to the **same
materialized column** that the rollup GROUP BY uses for that key (the alias), mirroring the q5
alias-normalization seen in the HAVING-rollup fix
([[project_q14_having_rollup_vs_scalar_invalid_ref]]'s rename thread). The grouping() arg and the
rollup GROUP BY key must reference the identical column. Until then, at minimum raise a clean error
rather than emit invalid SQL.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-174648/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(open('q14.preql').read())[-1]   # succeeds
eng.execute_raw_sql(sql)   # BinderException: GROUPING child "sales_channel" must be a grouping column
```
