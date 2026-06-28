# q70 — `grouping()` in a WHERE filter → DuckDB "GROUPING statement cannot be used without groups"

**Status:** CLOSED / FIXED 2026-06-28. `_propagate_select_grouping` in
`trilogy/parsing/v2/select_finalize.py` now scans `select.where_clause.conditional` for
STANDARD-mode `grouping()`/`grouping_id()` wrappers and raises a clean author-time
`InvalidSyntaxException` (fires regardless of rollup spec, before the spec check). HAVING
`grouping()` is left alone — it runs post-aggregation and is valid even without a rollup spec
(verified: `having grouping(brand)=0` executes as a no-op filter). Tests
`tests/engine/test_duckdb.py::test_grouping_in_where_raises_clean_error` and
`::test_grouping_in_having_no_rollup_executes`.

Run: `evals/tpcds_agent/results/repeat_q70_20260628-161336_enriched/` (failing rep `agent_log.q70.r03.jsonl`, seen 2x across reps).

## Symptom

`trilogy run query70.preql` succeeds at `generate_sql` but the generated SQL throws on execute:

```
(_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups
LINE 42:     grouping("ss_store_store"."S_STATE") as "_virt_agg_grouping_9769516219811737"
```

The offending CTE (`yummy`) is a standalone scalar `grouping()` with **no** `GROUP BY`:

```sql
yummy as (
SELECT
    grouping("ss_store_store"."S_STATE") as "_virt_agg_grouping_9769516219811737"
FROM
    "store" as "ss_store_store"),
```

That virtual concept is then consumed in a downstream filter
(`WHERE "yummy"."_virt_agg_grouping_..." = 1 OR ...`).

## What the agent wrote (idx 21, r03)

The misuse is `grouping()` inside the **main query's WHERE clause**:

```
where ss.date.year = 2000
  and (grouping(ss.store.state) = 1 or ss.store.state in top_states.state)   -- <-- grouping() in WHERE
select
    ss.store.state, ss.store.county,
    sum(ss.net_profit) as total_net_profit,
    grouping(ss.store.state) + grouping(ss.store.county) as hierarchy_level,
    ...
by rollup (ss.store.state, ss.store.county)
```

The agent was trying to keep only subtotal/total rows (`grouping=1`) plus the top-5 states.
The canonical `tests/modeling/tpc_ds_duckdb/query70.preql` never uses `grouping()` in any
WHERE/HAVING filter — it only uses `grouping()` in projection-derived `auto` concepts
(`g_state`, `g_county`, `lochierarchy`) and filters via `ss.store.state in top_states.ts_state`.
Canonical `generate_sql` produces valid SQL (3 `grouping()` calls, all under `GROUP BY ROLLUP`).

## Minimal repro

`.venv/Scripts/python.exe` (model = the 4-row `_ROLLUP_GROUPING_MODEL` from tests/engine/test_duckdb.py):

```python
from trilogy import Dialects
engine = Dialects.DUCK_DB.default_executor()
engine.execute_text(MODEL)          # key sale_id; brand, class, amount
engine.execute_text("""
where grouping(brand) = 1 or brand = 'B'
select brand, class, sum(amount) as total,
by rollup (brand, class);
""")
```

`generate_sql` succeeds; execute raises:

```
WITH quizzical as (SELECT amount, brand, class FROM sales),
highfalutin as (SELECT grouping("quizzical"."brand") as "_virt_agg_grouping_..." FROM "quizzical"),   -- groupless GROUPING()
wakeful as (... FULL JOIN ... WHERE "highfalutin"."_virt_agg_grouping_..." = 1 OR ...)
SELECT brand, class, sum(amount) FROM wakeful GROUP BY ROLLUP (1, 2)
-- (_duckdb.BinderException) GROUPING statement cannot be used without groups
```

This is the exact shape of the agent's `yummy` CTE.

Note: `grouping()` in **HAVING** with no rollup (the agent's `top_states` rowset,
`having grouping(ss.store.state) = 0`) is NOT a framework bug — it executes fine because the
plain aggregate emits an implicit `GROUP BY brand`, and `grouping(<group key>) = 0` is a valid
(no-op) post-aggregate filter. The reproducible bug is only `grouping()` in **WHERE**.

## Classification: guard-gap (should be a clean author-time error)

`grouping()` in a WHERE is semantically invalid in standard SQL — WHERE is evaluated *before*
grouping, so `GROUPING()` has no grouping set to anchor to. The framework should reject it at
author time, not emit SQL that DuckDB rejects.

## Root cause (file:line)

`trilogy/parsing/v2/select_finalize.py`:

- `_propagate_select_grouping` (lines 950-985) is the only place that validates / propagates
  the SELECT-level rollup spec onto `grouping()` wrappers. It iterates **only**
  `select.selection` (the projection) — see the loops at lines 966-973 (no-spec rejection) and
  978-985 (spec propagation). It never inspects `select.where_clause` or `select.having_clause`.
- Likewise `_validate_grouping_args_are_concepts` (921-947) only walks `select.selection`.
- Consequence: a `grouping()` `AggregateWrapper` sitting in the WHERE conditional is neither
  rejected (the no-rollup guard at 964-973 doesn't apply because the *main* query DOES have a
  rollup spec, and even with no spec the guard only scans projection items) nor propagated to
  inherit the rollup `by`/mode. It stays `STANDARD` mode with empty `by`, so the planner
  materializes it as a standalone aggregate → the groupless `_virt_agg_grouping_*` CTE
  (`grouping(col) FROM <parent>` with no `GROUP BY`) → BinderException at execute.
- WHERE-clause aggregates are otherwise handled (`_collect_condition_aggregates` 505-525,
  `_validate_where_aggregate_matches_select` 528-556) but none of those paths special-case or
  reject `grouping()`/`grouping_id()`.

## Correct behavior

Raise a clean author-time `InvalidSyntaxException` when a `grouping()`/`grouping_id()` wrapper
appears in a WHERE clause (and, for completeness, in HAVING when there is no rollup spec) —
guiding the author that `grouping()` is a post-aggregate level indicator that can only be used
in SELECT / HAVING / ORDER BY of a query carrying a `by rollup/cube/grouping sets` clause. The
fix belongs alongside `_propagate_select_grouping`, extending the grouping scan to
`select.where_clause.conditional` (reject) and `select.having_clause.conditional` (reject when
no spec). Do NOT silently materialize a groupless `GROUPING()` CTE.
