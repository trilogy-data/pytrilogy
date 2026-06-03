# Bug handoff: arithmetic on two `@def` calls using inferred-grain `by rollup ()` recurses

## Summary

`sum(x) by rollup ()` (empty parens) rolls up over the **inferred select grain**.
It works inline and in a single function call, but when a `def` body uses it and
**two such calls are combined with an arithmetic operator** in one select column,
SQL generation throws `RecursionError building concept local.<alias> with grain
Grain<...base grain...>`. The grain inference for the combined column collapses to
the base grain and re-enters itself.

It is a **planner / grain-inference** bug, not a modeling error: the same query
resolves with explicit rollup keys (`by rollup k1, k2`), or when the two
aggregates are written inline instead of through the function.

## Minimal repro

`evals/tpcds_agent/repro_rollup_inferred_grain_in_def.py` (prints the matrix):

| case | result |
|---|---|
| `def f(m) -> sum(m) by rollup ()`; `@f(v)` (single) | OK |
| `def f(m) -> sum(m) by rollup ()`; `@f(v) - @f(w)` | **FAIL** `RecursionError` |
| `def f(m) -> sum(m) by rollup ()`; `@f(v) + @f(w)` | **FAIL** `RecursionError` |
| `def f(m) -> sum(m) by rollup g`; `@f(v) - @f(w)` (explicit key) | OK |
| `(sum(v) by rollup ()) - (sum(w) by rollup ())` (inline, no def) | OK |

```trilogy
key id int;
property id.g string;
property id.v int;
property id.w int;
datasource t (id, g, v, w) grain (id)
query '''select 1 as id, 'a' as g, 10 as v, 1 as w
   union all select 2, 'a', 20, 2
   union all select 3, 'b', 5, 3''';

def f(m) -> sum(m) by rollup ();
select g, @f(v) - @f(w) as a;   -- RecursionError building concept local.a
```

## What narrows it

All three are required to trigger it:
1. the aggregate is defined in a `def` (inline `by rollup ()` is fine);
2. the body uses the empty-paren inferred grain `by rollup ()` (explicit keys are fine);
3. two such calls are combined arithmetically (`+` or `-`) in one column (a single call is fine).

## Real-world failure (TPC-DS q05)

`tests/modeling/tpc_ds_duckdb/query05.preql` factors its windowed rollup sum into
`def windowed(metric, date_field, id_field, lo, hi)`. The `profit` column is
`@windowed(net_profit, …) - @windowed(return_net_loss, …)`. With `by rollup ()`
in the def body this recurses; the file works around it by naming the rollup keys
explicitly (`by rollup channel_label, entity_id`) in the def. (An earlier symptom
was `RecursionError building concept local.sales` — the recursion surfaces on the
first select concept, not necessarily the offending one.)

## Desired fix

Grain inference for `by rollup ()` should resolve once against the select grain
and be reused when the aggregate is reached through a `def` and combined with
another such aggregate, instead of re-deriving (and re-entering) the enclosing
concept. Equivalently: an arithmetic combination of two inferred-grain rollups
should infer the same grain as the inline form, which already works.

## Repro assets

- `evals/tpcds_agent/repro_rollup_inferred_grain_in_def.py` — runs the matrix
  above through `generate_sql`; no DB needed.
