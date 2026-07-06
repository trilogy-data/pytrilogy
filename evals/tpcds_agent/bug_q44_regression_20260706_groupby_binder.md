# q44 regression — GROUP BY BinderException (2026-07-06 run 20260706-023449)

## Symptom
Agent run `evals/tpcds_agent/results/20260706-023449` burned **735,452 tokens** on q44,
emitting the same DuckDB error twice:

```
(_duckdb.BinderException) Binder Error: column "ss_item_id" / "SS_ITEM_SK" must appear
in the GROUP BY clause or must be part of an aggregate function.
```

The engine generated an invalid filter/aggregate CTE. For the minimal repro (below) the
offending CTE is:

```sql
thoughtful as (
SELECT
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    CASE WHEN avg("ss_store_sales"."SS_NET_PROFIT") > "cheerful"."threshold"
         and "ss_store_sales"."SS_STORE_SK" = 1
         THEN "ss_store_sales"."SS_ITEM_SK" ELSE NULL END as "_virt_filter_id_..."   -- SS_ITEM_SK ungrouped
FROM "cheerful" LEFT OUTER JOIN "store_sales" as "ss_store_sales" on 1=1
GROUP BY 1, "cheerful"."threshold"     -- groups by store_id + threshold, NOT item_id
HAVING avg("ss_store_sales"."SS_NET_PROFIT") > "cheerful"."threshold")
```

The HAVING-derived filter concept `_virt_filter_id` wraps the SELECT's grain key
`ss.item.id` (SS_ITEM_SK) in a CASE, but the CTE is grouped by the WHERE's off-grain
dimension `ss_store_id` (+ the cross-statement `threshold`). The content key is neither
grouped nor aggregated → binder error.

Note: the agent eventually reformulated into a working `query44.preql` (the saved
workspace file generates valid SQL, 10 rows) and returned control OK. The framework bug
is what consumed ~500k tokens before it escaped the idiom. The "FAIL" is a
token/answer-scoring outcome, not a crash on the final query.

## Minimal deterministic repro
Against `evals/tpcds_agent/results/20260706-023449/workspace` (harness in task prompt):

```
import raw.store_sales as ss;
auto base <- avg(ss.net_profit) by *;
auto threshold <- base * 0.9;
where ss.store.id = 1
select ss.item.id, avg(ss.net_profit) as avg_profit
having avg_profit > threshold;
```
→ `BinderException ... "SS_ITEM_SK" must appear in the GROUP BY clause`.

## Trigger matrix (both factors are jointly necessary)
| variant | WHERE dim | threshold form | result |
|---|---|---|---|
| off-grain WHERE + BASIC-wrapped grainless scalar | `ss.store.id=1` | `base*0.9` (BASIC over `by *` agg) | **FAIL: GROUPBY** |
| drop WHERE | — | `base*0.9` | OK |
| bare grainless agg (no BASIC wrapper) | `ss.store.id=1` | `avg(ss.net_profit) by *` | OK |
| WHERE on the SELECT grain key | `ss.item.id>0` | `base*0.9` | OK |
| literal threshold | `ss.store.id=1` | `> 100.0` | OK |
| HAVING vs external `by ss.item.id` agg | — | `> 100.0` | OK |
| canonical idiom: cond in WHERE, ext item-grain agg | `ss.store.id=1 and item_avg>threshold` | `base*0.9` | OK (7622 rows) |

So the crash requires **(A) a WHERE on a dimension not in the SELECT grain** (store.id,
while the select grain is item.id) **AND (B) the HAVING compares the inline
select-grain aggregate to a *grainless* (`by *`) aggregate wrapped in a BASIC arithmetic
expression** (`base * 0.9`). Remove either and the SQL is valid. Adding a plain bare
grainless aggregate (no `*0.9`) also works — the BASIC wrapper is load-bearing.

## Causation verdict: PRE-EXISTING bug, newly tripped by idiom choice (NOT a new regression)
Proof:
1. **Orthogonal to all four listed changes.** The minimal repro contains **no union arms**
   (so `union_arm_cast_target` is irrelevant), **no decimal/double CAST** (DataType.DOUBLE
   / decimal aliases irrelevant), **no nullability-specific construct** exercising the
   `find_nullable_concepts` KeyError guard, and **no `is_returned`** usage. The trigger is
   purely a HAVING-vs-grainless-BASIC-scalar + off-grain-WHERE grain-routing shape.
2. **Trigger flips only on grain/BASIC factors** (table above), none of which touch the
   four changed code paths.
3. **Canonical `tests/modeling/tpc_ds_duckdb/query44.preql` still passes** (`pytest -k
   forty_four` → 1 passed). It uses the *other* idiom: external `item_avg_profit <-
   avg(ss.net_profit) by ss.item.id` and puts the aggregate comparison in a rowset
   `WHERE` (`item_avg_profit > 0.9*addr_null_threshold.threshold`), never inline
   `having <agg> > <grainless scalar>`. So this is not a broad break.
4. **Prior passing runs never emitted this binder signature** (per regression evidence) —
   earlier agents wrote the working rowset/WHERE idiom, so the framework path for the
   inline-having-vs-grainless-BASIC-scalar idiom was simply never exercised. It was not
   introduced by the recent changes; this run's agent just chose the unsupported idiom.

## Root cause (file:line)
The HAVING filter concept `ss.item.id ? (avg(ss.net_profit) > threshold and ss.store.id=1)`
gets its aggregate co-grain from `Factory._build_filter_where`
(`trilogy/core/models/build.py:3658-3681`), which pins `aggregate_grain` to the filtered
**content grain** (`ss.item.id`). That co-graining is correct for the *inner* aggregate,
but when the filter condition also fuses a pushed **off-grain WHERE predicate**
(`ss.store.id=1`) and compares against a **grainless BASIC-wrapped scalar** (`base*0.9`,
built at `Grain()` via the `aggregate_grain=base.grain` WHERE factory at
`build.py:3718-3723`), the downstream filter/aggregate node's group-by resolves to the
WHERE dimension (`ss_store_id`) + the scalar (`threshold`) instead of the content key
(`ss.item.id`). The content key is then emitted ungrouped inside the `_virt_filter` CASE.
The defect is that the filter/aggregate CTE's GROUP BY does not include the filter
**content** concept (`ss.item.id`) when the condition mixes a grainless BASIC scalar
comparison with an off-grain WHERE predicate — same family as the known
HAVING/`_virt_filter` grain-routing entries (q21/q23; `_build_filter_where`,
`_promote_having_aggregates_to_outputs`). Not fixed (read-only task).
