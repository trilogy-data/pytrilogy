# **P0** q36: named intermediates before `by rollup` + partitioned window re-anchor at fact grain and fan out

## Symptom

A `by rollup (category, class)` select with a partitioned `rank()` window returns massively
duplicated rows when the window's partition CASE key references the rollup output through a
*chained* named intermediate (an `auto` that references another `auto`). The generated SQL
contains a spurious CTE at item/fact grain that is INNER JOINed back onto the ROLLUP CTE on
category/class, keys that are neither unique nor non-NULL on rollup output (subtotal rows are
NULL-injected), so every rollup row fans out per matching item/fact row.

Observed in the 2026-08-13 agent runs
(`evals/tpcds_agent/results/20260813-125008_ingest/agent_log.q36.conversation.txt`):
9030 rows instead of 133, and a 94M-row intermediate in a later consumer of the same shape.
Writing the same query with the expressions inlined into single-level autos emits clean SQL.

## Reproducible-on

HEAD `2435237d4` (branch `more-benchmarking`, freshly rebased onto main). Reproduced 2026-08-15
via `evals/common/scoring.make_scoring_engine(db, ws, 'tpcds')` + `generate_sql` against the
enriched run workspace model
(`evals/tpcds_agent/results/20260813-125008_enriched/workspace`, db copied to scratchpad).

## Minimal repro

### BAD, named-intermediate chain (9030 rows, expected 133)

```
import raw.store_sales as ss;

auto total_profit <- sum(ss.net_profit);
auto total_sales <- sum(ss.ext_sales_price);
auto gm <- total_profit / total_sales;
auto g_cat <- grouping(ss.item.category);
auto g_class <- grouping(ss.item.class);
auto level <- g_cat + g_class;
auto parent <- case when level = 0 then ss.item.category else null end;
auto rnk <- rank(ss.item.category, ss.item.class) over (partition by level, parent order by gm asc);

where ss.sale_date.year = 2001 and ss.store.state = 'TN'
select
    gm,
    ss.item.category,
    ss.item.class,
    level,
    rnk
by rollup (ss.item.category, ss.item.class)
order by level desc nulls first, ss.item.category asc nulls first, rnk asc nulls first
limit 100;
```

Generated SQL (excerpt), note the spurious item-grain CTE `uneven` and the join back onto the
rollup CTE `questionable`:

```sql
uneven as (
SELECT
    "thoughtful"."ss_item_category" as "ss_item_category",
    "thoughtful"."ss_item_class" as "ss_item_class",
    "thoughtful"."ss_item_sk" as "ss_item_sk"
FROM "thoughtful"
GROUP BY 1, 2, 3),
questionable as (
SELECT ... sum(...) / sum(...) as "gm", ...
FROM "thoughtful"
GROUP BY ROLLUP (1, 2))
SELECT
    ...
    rank() over (partition by "questionable"."level", CASE
        WHEN "questionable"."level" = 0 THEN "questionable"."ss_item_category" ... ) as "rnk"
FROM
    "questionable"
    INNER JOIN "uneven" on "questionable"."ss_item_category" is not distinct from "uneven"."ss_item_category"
        AND "questionable"."ss_item_class" is not distinct from "uneven"."ss_item_class"
```

The join is fully spurious: no `uneven` column is referenced in the outer SELECT (the CASE is
already resolved against `questionable`), yet the join multiplies each (category, class) rollup
row by its item count, and matches subtotal rows against data-NULL leaf rows. 133 rows -> 9030.
The worst spelling (variant D below) joins the raw fact CTE instead and returns 535,135 rows.

### GOOD, same query with expressions inlined into single-level autos (133 rows, clean SQL)

```
import raw.store_sales as ss;

auto gm <- sum(ss.net_profit) / sum(ss.ext_sales_price);
auto level <- grouping(ss.item.category) + grouping(ss.item.class);
auto parent <- case when grouping(ss.item.class) = 0 then ss.item.category else null end;
auto rnk <- rank(ss.item.category, ss.item.class) over (partition by level, parent order by gm asc);

where ss.sale_date.year = 2001 and ss.store.state = 'TN'
select
    gm,
    ss.item.category,
    ss.item.class,
    level,
    rnk
by rollup (ss.item.category, ss.item.class)
order by level desc nulls first, ss.item.category asc nulls first, rnk asc nulls first
limit 100;
```

Clean SQL: one rollup CTE, window and CASE rendered directly over it, no extra join.

## Trigger matrix

All variants share the same where/select/rollup/order tail; only the auto definitions vary.
Row counts are `count(*)` with the limit removed; correct answer is 133.

| Variant | Aggregates | grouping()/CASE | Window | Rows | Verdict |
|---|---|---|---|---|---|
| A named full | chained (`gm <- total_profit / total_sales`) | chained (`parent <- case when level = 0`) | partition by level, parent | 9030 | BAD: item-grain CTE joined back |
| B inline full | inline | inline (`case when grouping(class) = 0`) | partition by level, parent | 133 | clean |
| C named agg only | chained | inline | partition by level, parent | 133 | clean |
| D named grouping only | inline | chained (`case when level = 0`) | partition by level, parent | 535135 | BAD: raw fact CTE joined back |
| E named grouping, CASE on `g_class` directly | inline | named `g_cat`/`g_class`/`level`, but `parent <- case when g_class = 0` | partition by level, parent | 133 | clean |
| F rollup, no window | chained | chained | none | 133 | clean |
| G window, no rollup | chained | none | partition by category | 121 (leaf count) | clean |
| H partition by level only | chained | chained, `parent` unused | partition by level | 133 | clean |

Minimal failing combination (variant D): `by rollup` + partitioned window + a CASE partition key
whose condition references the grouping output **through a chained named intermediate**
(`parent` -> `level` -> `g_cat`/`g_class`). One hop is safe (E: `parent` -> `g_class`); two hops
break. Named aggregate chains alone (C), rollup without the window (F), the window without
rollup (G), and dropping the CASE key from the partition (H) are all clean. This also explains
why the canonical `tests/modeling/tpc_ds_duckdb/query36.preql` is unaffected: its
`partition_cat` conditions on `g_class = 0` directly (the E spelling).

## Root cause

Three cooperating pieces; the third is where the plans diverge.

1. **Parse-time key re-anchoring** - `function_to_concept`,
   `trilogy/parsing/common.py:1036-1041` ("for row ops, assume keys are transitive"): the CASE
   concept `parent` gets `keys = grain = {ss.item.sk}` because its `ss.item.category` argument
   is traversed to the item key. This holds in every variant (verified: `local.parent`
   grain=`['ss.item.sk']` in both A and E), so `parent` is treated as an item-grain dimension
   even though, consumed under `by rollup`, it is a function of the rollup output where that FD
   does not hold (subtotal rows).

2. **Bucket split** - `_partition_by_signature_and_grain`,
   `trilogy/core/processing/v4_helper/group_rules.py:796` (grain-nest test at :881): `parent`
   (grain `{ss.item.sk}`) cannot co-bucket with the chained `gm`/`level` bucket (grain
   `{category, class}`), so the group graph interposes a mid-chain BASIC bucket between the
   ROLLUP aggregate and the fact-grain bucket holding `parent`. Group-graph dumps on this tree:
   bad variant A wires `aggregate -> basic(cat|class: gm, level) -> basic(item.sk: parent)` plus
   `root -> basic(item.sk)`, while good variant E wires `aggregate -> basic(sk|ticket: gm,
   parent)` directly.

3. **Fatal parent election** - `_parent_nodes_for`,
   `trilogy/core/processing/v4_helper/strategy_builder.py:560`, specifically the
   `covered_by_descendant` drop at `strategy_builder.py:775-812`: a ROOT-scan parent is dropped
   only when a sibling parent provides everything it contributes. In E the sibling is the ROLLUP
   aggregate group, whose secondary members re-expose `category`/`class`, so the ROOT scan is
   dropped and everything renders over the rollup CTE. In A/D the sibling is the mid-chain BASIC
   bucket, which exposes only `gm`/`level` (secondary members empty), so the ROOT scan survives,
   is grouped to the bucket grain `{ss.item.sk}` (the `uneven` CTE), and is INNER JOINed back
   onto the rollup output on `category`/`class`. That join violates the contract documented at
   `node_nulls_grouping_keys`, `strategy_builder.py:513-527`: nothing may join back to a
   ROLLUP/CUBE/GROUPING SETS node on a non-unique key because no later dedup absorbs the
   duplicates (the q18 guard covers renames but not this shape).

## Verdict

Framework bug (planner), P0. Correct authored Trilogy silently returns wrong results (9030 or
535k rows vs 133), with the failure gated on a spelling difference (chained vs one-hop named
intermediates) that users cannot be expected to know about; the same shape burned two agent eval
runs. Per placement policy this is a planner fix, not an optimizer rule: the `uneven` join is
provably omittable at plan time. The fix direction is either to anchor a BASIC concept whose
lineage (transitively) contains grouping()/rollup-pass members to the grouping output rather
than to its raw-dimension keys, or to make the mid-chain BASIC bucket re-expose the rollup
grouping keys it rides so the ROOT re-scan parent is recognized as redundant in
`_parent_nodes_for`. Repro harness: scratchpad `q36_repro.py` (this session), SQL for all eight
variants saved alongside it.
