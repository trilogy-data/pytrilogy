# q44 — silent EMPTY result: query WHERE not pushed into HAVING-side aggregate (named threshold concept)

Run: `evals/tpcds_agent/results/20260708-135136_enriched` — q44 status=PASS but burned **980,096 tokens**.
Classification: **FRAMEWORK bug** (silent wrong result). Not agent, not guidance.

## Symptom

The agent's first real attempt (msg 14) was a reasonable, near-canonical Trilogy formulation:

```trilogy
import raw.store_sales as ss;
auto null_addr_avg_profit <- avg(ss.net_profit ? ss.sale_address.sk is null) by ss.store.sk;
auto threshold <- 0.9 * null_addr_avg_profit;
auto item_avg_profit <- avg(ss.net_profit) by ss.item.sk;
where ss.store.sk = 1
select ss.item.product_name, item_avg_profit
having item_avg_profit > threshold
order by item_avg_profit asc limit 10;
```

It ran cleanly and returned **0 rows** (msg 17). The correct answer is 10 rows. The agent
trusted the "empty can be correct" guidance, then spent ~25 more turns probing the data
(store 1 exists, 39949 sales, 9395 null-addr sales, threshold = -738.04, many items qualify),
proving the empty result was WRONG, and finally rewrote into a multi-rowset `subset join`
shape (workspace `query44.preql`, 10 rows, PASS). The entire spiral was driven by the
framework silently emitting an empty result for a correct query.

The two log error signatures ("Resolution error"/"Discovery error", msg 43; Syntax [104], msg 13)
are downstream flailing after the silent-empty misdirection — not the root cause.

## Minimal repro (harness: 20260708-135136_enriched/workspace)

Query A above → `ROWS=0`. Correct = 10.

## Root cause

Generated SQL for A builds **two** materializations of `item_avg_profit` with inconsistent filtering:

- `abundant` = `avg(SS_NET_PROFIT) ... GROUP BY SS_ITEM_SK` — **no** `WHERE SS_STORE_SK=1` (all stores)
- `vacuous`  = `avg(SS_NET_PROFIT) WHERE SS_STORE_SK=1 GROUP BY SS_ITEM_SK` — store-1 only

The final SELECT outputs `vacuous` (filtered, correct). The HAVING is reconstructed as a
membership semijoin over a virt_filter CTE built from `uneven`, which sources
`abundant.item_avg_profit` (UNFILTERED) and applies the store filter only as a post-aggregate
row condition:

```sql
yummy: CASE WHEN uneven.item_avg_profit > uneven.threshold AND uneven.ss_store_sk = 1 THEN ... END
final WHERE (vacuous.item_avg_profit, product_name) IN (select ... from juicy where ... not null)
```

Because `abundant` averages over all stores, its per-item value differs from `vacuous`'s
store-1 value for the same item (proven: e.g. item avg all-stores -644.20 vs store-1 -973.68).
The membership tuple `(store1_avg, name) IN (allstores_avg, name)` can therefore **never match**
→ empty result.

The defect: the query-level WHERE `ss.store.sk = 1` filters an aggregate whose grain
(`by ss.item.sk`) does NOT contain the filter dim `store.sk`. On the OUTPUT path the filter is
correctly pushed into the aggregate input (pre-aggregate). On the HAVING path — triggered by
comparing `item_avg_profit` against a **named** store-grained sibling concept (`threshold`) —
the filter is instead deferred to a post-aggregate row condition (`AND ss_store_sk=1`) over an
`item x store`-grouped CTE, leaving `item_avg_profit` itself unfiltered (all stores). The two
divergent group nodes (different `preexisting_conditions`) are then stitched by a value-equality
membership semijoin that assumes they agree.

## Trigger matrix (item_avg_profit grain vs how threshold is referenced)

| case | item_avg grain | HAVING RHS | result |
|------|----------------|-----------|--------|
| A | `by ss.item.sk` | named `threshold <- 0.9 * null_addr_avg_profit` | **0 (WRONG)** |
| B | `by ss.item.sk` | inline `0.9 * null_addr_avg_profit` | 10 (correct) |
| C | `by ss.item.sk, ss.store.sk` | inline | 10 (correct) |
| D | `by ss.item.sk` | constant `-738.0` | 10 (correct) |
| E | `by ss.item.sk`, NO store WHERE | named `threshold` | 10 (self-consistent, unfiltered both sides) |
| F | `by ss.item.sk, ss.store.sk` | named `threshold` | 10 (correct) |
| G | `by ss.item.sk` | named alias `threshold <- null_addr_avg_profit` (no arithmetic) | **0 (WRONG)** |
| H | `by ss.item.sk` | named `threshold`, item_avg only in HAVING | **0 (WRONG)** |

Trigger = ALL of: (1) query WHERE on a dim (`store.sk`) NOT in the aggregate's grain, AND
(2) HAVING compares that aggregate to a **named** concept derived from a `store.sk`-grained
aggregate. Inlining the RHS (B), adding `store.sk` to the aggregate grain (C/F), or a constant
RHS (D) all avoid it. The naming (G shows even a bare alias, not the arithmetic) is what routes
the HAVING into the separate unfiltered-aggregate subgraph.

Note: this is a *different* defect from the prior fixed q44 case
(`project_q44_basic_scalar_granularity_having_misroute_fixed`). That granularity fix
(common.py:1041-1050, BASIC all-SINGLE_ROW args → SINGLE_ROW) is present and does NOT apply
here — `null_addr_avg_profit` is grained `by ss.store.sk` so `threshold` is correctly
MULTI_ROW. This is a WHERE-pushdown / condition-disposition divergence, not a granularity mis-tag.

## Suspected code area (not yet pinned to one line; read-only, not traced to the setter)

The divergence is between the output group node (gets `WHERE ss.store.sk=1` pushed into the
`item_avg_profit` aggregate parent) and the HAVING/virt-filter group node (does not; applies the
condition post-aggregate). Relevant machinery:
`trilogy/core/processing/node_generators/group_node.py` (`get_group_parent_inputs`,
`_preexisting_conditions_from_parents`) and the filter/where routing in
`trilogy/core/processing/node_generators/filter_node.py` (`build_parent_concepts`,
`pushdown_filter_to_parent`) — the HAVING-side group node for `item_avg_profit` is built without
the query condition as a `preexisting_conditions`/pushed WHERE, so it materializes the
all-stores aggregate (`abundant`) that the reconstructing membership semijoin then compares
against the filtered output aggregate (`vacuous`).

## Verdict

Real framework bug: a correct query silently returns 0 rows. Highest-severity class
(silent wrong result), and it directly caused the ~980k-token spiral. DO NOT FIX (per task).
Canonical `tests/modeling/tpc_ds_duckdb/query44.{sql,preql}` and the workspace final rewrite
both return the correct 10 rows.
