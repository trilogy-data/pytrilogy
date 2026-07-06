# q83 pass→TIMEOUT — redundant `*_sales` fact-join makes all_sales returns queries ~4.6 min (runaway, not a planner loop)

Runs: OLD `results/20260706-135542_enriched` (q83 **PASS**, 833k tok) →
NEW `results/20260706-222300` (q83 **TIMEOUT** 900s agent / scoring 180s, 865k tok).
Builds on `bug_q83_churn_030015.md` (different failure that run: silent WHERE-agg no-op).

## Symptom — SLOW query, NOT a planner infinite loop

The new-run signature "likely planner loop or runaway query" is a **runaway query**, not a
planner loop. Evidence from `agent_log.q83.jsonl` timestamps:

| `trilogy run query83.preql` start | result | wall |
|---|---|---|
| 22:39:30 | 22:43:47 exit 0, correct rows | **4m17s** |
| 22:44:06 | 22:48:12 exit 0, correct rows | **4m06s** |
| 22:48:28 | (never — agent hit 900s wall) | — |

Each execution **completes and returns the correct 24 rows** (`AAAAAAAAAHFBAAAA` = 75/69/1,
matches canonical). It is just ~4.6 min per run, so 2–3 iterate-and-run cycles exhaust the
900 s budget. `generate_sql` itself is **fast (<1 s)** — the planner does not loop.

Direct repro of the shipped `query83.preql` SQL against `workspace/tpcds.duckdb`:
**`ROWS 24 in 277.2 s`** (~4.6 min).

The discovery error `cannot merge all concepts…3 disconnected subgraphs` in the log is from
an *earlier* draft (separate `sr/cr/wr` imports with no join) — a correct disconnect, not the
timeout. The timeout is the final all_sales + `union join` form.

## Root cause — redundant RIGHT OUTER JOIN onto the huge `*_sales` fact table

The agent's final query imports `raw.all_sales` (the unified sales+returns model) and, per
channel, does `where s.channel='STORE' … select s.item.text_id, sum(s.return_quantity), count(s.item.id)`.
Generated SQL for **one** channel (`scratchpad q83.sql`, CTE `yummy`/`wakeful`):

```sql
FROM   "store_sales"   as ...            -- 2,880,404 rows, anchor
  RIGHT OUTER JOIN "wakeful"(store_returns filtered)
       on 'STORE'=s_channel AND ss_item_sk=s_item_id AND ss_ticket_number=s_order_id
  INNER JOIN "item"  on wakeful.s_item_id = item.I_ITEM_SK   -- item joined via RETURNS fk
  INNER JOIN date_filter ...
GROUP BY item_code
```

`store_sales` contributes **no output column**: the item join uses `wakeful.s_item_id`
(= `SR_ITEM_SK`, already on the returns row) and the measure is a return measure. The planner
sources the shared grain key `s.item.id` from the *sales* datasource and RIGHT-joins the
returns onto it, instead of sourcing `item.id` from the **returns** datasource it already
selected for `return_quantity`. q83 does this **3×** (store 2.88M + catalog 1.44M + web 719k
fact scans) → ~4.6 min. It is also a latent fan-out hazard (a return matching multiple sale
lines would double-count return_quantity; harmless on this data).

Grain of the unified model is `<order_id, channel, item.id>`; `item.id` is a member key
present on *both* the `*_sales` and `*_returns` datasources, so datasource selection is free to
(and does) pick the sales side. Locus: source/datasource selection in
`trilogy/core/processing/concept_strategies_v4.py` (select-node source resolution) + the
join assembly that RIGHT-joins the returns onto the sales anchor. No exact scoring line is
pinned here (behavioral root cause is the datasource choice for the shared key).

## Trigger matrix (SQL gen; `*_sales` tables referenced)

| variant over `import raw.all_sales as s` | `*_sales` joined? |
|---|---|
| `where s.channel='STORE' select s.channel, sum(s.return_quantity)` (no item) | **no** |
| `where s.channel='STORE' select s.item.text_id, count(s.item.id)` (no return measure) | **no** |
| `where s.channel='STORE' select s.item.text_id, sum(s.return_quantity)` | **YES (store_sales)** |
| `select s.item.text_id, sum(s.return_quantity ? s.channel='STORE')` (filtered-agg, canonical) | **no** |
| canonical `query83.preql` (all_sales, filtered aggs, `having`, one select) | **no** |
| OLD form `import raw.store_returns`, `select item.text_id, sum(return_quantity)` | **no** |

Minimal trigger = **item dimension output + a return measure + a row-level `where s.channel=` filter**,
all three, over `all_sales`. Remove any one and the sales join disappears. Canonical dodges it
because the channel predicate lives *inside* the aggregate (`? channel=`), so `item.id` sources
from returns.

## Regression attribution

**Not a code regression from `4e69c5547`.** That commit's touched hunks are all `SetOperator`
plumbing for the `union(...)/except(...)/intersect(...)` TVF (`union_node.py`,
`union_select_node.py`, `union_dim_pushdown.py`, `predicate_pushdown.py`, `base_node.py`,
`concept_strategies_v4._resolve_union_select`, `statements/author.py`) plus the ORDER-BY
(`_substitute_order_by_outputs`) and tuple-membership (`expr_tuple`) changes. **None touch
datasource cost-selection for `all_sales`.** The redundant sales-join reproduces trivially on
the current engine for a two-line query, so it is **pre-existing**.

What flipped pass→timeout is a **query-strategy change by the agent**: OLD run used three
separate `rowset`s over `raw.store_returns / catalog_returns / web_returns` (generates a clean
returns-only plan, no sales join — verified fast). NEW run used `raw.all_sales` + per-channel
`with … as` scoped selects + `union join`, which triggers the pre-existing perf bug 3×. The
`all_sales.preql` model doc itself steers this ("Use as default for multi-channel analysis
(WEB/CATALOG/STORE)"), and the commit added a set-ops syntax example (`except/intersect`) the
agent browsed; but the decisive nudge is the model doc, not a `4e69c5547` code hunk.

## Classification

Real **framework performance bug** (pre-existing), surfaced as a regression via agent strategy
choice. **Not agent error** — the shipped query is *correct* (24 rows, matches canonical), just
pathologically slow. Guidance/model-doc trap: the model recommended for multi-channel produces
a redundant `*_sales` scan for returns-only, per-channel-filtered queries.

## Suggested follow-ups (not done — READ-ONLY task)

- Datasource selection: when a shared grain key (`item.id`) is needed alongside a measure
  exclusive to datasource X (returns), prefer sourcing the key from X over pulling in a second
  fact datasource (sales) that adds no columns — avoids the redundant RIGHT JOIN.
- Guidance: for **returns-only** multi-channel questions, steer toward the filtered-aggregate
  form (`sum(x ? channel=…)`) or the per-channel returns datasources, not row-level
  `where channel=` over `all_sales`.
