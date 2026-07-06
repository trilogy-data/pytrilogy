# Deep dive: TPC-DS "Discovery error / Could not resolve connections" token sinks

Run: `evals/tpcds_agent/results/20260706-135542_{ingest,enriched}`
Trilogy 0.3.290. All probes via the Python API (`Environment.parse` + `DUCK_DB.generate_sql`) against the run's own `workspace/raw/` model. Probe files written into the workspaces were deleted after.

## Headline verdict

**q75 is NOT a framework join bug.** None of the three targets is an (A) framework false-disconnect. Every disconnect error reproduced deterministically, and in every case it was the CORRECT authored error against a *malformed / awkward intermediate* form the agent tried. Each query's FINAL workspace version RESOLVES and EXECUTES on today's engine; all three ultimately failed with `result set differs from reference` (values wrong, row-counts right: q75 100/100, q35 100/100, q64 2/2) — a correctness issue, not a resolution failure. The "Discovery error" lines are the agent thrashing against real errors and burning tokens (q75 = 2.89M) before landing a resolving-but-semantically-wrong query.

Proof that the disconnects are correct, not spurious: the netting join and the union arms all resolve when spelled correctly.

---

## q75 (INGEST, 2.89M tok, fail) — classification: **B (ingest under-specification) + C (malformed intermediate forms). NOT A.**

Business: 3-channel (store/catalog/web) year-over-year item sales-count, returns netted out (sales_qty − return_qty), 2002 vs 2001.

Errors seen across iterations:
- `not joinable from model: {st_net.bid, st_net.catid, st_net.cid, st_net.isk, st_net.mid, st_net.ok, st_net.yr}`
- `2 disconnected subgraphs: {cs_lines.bid, ...yr}; {_virt_agg_sum_...}`

Root of the difficulty is **(B)**: the ingest `raw/` model has **no `all_sales` combined datasource**. The curated model DOES (`raw/all_sales.preql`, grain `(order_id, channel, item.id)`, carrying both sales AND return measures on one line grain), and the curated `query75.preql` is built entirely on `import all_sales as sales` — it never relates `store_sales`↔`store_returns` directly. In both models `store_sales.ticket_number` and `store_returns.ticket_number` are **separate local `key`s** (each declared `key ticket_number` in its own namespace) sharing only `item.item_sk`; there is no declared merge. The curated model sidesteps this via `all_sales`; the ingest agent had to hand-roll the union + per-channel sales↔returns netting.

The disconnect errors themselves are **(C)** — correct errors on a broken form. The `st_net` form defined the line-grain dims as a rowset but computed the netted measure as a **bare, grainless aggregate** disconnected from those dims:
```
rowset st_net <- ... select st.date_dim.year as yr, st.item.brand_id as bid, st.ticket_number as ok, st.item.item_sk as isk ;
auto st_nqty <- sum(st.quantity) - sum(coalesce(sr.return_quantity, 0));   -- grainless!
select st_net.yr, st_net.bid, st_net.ok, st_net.isk, st_nqty ;
```
Minimal repro reproduces the exact error:
`2 disconnected subgraphs: {_virt_agg_sum_...}; {st_net.bid, st_net.isk, st_net.ok, st_net.yr}`.
This is correct: a global scalar `sum(...)−sum(...)` has no grain relating it to per-line rowset outputs.

Toggle that fixes it — the **per-line** netting the agent eventually used (its final workspace version): each union arm does
`st.quantity - coalesce(sr.return_quantity,0)` with `union join st.ticket_number = sr.ticket_number and st.item.item_sk = sr.item.item_sk` INSIDE the arm. The minimal netting join **resolves cleanly** to `FULL JOIN store_returns ON item_sk AND ticket_number`, and the full final `query75.preql` **resolves** (1 statement). So the framework can express the netting; the agent's early bare-aggregate form genuinely could not.

Note (correctness, out of disconnect scope): the agent's `union join` renders a FULL JOIN, whereas the reference nets via a LEFT join (sales left join returns). FULL admits return-only rows with no matching sale — a likely source of the `values differ` failure. Separate from the disconnect question.

---

## q64 (ENRICHED, 1.16M tok, fail) — classification: **C (agent omitted the join in that iteration). NOT A.**

Error at line 3 (the `cat_metrics` CTE):
`2 disconnected subgraphs: {cs.item.id, cat_list_price}; {cat_refund}`

The errored form imported `cs = catalog_sales` and `cr = catalog_returns` as **independent aliases** and summed `cs.ext_list_price` and `cr.(refunded_cash+reversed_charge+store_credit)` **with no join bridging them** (they share only the `item.id` dimension — insufficient). Minimal repro reproduces the EXACT error `{cs.item.id, cat_list_price}; {cat_refund}`. This is a correct disconnect.

Two toggles both resolve:
1. Add the bridge (what the agent's final version did): `union join cs.item.id = cr.item.id` + `union join cs.order_number = cr.order_number` → resolves to `FULL JOIN catalog_returns ON item_sk AND order_number`.
2. The curated idiom (curated `query64.preql`, lines 6-11, explicitly comments *"catalog_sales is reached through cr.sales ... so we avoid the cross-import merge"*): reach catalog_sales via `cr.sales.*` (catalog_returns already imports catalog_sales as `sales`, and `CR_ORDER_NUMBER: sales.order_number` wires the FK). Also resolves.

The agent's final `query64.preql` **resolves and executes** (2/2 rows); it failed on values, not resolution.

---

## q35 (ENRICHED, 595k tok, fail) — classification: **C (awkward multi-rowset structure). NOT A.**

Error at line 35:
`{... demographic dims + count aggregates ...}; {customer_count, customer_count2, customer_count3, store_custs.cust_id}`

The errored form built THREE rowsets (`store_custs`, `web_custs`, `catalog_custs`), combined web+catalog into a 4th (`web_or_catalog` via `union join`), imported `customer` separately as `cust`, then double-`subset join`ed `store_custs`↔`web_or_catalog` and `store_custs`↔`cust`, and did `count(store_custs.cust_id)` grouped by `cust.demographics.*`. The planner split the rowset-anchored count from the separately-imported `cust` dims.

Every clean idiom resolves:
- Minimal `count(store_custs.cust_id)` grouped by `cust.demographics.gender` over a single `subset join store_custs.cust_id = cust.id` → **resolves**.
- The canonical anchor-on-`ss` idiom (`ss.customer.demographics.gender`, `count(ss.customer.id)`) → **resolves**.
- The agent's OWN final `query35.preql` (anchored on `ss`, membership via `ss.customer.id in web_custs.cust_id or ... catalog_custs.cust_id`) → **resolves and executes** (100/100 rows).

So the disconnect appeared only under the agent's contorted 3-rowset + separate-`cust`-import structure, not for any genuinely-connected pair. Not a false-disconnect. Failed on values.

---

## Bottom line for the framework

No (A) false-disconnect found; **no `trilogy/` fix warranted** for these. The disconnect resolver is behaving correctly — it flags genuinely-ungrouped bare aggregates (q75), missing cross-import joins (q64), and unrelated rowset-anchored aggregates (q35). The token sink is the *agent* re-trying malformed forms, aggravated by (B) the ingest model's missing `all_sales` for q75. Possible non-bug follow-ups (guidance/DX, not correctness): (1) the disconnect error message could hint "an aggregate with no `by`/grain cannot sit beside row-grain outputs" to shorten the q75 thrash; (2) the real q75/q64/q35 failures are `values differ` (e.g. `union join` = FULL vs reference LEFT-join netting) — a distinct correctness track worth its own investigation.
