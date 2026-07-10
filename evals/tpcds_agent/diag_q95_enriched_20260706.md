# Diagnosis: q95 FAIL in enriched leg (run 20260706-135542_enriched)

## Classification: AGENT error (scope-of-exists) — with counterfactual proof. NOT framework, NOT model, NOT question.

## Symptom
1 ref row vs 1 candidate row, all three values differ:

| | order count | total ship cost | total net profit |
|---|---|---|---|
| reference (query95.sql) | 68 | 100592.32 | -18202.90 |
| candidate (actual, engine-executed) | 6 | 23051.02 | -879.38 |

No framework errors anywhere in `agent_log.q95.conversation.txt` — the query ran cleanly first try (message 12→14); silent wrong values only.

## Root cause
The question's two exists-conditions are **order-wide** ("the order has at least one other
line shipping from a different warehouse", "at least one web return exists for the order"),
and the reference computes both over the **entire** `web_sales`/`web_returns` tables
(the `ws_wh` self-join CTE and the `wr_order_number IN` subquery carry no date/state/site
filters). The agent instead computed both **inside the pre-filtered rowset**
(`workspace\query95.preql` puts the date/IL/'pri' WHERE on the `order_agg` rowset, then
takes `count_distinct(ws.warehouse.id)` and `sum(ws.is_returned::int)` over only those
filtered lines). TPC-DS web orders ship each line separately (per-line ship date /
ship address), so most orders' "other warehouse" and "returned" evidence lives on lines
outside the 1999-02-01..1999-04-02 / IL / 'pri' window.

Transcript proof (message 12): the agent explicitly wrote *"at the ORDER level, not the
line level"* and *"'at least one web return exists for the order' — at the ORDER level,
at least one line has a return"* — it caught the grain issue (order vs line) but conflated
it with scope (whole order vs filtered subset) and never lifted the WHERE out of the rowset.

## Per-condition decomposition (raw SQL on DB copy; base filtered orders = 100)
| condition variant | qualifying orders | count / ship / profit |
|---|---|---|
| both in-filter (candidate semantics, emulated) | mw=27 ∩ ret=10 → 6 | 6 / 23051.02 / -879.38 — **matches candidate exactly** |
| mw order-wide, returns in-filter | 10 | 10 / 24414.87 / -1625.27 |
| mw in-filter, returns order-wide | 21 | 21 / 57371.98 / -14362.84 |
| both order-wide (reference semantics) | 68 | 68 / 100592.32 / -18202.90 — **matches reference exactly** |

Condition counts among the 100 base orders: mw order-wide = 100 (at sf=1 every base order
is multi-warehouse order-wide, so at this scale the reference's 68 is set purely by the
order-wide returns-exists); mw in-filter = 27; returns order-wide = 68; returned-line-in-filter = 10.

## Framework exoneration
- Candidate `.preql` → generated SQL (3847 chars) → (6, 23051.02, -879.38), byte-equal to a
  hand-written SQL emulation of the agent's stated semantics (filtered-scope count_distinct
  warehouse + filtered-line is_returned). The engine compiled the agent's Trilogy faithfully;
  the CTE plan (cooperative/abundant/yummy/vacuous) is correct for what was written.
- Canonical `tests\modeling\tpc_ds_duckdb\query95.preql` compiled and executed on the same
  DB copy → (68, 100592.32, -18202.90) = exact match to `query95.sql`. Canonical path healthy;
  the UpgradeJoinOnGuards q94 semijoin fix (2026-07-05) did not regress q95.

## Known-pattern check (9 sibling probes)
1. QUESTION/implicit-filter nullable FK — no: both sides inner-join the same three dims.
2. AGENT/identifier — no: `ws.warehouse.id` = `WS_WAREHOUSE_SK`, correct.
3. AGENT/measure-choice — no: `ext_ship_cost`→`WS_EXT_SHIP_COST`, `net_profit`→`WS_NET_PROFIT`, both correct.
4. AGENT/output-shape — no: 1 row x 3 cols both sides.
5. Multi-warehouse self-join exists — expressed as `count_distinct(warehouse.id) > 1` per order;
   compiled faithfully; the defect is its *scope* (filtered rowset), not its compilation.

Note: the model's line-level `is_returned` (matched on order+item) is order-wide-equivalent to
the reference's `wr_order_number IN` when evaluated over all lines (every web_return maps to a
sold line); the returns divergence (68→10) also comes purely from the filtered-line scope.

## Correct Trilogy shape (for eval-guidance purposes; not applied)
Compute `order_agg` (distinct_warehouses, return presence) over UNFILTERED `ws`, apply the
date/IL/'pri' filters only on the outer consuming select — as canonical query95.preql does
(unfiltered `multi_warehouse_order` / `returned_orders` rowsets + membership tests inside the
filtered aggregate select).

## Evidence files
- Candidate: `evals\tpcds_agent\results\20260706-135542_enriched\workspace\query95.preql`
- Transcript admission: `agent_log.q95.conversation.txt` message 12 (~line 1833)
- Reference: `tests\modeling\tpc_ds_duckdb\query95.sql` / `query95.preql`
- DB: copy of `evals\tpcds_agent\.cache\tpcds_sf1.duckdb` (deleted after diagnosis)
