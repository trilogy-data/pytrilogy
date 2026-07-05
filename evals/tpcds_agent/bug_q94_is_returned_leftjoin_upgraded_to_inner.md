# q94 — `is_returned` LEFT-join silently upgraded to INNER under a semijoin (SILENT wrong result)

**STATUS: FIXED 2026-07-05** — see the status header in
`handoff_q94_join_upgrade_leftjoin_to_inner_on_shared_key.md` for the actual root cause
(seed misdetection via an existence-only parent + `_blocked_partials` intersection-vs-subset),
which differs from the `_inner_pair_rejections` theory below.

**Run:** `evals/tpcds_agent/results/20260705-142435` — q94, 896,266 tokens, FAILED.
**Classification:** REAL FRAMEWORK BUG (silent). Optimizer unsoundly upgrades a
LEFT OUTER JOIN to INNER, making the model's documented "not returned" state
(`is_returned = false`) structurally unreachable at order grain.

## Symptom
q94 excludes orders that had any web return ("no line of that order has a web
return"). The model exposes this via `auto is_returned <- _returned_order_number
is not null`, where `_returned_order_number` is a NULL-padded column on a
separate partial (`~`) `web_returns` datasource inside the `web_sales` model —
documented as: *"Computed post-join from a real (NULL-padded) web_returns
column, so 'not returned' works as `= false` / `is not true` / `is null`."*

The agent (correctly) aggregated `is_returned` per order to find orders with no
return, but **every order came back `has_return = true`** and the query returned
`[]`. The agent then concluded (wrongly, but reasonably given the engine
output) *"every order with ≥2 warehouses also has a returned line"* and
submitted 0 rows as correct (msg 61). Ground truth = **32 orders**.

- Ground truth (`query94.sql` raw, and canonical `query94.preql`): `(32, 65144.28, -19548.52)`.
- Agent `query94.preql` (as submitted): `[]`.

## Root cause
The per-order aggregate CTE joins `web_sales`→`web_returns`. It should be LEFT
OUTER (padded non-returned lines carry `is_returned = false`). Instead the
generated SQL emits **`INNER JOIN "web_returns"`**, dropping every non-returned
line. Consequences at order grain: `bool_or(is_returned)` can only ever be
`true`, `count(line_item)` counts only returned lines, and orders with zero
returns vanish entirely — the exact rows q94 wants.

The INNER comes from the optimizer rule **`UpgradeJoinOnGuards`** (config
`Optimizations.upgrade_condition_joins`, `trilogy/core/optimizations/join_upgrade.py`,
class at line 834; cross-CTE proof gathering in `_external_forced_map`
lines 735–831, gate in `_external_proofs` lines 851–883).

The rule upgrades LEFT→INNER when a downstream consumer "proves non-null" a
producer output. Here the **semijoin membership** `ws.order_number in
base_orders.ws.order_number` renders as an INNER equality on `order_number`
(`_inner_pair_rejections`, line 787), which the rule treats as a non-null proof
on the producer's `order_number`, licensing the upgrade of the *web_returns*
join. This is **unsound**: `order_number` is the shared key and is non-null on
the padded (non-returned) rows too — it renders as `coalesce(wr.WR_ORDER_NUMBER,
ws.WS_ORDER_NUMBER)`, non-null from the left side. Rejecting null-`order_number`
does NOT reject the padded rows, but the INNER upgrade removes them anyway. The
COALESCE-mask guard at line 883 (`len(set(cte.source_map.get(a))) == 1`) fails to
exclude `order_number` because it is registered single-source (a real grain key
mapped to the web_sales datasource) even though it *renders* as a COALESCE.

## Minimal repro / trigger matrix
Harness: `scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')`, workspace
of the run above; `is_returned` aggregated at order grain.

| # | body shape | web_returns join | `is_returned=false` reachable? |
|---|-----------|------------------|-------------------------------|
| P1 | plain `where ws.ship_address.state='IL'` + `bool_or(is_returned) by order` | **LEFT** | yes (546) ✅ |
| P2 | no filter + `bool_or(is_returned) by order` | **LEFT** | yes (17758) ✅ |
| P3 | `where ws.order_number in bo.ws.order_number` + `bool_or(is_returned) by order` | **INNER** | **no (0)** ❌ |
| P4 | plain where, then downstream `where oal.hr=false` | **LEFT** | yes ✅ |
| T4 | `count(line_item) by order`, no `is_returned` ref | none | (base = 100 orders) |

The differentiator is a **semijoin membership on the shared join key
`order_number`** (P3), not a filter on `is_returned` itself (P4 is fine).
Adding any web_sales measure (`sum(ext_ship_cost)`, `count_distinct(warehouse)`)
does NOT change it — the `is_returned` CTE stays INNER regardless.

**Direct confirmation** (agent's exact submitted `query94.preql`):
- default: `[]`
- with `CONFIG.optimizations.upgrade_condition_joins = False`: `(32, ...)` — order count now **matches ground truth (32)**.
  (The shipping/profit totals differ because the agent's `order_all_lines` sums
  over *all* lines of qualifying orders rather than the filtered lines — a
  separate modeling choice, not this bug.)

## Why it was a 896k-token sink
The engine silently returned `is_returned = true` for every line/order, so every
sanity check the agent ran "confirmed" a false data property. There was no error
to react to; the agent kept re-verifying an impossible result and finally
rationalized `[]` as correct. Classic silent-wrong-result confusion loop.

## Fix direction (NOT applied)
`UpgradeJoinOnGuards` must not treat a non-null proof on a **shared join key**
(one whose value survives on the null-padded side via the left source / COALESCE)
as license to upgrade the join that pads it. The line-883 single-source guard
should key off render-time COALESCE / multi-side provenance of the *join key on
the padded side*, not `source_map` cardinality. Alternatively, exclude a
producer output from the forced set when it is one of the outer join's ON-keys
(rejecting it cannot reject the padded rows). Re-check q64 (`is_returned` +
`C_CURRENT_ADDR_SK is not null`, referenced in the rule's own docstring) when
tightening.
