# q66 token sink — NO framework bug (question/reference-quirk defect + schema-discovery churn)

Run: `evals/tpcds_agent/results/20260711-042547_enriched` (id 66, key 65).
Tokens: 1,638,489 · iterations 29 · tool_calls 41 (20 `explore`, 13 `run`) · 4 tool_errors.
Verdict: **agent value error driven by a question/model-documentation conflict** (TPC-DS
spec quirk). The Trilogy engine mis-resolves and mis-codegens **nothing** here — both the
canonical `all_sales` answer and the agent's own raw-union approach produce the reference
result exactly on the current engine.

## Symptom
Agent's `query66.preql` runs cleanly, returns the correct 5 rows with correct grouping keys,
but every metric is ~20x too small → scorer `result set differs from reference`.

Row keys match perfectly; only values differ:
```
              jan_sales (agent)   jan_sales (ref)
None          521,523.79          10,548,090.79    (ref/agent ≈ 20.2x)
```

## Root cause: agent dropped `* quantity`
Question says: "monthly sales amount is its **quantity times the extended sales price** for web
sales and its **quantity times the per-unit sales price** for catalog sales."

The reference SQL (`tests/modeling/tpc_ds_duckdb/query66.sql`) implements this as the TPC-DS
spec quirk — **asymmetric** and, for web, deliberately "double-counted":
- web:     `ws_ext_sales_price * ws_quantity`  ,  `ws_net_paid * ws_quantity`
- catalog: `cs_sales_price     * cs_quantity`  ,  `cs_net_paid_inc_tax * cs_quantity`

Because `web_sales.preql` documents `ext_sales_price = sales_price * quantity`, the agent
reasoned (conversation lines ~2598-2622) that `quantity * ext_sales_price` = `quantity² *
sales_price` "can't be right," and concluded web/catalog sale amount should just be
`ext_sales_price`. It therefore:
- used `ws.ext_sales_price` / `cs.ext_sales_price` with **no `* quantity`**, and
- used `cs.ext_sales_price` for catalog instead of `cs.sales_price` (per-unit).

That single omission fully accounts for the divergence.

## Trigger matrix (one ingredient toggled at a time)
| Variant | Result |
|---|---|
| Agent query as-submitted (no `*quantity`, catalog `ext_sales_price`) | rows OK, values ~20x low → FAIL |
| Agent's **exact union structure** + web `ext_sales_price*quantity` & `net_paid*quantity`, catalog `sales_price*quantity` & `net_paid_inc_tax*quantity` | **EXACT match** (jan_sales 10,548,090.79; jan_net 33,568,345.58) → PASS |
| Canonical `.preql` (`all_sales` channel model), import fixed to `raw.all_sales` | **EXACT match** → PASS |

The union rowset, filtered aggregate `sum(x ? mo = k)`, per-sqft division, `nullif`, and
`coalesce(...,0)` all codegen correctly. Flipping only the `* quantity` ingredient turns
fail into pass — nothing in the framework is load-bearing for the wrong result.

## The 4 tool errors were all clean agent errors (no framework signature)
1. `ws.net_paid_inc_tax` Undefined concept — **correct**: web_sales has no net_paid_inc_tax
   (catalog does). Good suggestions returned (`ws.net_paid`, `ws.return_amount_inc_tax`).
2. `coalsece(1/0,0)` — agent typo; clean pest parse error with caret pointer.
   (Remaining 2 counted errors are non-fatal: the `agent-info` guide payload and a benign run.)

## What actually burned the tokens (turn count, per prior)
- 20 `explore` calls churning on schema discovery: hunting `net_paid_inc_tax` per-channel,
  and repeatedly regex-probing for the `time` concept / `_time_sk` roles across
  catalog_sales & web_sales. Concept-per-channel asymmetry (catalog has net_paid_inc_tax,
  web has net_paid) drove several dead-end explores.
- Long deliberation over the quantity semantics above.
The agent also never explored `raw/all_sales.preql` (the pre-unified channel model the
canonical answer uses), so it hand-rolled the 2-arm `union(...)` from raw web+catalog.

## Classification
**Guidance / question defect + agent modeling error. NOT a framework bug.**
- The reference encodes a genuine TPC-DS spec oddity (web multiplies an already-extended
  price by quantity again → qty²·price; catalog multiplies per-unit price by quantity).
  A careful reader of the model's `ext_sales_price` docstring will read the web clause as
  double-counting and "fix" it away — exactly what happened.
- Optional question hardening (do NOT change engine): make the web clause explicit that it is
  `quantity * ext_sales_price` even though ext_sales_price is already extended (i.e. state the
  spec quirk), or point the agent at the `all_sales` channel model. This is a wording/scaffold
  choice, not a code fix.

## Files
- Agent query: `evals/tpcds_agent/results/20260711-042547_enriched/workspace/query66.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query66.sql` (scorer ref), `.preql` (canonical, builds+matches)
- Repro harness (scratchpad): `corrected66.preql`/`diff66b.py`, `canon66b.py`
