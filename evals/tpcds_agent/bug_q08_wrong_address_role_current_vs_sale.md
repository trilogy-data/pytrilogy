# q08 INGEST blowup — wrong-result from `customer_address` (sale addr) vs `customer.customer_address` (current addr) role

Run: `evals/tpcds_agent/results/20260720-140600` — q08, 998,728 tokens (baseline ~100k, ~10x), FAILS
"result set differs from reference". INGEST leg (agent uses auto-ingested `workspace/raw/*.preql`).

## Symptom
Agent's final `workspace/query08.preql` runs cleanly and returns **6 stores**
(able, ation, bar, **eing**, ese, ought). Correct answer is **5 stores**
(able, ation, bar, ese, ought). The extra row is **`eing`** (−10,847,375.58); every
shared store's value is identical. One-row over-inclusion, no error, no warning — a
SILENT wrong result the agent had no signal to catch.

## How the rows differ (mechanism)
- Store zips are only `31904` (prefix 31) and `35709` (prefix 35). `eing` exists at
  BOTH zips; `anti` only at 35709 (no Q2-1998 sales). So the whole result hinges on
  **whether prefix `35` qualifies**, which hinges on one zip: **`35258`**.
- Question set (b): "5-digit zips where >10 preferred customers have their **current
  address** record." Reference truth for `35258` (customer JOIN customer_address on
  `c_current_addr_sk`): **9 preferred customers → NOT dense (≤10) → prefix 35 does
  NOT qualify → `eing` excluded.**
- The agent instead counted preferred customers by the **sale's** address
  (`ss_addr_sk`) and only among customers who had a store sale: `35258` → **15 →
  dense → prefix 35 qualifies → `eing` wrongly included.**

Root of the divergence is a **role mis-selection**. The auto-ingested `store_sales`
binds two distinct, non-interchangeable address roles:
- `store_sales.customer_address.*` — DIRECT, from `ss_addr_sk` (the address on the
  sale). *This is what the agent used.*
- `store_sales.customer.customer_address.*` — VIA customer, from `c_current_addr_sk`
  (the customer's current address). *This is what the question asks for.*

## Minimal repro / trigger matrix (verified against the run's DB copy)
Count of preferred customers for zip `35258`, three ways:

| Form | Path | Count | Dense(>10)? |
|---|---|---|---|
| Reference (customer table) | `count(customer.customer_sk ? pref='Y') by customer.customer_address.zip` | **9** | no |
| Via-customer through fact | `... by store_sales.customer.customer_address.zip` | **9** | no |
| Agent (direct sale-addr) | `... by store_sales.customer_address.zip` | **15** | **yes** |

Raw-SQL cross-check: current-address count = 9, sale-address distinct-preferred = 15.
**Both Trilogy numbers match raw SQL exactly — the engine computes each semantic
correctly.**

Whole-query trigger: taking the agent's EXACT final query and changing ONLY
`store_sales.customer_address.zip` → `store_sales.customer.customer_address.zip`
(both occurrences) flips the result **6 → 5 rows (correct)**. Nothing else changes.

## Classification: AGENT semantic error + FRAMEWORK/affordance defects (NOT a silent engine bug)
The engine is correct: the via-customer role, the `substring(zip,1,2) in substring(qzip,1,2)`
derived-key membership, the `unnest(split(...))` param set, and the "nested dimension =
all values" promise all work (the via-fact count is 9, i.e. all customers, NOT restricted
to sale-havers). No grain fan-out (`count(<key>)` is distinct; 15 is a legitimately
different semantic, not inflation).

What the framework contributed to the 10x blowup and the mis-selection:

1. **Ambiguous `Undefined concept` error** — `trilogy/parsing/v2/semantic_state.py:695`
   (also `parsing/v2/select_finalize.py:141`). The decisive turns (conv. msgs 15–24):
   the agent drafted the rowset with **no `import`** and referenced
   `customer.customer_address.zip` → `Undefined concept: customer.customer_address.zip`.
   The message is emitted identically for "concept does not exist" and "no model
   exposing it has been imported yet," with only fuzzy name suggestions and **no
   "did you forget to import?" hint**. The agent misread it as "the via-customer path
   is invalid," added `import raw.store_sales`, and settled on the DIRECT
   `store_sales.customer_address.zip` — **never retrying `store_sales.customer.customer_address.zip`,
   which would have worked.**

2. **`allow_file_read=false`** (`workspace/trilogy.toml`) — when the agent tried to
   re-read its own `answer_*.preql` to audit the logic (msgs 50–51), it was **denied**
   and redirected to `explore` (which prints model concepts, not the query body). This
   blocked self-audit of the address role and drove repeated re-derivation of the param
   intersection (most of the token burn is msgs 28–53 re-verifying `unnest`/`in`/the
   intersection, not the actual bug).

3. **Role-collapsed explore output** — `explore` prints the two roles under one combined
   key `"customer_address, customer.customer_address"` with the DIRECT role listed first.
   The short/first path (`customer_address.zip`) reads as the obvious choice; the
   non-interchangeability note is prose the agent did not act on for this dimension.

## Root cause (file:line)
- Wrong result: agent selected `store_sales.customer_address` (direct `ss_addr_sk`)
  instead of `store_sales.customer.customer_address` (via `c_current_addr_sk`). Model
  bindings are correct: `workspace/raw/store_sales.preql:37` (`ss_addr_sk: ~?customer_address.address_sk`)
  vs `workspace/raw/customer.preql:33` (`c_current_addr_sk: ~customer_address.address_sk`).
- Primary framework lever that steered the error: `Undefined concept` message lacks an
  "unimported model" hint — `trilogy/parsing/v2/semantic_state.py:692-698`
  (`_raise_undefined`) and `trilogy/parsing/v2/select_finalize.py:141`.

## Suggested direction (do NOT fix here)
When an `Undefined concept: a.b.c` has a dotted prefix that matches no imported model,
append a hint (e.g. "no imported model exposes namespace `customer`; add
`import raw.<model> as customer;`"). Consider allowing `file read` on the agent's own
answer file so it can self-audit. Consider surfacing role provenance (direct vs via)
inline in explore for address-like dimensions.
