# Theme C — Business-language → concept mapping (the biggest gap: 12 of 23)

**One line:** when the model exposes several plausible concepts for one business word, the agent
picks the wrong one — a surrogate key instead of the business code, a customer's *current*
snapshot instead of the sale/line role, a line-extended measure instead of the per-unit one. Every
canonical `.preql` picks correctly; the engine is never at fault. These are **prompt-wording +
model-comment** fixes.

Four sub-patterns.

## Why raw SQL wins here (the abstraction actively hurts)

Comparing the `sql_bare` agent's column choices to the enriched agent's confirms the semantic
layer INVERTS or DUPLICATES the mapping the raw schema encoded correctly:

| case | sql_bare picked (✓) | raw name encodes | Trilogy concept | the trap |
|---|---|---|---|---|
| C1 customer/item code (q11,17) | `c_customer_id` / `i_item_id` | name = "…**id**" = business code | code = `.text_id`; surrogate = **`.id`** | Trilogy renamed `_id`→`text_id` and `_sk`→`id`, so `.id` is the SURROGATE — a false friend |
| C1 item join (q37) | `i_item_sk`/`inv_item_sk` | joins use `_sk` | join must use `.id` | agent joined on `.text_id` (non-unique SCD) |
| C2 hdemo role (q73,88) | `ss_hdemo_sk` | the only demo FK *on* the fact | `ss.household_demographic` AND `ss.customer.household_demographic` both 1 hop | wrong (customer-current) path is as reachable as the right (sale-time) one |
| C2 bill/ship (q74) | `ws_bill_customer_sk` | direct on the fact | `web.billing_customer` vs `web.ship_customer` | equally-prominent alternates |
| C-measure (q64,78) | `ss_sales_price` | "sales price" = the plain column | `sales_price` vs `ext_sales_price` (**same names**) | NOT a naming issue — prompt says "per-line" |

Takeaways: **C1 is a model-naming inversion** (fix at the model — rename or loudly comment
`.id`↔`.text_id`), **C2 is a model-structure duplication** (deprioritize / cross-reference the
customer-`current` role), **C-measure is genuinely prompt wording** (say "per-unit" not
"per-line"; the Trilogy and raw names match). The SQL agent literally cannot make the C1 mistake
because `c_customer_id` is a direct hit on the question's "customer id/code."

---

## C1 — Surrogate `.id` vs business `.text_id` (q11, q25, q37; + q17)

Business "code"/"id" means the stable business identifier the model exposes as `.text_id`
(TPC-DS `*_id`, a 16-char alnum like `AAAAAAAAAMGDAAAA`), NOT the internal surrogate `.id`
(`*_sk` integer). Also a JOIN hazard: `text_id` is non-unique (SCD type-2), so cross-table joins
must go through the surrogate `.id`.

- **Confusion quote (q11):** *"That's an int key from the store_sales aggregate, so it shouldn't
  be null. The ordering looks correct."* — the agent reasons about `customer_id` purely as an
  integer, never registering "customer code" = the business text id.
- **Join variant (q37):** *"the `text_id` … 'The typical per-item identifier'. So `item_code`
  would be `text_id`."* — then used `cs.item.text_id in filtered_inv.text_id` as the inventory
  join; because `text_id` maps to several `i_item_sk`, it stitched one sk's filter-pass to
  another sk's inventory-qualify → +1 spurious row (2 vs ref 1).
- **Error signature (q11/q25):** no exception; column-1 diff — agent integers `2533, 4561, …` /
  `9361` vs reference codes `AAAAAAAAAMGDAAAA` / `AAAAAAAABJECAAAA`, plus divergent sort order;
  all other columns match the same rows.
- **The agents read the warning and ignored it for the relevant use:** the model doc says
  `text_id` = "use this for per-item results, not the surrogate id" and warns "SEVERAL ids share
  one text_id … Not unique - this table is an SCD" — applied to output only, not joins.

**Fix:** (a) a global convention in the task preamble or model comments — *"a 'code'/'id'/'number'
the question asks you to REPORT = the business `text_id`; internal `.id` (`_sk`) is a surrogate,
never reported. CROSS-TABLE JOINS must use the surrogate `.id`, never `text_id` (non-unique
SCD)."* (b) optionally, per-prompt: name the identifier ("the 16-char business customer code").

---

## C2 — Role among same-typed dimensions (q46, q73, q74, q85, q88)

The model over-provides multiple same-typed roles; the agent defaults to the customer's *current*
snapshot or the wrong customer role instead of the sale/line-time attribute the question means.

- **Sale-time vs current household demographic** (q46, q73, q88): `ss.household_demographic`
  (`SS_HDEMO_SK`, sale-time) vs `ss.customer.household_demographic` (`C_CURRENT_HDEMO_SK`, the
  customer's current profile). Quote (q88): *"customer.household_demographic.dependent_count …
  (or just household_demographic.dependent_count)"* — saw both, chose the customer-routed one.
  q73: 9 rows vs ref 1, fully disjoint. (The same agent got q34 right with the direct role —
  inconsistent, not a model gap.)
- **Bill-to vs ship-to customer** (q74): used `web.ship_customer` (`ws_ship_customer_sk`) where
  the ref joins `web.billing_customer` (`ws_bill_customer_sk`). Swap → exact 92-row match. Quote:
  *"both `store_sales.customer` and `web_sales.ship_customer` should reference the same `customer`
  model"* — never weighed bill vs ship.
- **Return-line demo vs customer's own demo** (q85): `wr.returning_demographic` /
  `wr.refunded_demographic` (return-line FKs) vs `wr.billing_customer.demographics` (customer
  current snapshot). Agent picked the customer path → wrong averages (94.0/3078.98 vs 53.0/2843.56).

- **Error signature:** silent wrong result; mixed-direction per-bucket deltas (q88 bucket2 +124,
  bucket4 −495) — diagnostic of a wrong *role* join rather than fanout or null-drop.

**Fix:** (a) model comments should CROSS-REFERENCE the trap on the attractant role, e.g. on
`customer.household_demographic`: "for a filter about the demographics OF A SALE, use
`store_sales.household_demographic` (sale-time), not this (customer's current)." (b) per-prompt:
say "the household demographics recorded ON THE SALE", "the billing customer", "the demographic
recorded on the return."

---

## C-measure — Extended vs per-unit (q64, q78)

`ss_sales_price`/`ss_list_price`/`ss_wholesale_cost` are PER-UNIT; `ss_ext_*` are line-extended
(per-unit × quantity). The model exposes both (`sales_price` vs `ext_sales_price`); the agent used
the extended columns where the reference sums per-unit → values inflated by the line quantity.

- **Quote (q64):** *"'cumulative extended list price (across catalog sales for that item)' -
  sum(ext_list_price)…"* — anchored on "extended" from the catalog qualifier and carried it into
  the plain-worded store `list price`/`wholesale cost` sums.
- **Error signature:** keys/quantities/counts match; cost/price columns inflated by exactly the
  per-row quantity (×20, ×56, ×58, ×87), reshuffling the sorted top-100 → all rows differ.
- **The prompt wording is itself misleading (q78):** it says *"per-line sales price"*, which reads
  as line-extended, but the reference uses per-unit `ss_sales_price`.

**Fix (prompt):** say **"per-unit"** explicitly ("per-unit list price", "per-unit sales price")
and reserve "extended"/"line total" for the `ext_*` cases; never "per-line" for a per-unit price.

---

## C-format / spec-adherence (q36, q49) — lower priority

The wording is already explicit; the agent just didn't follow it. q49: emitted channel
`'CATALOG'` where the prompt demands lowercase `'catalog'` (agent fixated on the column NAME,
blind to value re-casing: *"the issue is that `s.channel` becomes `s_channel` … Trilogy prefixes
with the import alias"*). q36: ordered category across all levels instead of gating to leaf rows,
and used `limit 150` vs the stated `limit 100`. These are agent spec-adherence slips (partly
variance), not wording gaps — a per-query reword won't reliably help.

---

## Aggregate fix & impact

The high-leverage move is ONE shared convention (preamble + sharpened/cross-referenced model
comments) covering C1 + C2 + C-measure: **codes = `text_id`; joins = surrogate `id`; a bare
dimension attribute = the SALE/LINE role, not the customer's current snapshot; prices are
per-unit unless it says extended.** That targets ~11 of the 12 C queries with a single edit set,
rather than 11 separate prompt tweaks.
