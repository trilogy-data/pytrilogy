# web_sales scan node: v3 vs v4

Build phase (merge/canonicalization) is shared. Below: the base `BuildDatasource` (identical) and the built `QueryDatasource` node each engine's discovery produces for the bare web_sales scan in query02-one (`datasource_inlining` off).

## v3: CTE `questionable` — QueryDatasource grain `Grain<web_sales.item.id,web_sales.order_number>`

**QueryDatasource.output_concepts** (what the node emits)

| address | pseudonyms |
|---|---|
| `web_sales.date.id` | `date.id` |
| `web_sales.ext_sales_price` | `` |
| `web_sales.item.id` | `` |
| `web_sales.order_number` | `` |

**QueryDatasource.source_map** (address → provider)

| address | providers |
|---|---|
| `web_sales.bill_address.id` | ['web_sales.web_sales'] |
| `web_sales.bill_household_demographic.id` | ['web_sales.web_sales'] |
| `web_sales.billing_customer.id` | ['web_sales.web_sales'] |
| `web_sales.date.id` | ['web_sales.web_sales'] |
| `web_sales.ext_discount_amount` | ['web_sales.web_sales'] |
| `web_sales.ext_list_price` | ['web_sales.web_sales'] |
| `web_sales.ext_sales_price` | ['web_sales.web_sales'] |
| `web_sales.ext_ship_cost` | ['web_sales.web_sales'] |
| `web_sales.ext_wholesale_cost` | ['web_sales.web_sales'] |
| `web_sales.item.id` | ['web_sales.web_sales'] |
| `web_sales.list_price` | ['web_sales.web_sales'] |
| `web_sales.net_paid` | ['web_sales.web_sales'] |
| `web_sales.net_profit` | ['web_sales.web_sales'] |
| `web_sales.order_number` | ['web_sales.web_sales'] |
| `web_sales.promotion.id` | ['web_sales.web_sales'] |
| `web_sales.quantity` | ['web_sales.web_sales'] |
| `web_sales.row_counter` | ['web_sales.web_sales'] |
| `web_sales.sales_price` | ['web_sales.web_sales'] |
| `web_sales.ship_address.id` | ['web_sales.web_sales'] |
| `web_sales.ship_customer.id` | ['web_sales.web_sales'] |
| `web_sales.ship_date.id` | ['web_sales.web_sales'] |
| `web_sales.ship_household_demographic.id` | ['web_sales.web_sales'] |
| `web_sales.ship_mode.id` | ['web_sales.web_sales'] |
| `web_sales.time.id` | ['web_sales.web_sales'] |
| `web_sales.warehouse.id` | ['web_sales.web_sales'] |
| `web_sales.web_page.id` | ['web_sales.web_sales'] |
| `web_sales.web_site.id` | ['web_sales.web_sales'] |
| `web_sales.wholesale_cost` | ['web_sales.web_sales'] |

## v4: CTE `juicy` — QueryDatasource grain `Grain<web_sales.item.id,web_sales.order_number>`

**QueryDatasource.output_concepts** (what the node emits)

| address | pseudonyms |
|---|---|
| `web_sales.date.id` | `date.id` |
| `web_sales.ext_sales_price` | `` |
| `web_sales.item.id` | `` |
| `web_sales.order_number` | `` |

**QueryDatasource.source_map** (address → provider)

| address | providers |
|---|---|
| `web_sales.bill_address.id` | ['web_sales.web_sales'] |
| `web_sales.bill_household_demographic.id` | ['web_sales.web_sales'] |
| `web_sales.billing_customer.id` | ['web_sales.web_sales'] |
| `web_sales.date.id` | ['web_sales.web_sales'] |
| `web_sales.ext_discount_amount` | ['web_sales.web_sales'] |
| `web_sales.ext_list_price` | ['web_sales.web_sales'] |
| `web_sales.ext_sales_price` | ['web_sales.web_sales'] |
| `web_sales.ext_ship_cost` | ['web_sales.web_sales'] |
| `web_sales.ext_wholesale_cost` | ['web_sales.web_sales'] |
| `web_sales.item.id` | ['web_sales.web_sales'] |
| `web_sales.list_price` | ['web_sales.web_sales'] |
| `web_sales.net_paid` | ['web_sales.web_sales'] |
| `web_sales.net_profit` | ['web_sales.web_sales'] |
| `web_sales.order_number` | ['web_sales.web_sales'] |
| `web_sales.promotion.id` | ['web_sales.web_sales'] |
| `web_sales.quantity` | ['web_sales.web_sales'] |
| `web_sales.row_counter` | ['web_sales.web_sales'] |
| `web_sales.sales_price` | ['web_sales.web_sales'] |
| `web_sales.ship_address.id` | ['web_sales.web_sales'] |
| `web_sales.ship_customer.id` | ['web_sales.web_sales'] |
| `web_sales.ship_date.id` | ['web_sales.web_sales'] |
| `web_sales.ship_household_demographic.id` | ['web_sales.web_sales'] |
| `web_sales.ship_mode.id` | ['web_sales.web_sales'] |
| `web_sales.time.id` | ['web_sales.web_sales'] |
| `web_sales.warehouse.id` | ['web_sales.web_sales'] |
| `web_sales.web_page.id` | ['web_sales.web_sales'] |
| `web_sales.web_site.id` | ['web_sales.web_sales'] |
| `web_sales.wholesale_cost` | ['web_sales.web_sales'] |

## Base BuildDatasource `web_sales` (shared, build-phase) — relevant columns

| address | pseudonyms | column alias |
|---|---|---|
| `web_sales.date.id` | `date.id` | `WS_SOLD_DATE_SK` |
| `web_sales.ext_sales_price` | `` | `WS_EXT_SALES_PRICE` |
| `web_sales.item.id` | `` | `WS_ITEM_SK` |
| `web_sales.order_number` | `` | `WS_ORDER_NUMBER` |
| `web_sales.ship_date.id` | `` | `WS_SHIP_DATE_SK` |
---

## Conclusion

**The build phase is shared and identical** — the base `BuildDatasource` carries the merge (`web_sales.date.id` pseudonym `date.id`) under both engines. The ONLY divergence is the built node's `output_concepts`:

- **v3** `questionable` emits `web_sales.date.id` only.
- **v4** `juicy` emits `web_sales.date.id` **and** the extra canonical `date.id`.

The `source_map` is byte-identical (both resolve the FK as `web_sales.date.id ← web_sales.web_sales`). Because v4's node *advertises* `date.id`, the consumer's source_map records `date.id ← juicy`, and `InlineDatasource` (which checks the base datasource, where the FK only exists as `web_sales.date.id`) reports `date.id` missing and refuses to fold the scan.

### Why dropping the canonical at node-generation is NOT safe

Tried the dedup at two layers (bridge concept-assignment AND `create_datasource_node.output_concepts`). Both produce the SAME 8 regressions: `test_canonical_collision_{s1,s2,single_source}`, q46, q59, q64, q77, `membership_in_having`.

The "foreign canonical" is sometimes the **demanded output**, not a removable join-key free-rider. In the canonical-collision repro (`merge d1 into ~s1; merge d2 into ~s2`), the facts scan's `output_concepts` is `{id, s1, s2}` where `s1` (canonical, pseudonym `d1`) **is what the query selects** and `d1` is the native column. The dedup drops `s1` for `d1` and the multiselect loses its output. The distinguishing fact — "is this a mandatory query output?" — is NOT available at the datasource-node layer.

### Options

1. **Smarter inliner** (suggested): make `InlineDatasource` treat an inherited concept as covered when the base datasource provides it OR a pseudonym of it. Needs the narrow collision gate from the verbosity handoff — the coalesce/FULL-JOIN regression arises because a merged key has providers on *both* sides (fact FK + dimension), which is also true for `date.id`, so a naive "multiple providers" gate doesn't separate it from the collision `s1`.
2. **Demand parity upstream**: make v4 discovery demand the fact-native FK (`web_sales.date.id`) instead of canonical `date.id` for the shared-ROOT join key, so the canonical never enters the fact's `all_concepts`. Truest "match v3", but lives in the shared-ROOT/group-contract canonicalization (deep).

q2.1/q2.2 already pass at 7276 (grain fix); the `juicy`/`quizzical` inline (~270 chars) is a verbosity nicety, not a migration gate.

## EMPIRICAL TEST — unblocking the inliner (root_outputs |= pseudonyms)

Tried the one-line unblock (`root_outputs |= x.pseudonyms` so a pseudonym-served concept counts as covered):

- **q2.1 / q2.2: PASS, rows correct, 7276 → 6795** — the `juicy`/`quizzical` fold fires, exactly the win we wanted.
- **collision + q46 + q59: BREAK** — q59 = `INVALID_REFERENCE`; collision renders a **cartesian**:
  ```sql
  wakeful as (SELECT "facts"."id", "quizzical"."s1" as "s1"
              FROM "facts" FULL JOIN "quizzical" on 1=1)
  ```

### Why the pseudonym render does NOT save the collision

The merged concept has two physical providers, and they differ in JOIN TYPE:

| | providers of the merged key | join between them | fold result |
|---|---|---|---|
| q2.1 `date.id` | web_sales FK + date dim | **key join** (`date_id = date_id`) | render through pseudonym is consistent ✅ |
| collision `s1` | facts `d1` (merge) + date_spine | **cross join `1=1`** (spine is keyless) | folding drops `d1 as s1`; assembler re-sources `s1` from the spine + cartesian ❌ |

So it's not "multiple providers" (both have that). The signal is whether the merged concept is a **key join** between the parent and its sibling (safe) vs a keyless `1=1` cross join to a standalone home node like a date_spine (unsafe). That IS checkable in the consumer CTE's join structure → a **gated inliner is feasible**: allow pseudonym-coverage only when the concept key-joins the parent to its other provider.

## ROOT CAUSE of the date-spine collision (the `1=1` cross join)

Dug to the bottom. The collision is a **pre-existing v4 correctness bug**, independent of the inliner (these tests fail in v4 *isolation*; they only pass via full-suite ordering, and the inliner unblock flips them red in the full suite — they are the ONLY 3 full-suite regressions from the unblock; the q46/q59/q64/q77/membership "failures" were cold-cache isolation artifacts).

Chain (`merge d1 into ~s1`, a LEFT_OUTER merge):

1. **Build phase (shared v3/v4)**: the merge rewrites `d1` into an `UNNEST` at grain `s1`, and `facts` exposes `local.s1` as a **PARTIAL** column (`facts.partial_concepts = [local.s1]`) since `d1` is `?d1`/LEFT_OUTER.
2. **`_datasource_materializes(s1, facts) = False`** (correctly — a partial column doesn't fully materialize `s1`), so `materialized_roots = ∅` and `s1` is sourced from its UNNEST lineage = the **complete spine**. Semantically right for LEFT_OUTER (spine = complete date domain).
3. **The bug**: the aggregate `count(id) by s1` sources `id` from `facts` and complete-`s1` from the spine, but the bridge **drops `facts.s1` (the partial column)**, so the two nodes share no column → `FULL JOIN … on 1=1` → cartesian. The merge equivalence `facts.d1 = spine.s1` IS in the canonical map but never reaches the fact scan.
4. **v3** keeps `facts.d1 as s1` (the partial key) and `coalesce`s the spine — correct.

### Answers to the original questions
- The spine is keyless because it's an `UNNEST` (value = row) — inherent.
- Keyless does NOT degrade `x=x`→`1=1`. The `1=1` is because the fact was sourced WITHOUT its partial merge-key, so there's no shared column.
- We're not joining wrong — we're **dropping the fact's partial join key** in source-planning.

### Fix locus
The bridge/source-planner: when a complete-domain source (spine) supplies a key a fact carries only PARTIALLY, the fact must still emit that partial column as the join key (and LEFT-join the spine) rather than cross-joining. Fixing this makes the collision correct AND would let the inliner pseudonym-unblock land safely (it regresses only this case).

## FIXED 2026-06-30 — the date-spine partial-merge-key cross join

`_widen_merge_join_keys` (strategy_builder) computed `available` from
`parent_output_addresses` = the node's PARENTS' outputs. A leaf datasource
SelectNode has no parent nodes, so `available` was empty and the function bailed
(`if not available: continue`) — it never learned the fact could emit its partial
merge key. So the fact ROOT emitted only `id`, the merge with the spine had no
shared column, and it cross-joined `on 1=1` (cartesian).

Fix: include the leaf node's OWN datasource columns in `available` (`getattr(parent,
"datasource", None)` → its `output_concepts`). The declared join key (`s1`, in the
aggregate input contract's `preserve_keys`) is now satisfiable from the fact, so
it's carried as the join key. Result matches v3:

```sql
FULL JOIN "quizzical" on "facts"."d1" = "quizzical"."s1"
coalesce("facts"."d1","quizzical"."s1") as "s1"
```

Verified rows identical v3==v4: `select s1, m1` over `unnest([1..5])` merged with a
fact `d1` → `[(1,0),(2,2),(3,0),(4,1),(5,0)]`. Full sweeps: v4 4300 passed/0
failed, v3 4314 passed/0 failed. Lock: `tests/discovery/test_merge_unnest_partial_join.py`
(deterministic, executed). The inliner pseudonym-unblock can now land "forward" —
its only full-suite regression was this collision, now fixed.
