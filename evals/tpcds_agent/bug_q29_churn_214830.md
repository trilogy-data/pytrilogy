# q29 churn (run 20260629-214830) — 2.78M tokens, SILENT mis-grain. **FRAMEWORK BUG.**

> **FIXED 2026-06-29.** The defect was the silent *widening* of an explicit `inner join`
> to FULL (null-EXTENSION), NOT the null-safe equality. Per the project owner: NULL keys
> are VALID members — an inner join SHOULD match `NULL = NULL` (`IS NOT DISTINCT FROM`),
> and `ai/constants.py` already documents "Joins do NOT drop nulls." So NULLs must be
> PRESERVED, not dropped.
>
> Fix = new `scoped_inner_join_keys` registry (build.py, build_environment.py) carries the
> canonical INNER key into `get_join_type` (join_resolution.py): a scoped INNER key keeps
> `JoinType.INNER` instead of nullability widening it to FULL/LEFT. `get_modifiers` is
> UNCHANGED — `Modifier.NULLABLE` (→ `IS NOT DISTINCT FROM`) stays, so NULL rows align
> rather than drop. Two guards keep prior behavior: (1) `full_join_keys` is honored FIRST;
> (2) the override is gated on NOT-partial — a PARTIAL side is a genuine scoped LEFT/FULL
> directive (e.g. `inner join k=a` mixed with `left join k=b`) whose directionality is
> preserved. Automatic (non-scoped) nullable joins are untouched and keep widening to
> outer to preserve their NULL-key rows (`test_get_node_joins_merge_nullable_drives_outer_join`).
>
> Result for the minimal repro: INNER + `IS NOT DISTINCT FROM` → `[(10,…), (None,…)]`
> (anonymous rows matched/preserved; unmatched dropped). Tests: tests/test_scoped_join.py
> (8 new). NOTE: this does NOT by itself reduce real q29 to 1 row — the canonical answer
> needs the non-null return-customer FK or an explicit `is not null`; this fix removes the
> spurious FULL null-extension and honors the documented inner-join + nulls-preserved contract.


## Verdict: this OVERTURNS the prior handoff (`bug_q29_churn_030015.md`)
The prior note classified q29 as "agent modeling error, not a framework bug" (agent
pre-aggregated each fact before joining). That is wrong for *this* run. The agent's
final query is structurally a correct fine-grain correlated form. It is the **engine**
that silently rewrites the agent's `inner join` on a nullable foreign key into a
null-safe (`IS NOT DISTINCT FROM`) + FULL/LEFT OUTER join, producing NULL-extended,
fanned-out rows. The natural "same customer" reading (buyer FK) is made silently wrong.

## The obstacle (what the agent fought)
q29 = store sales Sep-1999 ⨝ store returns (same cust/item/ticket, Sep–Dec 1999)
⨝ catalog sales (same customer billed + item, 1999–2001); sum three quantities per
(item code/desc, store code/name). Correct answer = **1 row** `(…,71,41,23)`.

The agent read "same customer" the obvious way and joined on the store-sale's own
buyer FK: `inner join ss.customer.id = cs.bill_customer.id`. Its query then returned
**15 rows** with NULL `store_code`/`store_name` and NULL quantity columns. Over 2.78M
tokens it kept observing the impossible output and never escaped:
- log: *"store had returns (63 units) but the store sale quantity is null — which
  doesn't make sense. Unless the inner join to catalog_sales creates some weird
  behavior…"*
- log: *"store sales can have anonymous customers. So rows with null customer.id
  wouldn't match the inner join. But … the results show rows with null customer."*
  (It diagnosed the right cause — nullable customer — but the engine's `IS NOT
  DISTINCT FROM` makes NULLs *match*, the exact opposite of inner-join intuition, so
  the evidence contradicted its mental model and it churned.)

## Minimal repro (wrong vs correct — same db, run's own `workspace/tpcds.duckdb`)
The ONLY difference between wrong and correct is which **valid** customer FK keys the
cross-fact join, i.e. whether that FK is nullable.

```
import raw.store_sales as ss; import raw.catalog_sales as cs;
where ss.date.year=1999 and ss.date.month_of_year=9 and ss.is_returned=true
  and ss.return_date.month_of_year between 9 and 12 and ss.return_date.year=1999
  and cs.sold_date.year in (1999,2000,2001)
select ss.item.text_id as item_code, ss.item.desc, ss.store.text_id as store_code,
  ss.store.name, sum(ss.quantity), sum(ss.return_quantity), sum(cs.quantity)
inner join ss.customer.id = cs.bill_customer.id     -- nullable FK (SS_CUSTOMER_SK: ?customer.id)
inner join ss.item.id = cs.item.id ...
```
- buyer FK `ss.customer.id` (nullable)            → **15 rows, 6 with NULL store. WRONG.**
- return FK `ss.return_customer.id` (non-null)    → **1 row `(…,71,41,23)`. CORRECT.**
- buyer FK **+ `and ss.customer.id is not null`**  → **1 row `(…,71,41,23)`. CORRECT.**
- canonical `tests/.../query29.preql` (uses return_customer) → 1 row, correct.

Bare 2-fact probe (`generate_sql`), measuring join shape vs FK nullability:
- `inner join ss.customer.id = cs.bill_customer.id`        → top `FULL JOIN`, `IS NOT DISTINCT FROM`
- `inner join ss.return_customer.id = cs.bill_customer.id` → `IS NOT DISTINCT FROM` = False
- buyer key + `where ss.customer.id is not null`           → `FULL JOIN` = False, plain `=`

So the nullable FK alone flips a user-requested INNER join into null-safe outer
behavior. `NULL IS NOT DISTINCT FROM NULL = TRUE`, so anonymous (null-customer) store
sales spuriously match null-customer catalog rows and the FULL-join measure recombine
null-extends the output → bogus (item, store) pairs, NULL dims, NULL/inflated sums.

Repro scripts: scratchpad `repro29{b,c,d,e,f}.py` (engine = `scoring.make_scoring_engine`).

## Root cause (file:line)
`trilogy/core/processing/join_resolution.py`:
- `get_join_type` (lines 109–145): when the connecting key is nullable on **both**
  sides it returns `JoinType.FULL` (line 132–133), and LEFT/RIGHT/FULL when nullable
  on one side (135–144). This is purely structural nullability inference — it does
  **not** respect the user's explicit scoped `inner join` directive.
- `get_modifiers` / `_side_nullable` (lines 425–441): stamps `Modifier.NULLABLE`
  whenever both exposed join keys can be NULL, which the duckdb dialect renders as
  `IS NOT DISTINCT FROM` (null-safe equality).

The existing optimizer `trilogy/core/optimizations/null_safe_join.py`
(`SimplifyNullSafeJoins`) only downgrades `IS NOT DISTINCT FROM` → `=` when a key is
*provably* non-null. A genuinely nullable FK (`?customer.id`) can never be proven, so
it stays null-safe + FULL. There is no path that honors a user's explicit `inner join`
as "drop NULL keys, drop unmatched rows" regardless of column nullability.

## Why this is a framework bug, not agent error
For an explicit `inner join a = b`, SQL `=` already drops NULL keys and unmatched rows
— that is the user's stated intent. The engine instead substitutes null-matching
(`IS NOT DISTINCT FROM`) + FULL/LEFT outer recombination, changing the result set
silently (clean `exit_code:0`, no error). The agent's buyer-customer correlation is a
reasonable, arguably the most natural, reading of "same customer"; the engine made it
wrong while the semantically-equivalent return_customer phrasing (non-null FK) works.
The correct answer being reachable only by picking the non-nullable FK (as the
canonical does) is an accident, not expressibility.

## Suggested fix direction (do NOT implement here)
A user-authored scoped `inner join` should pin INNER + plain `=` semantics on its keys
regardless of FK nullability (NULLs must not match under an explicit inner join).
Either (a) carry the scoped-join's explicit INNER intent into `get_join_type` so it is
not overridden by nullability inference, and suppress `Modifier.NULLABLE` for
explicit-inner scoped keys in `get_modifiers`; or (b) at minimum stop using
`IS NOT DISTINCT FROM` for keys originating from an explicit `inner join`. A targeted
diagnostic ("inner join key is nullable; NULL rows are being null-matched — add
`is not null` or the join is treated null-safe") would also have saved the 2.78M tokens.
```
