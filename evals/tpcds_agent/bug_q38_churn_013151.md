# q38 token sink (run 20260629-013151) — nullable-FK LEFT join inflates cross-channel intersection (SILENT)

q38: count of unique `(customer last_name, first_name, sale date)` tuples present in **all
three** of store/catalog/web sales in 2000 (TPC-DS INTERSECT). Burned **1.69M tokens** and
FAILED. Reference answer = **107**. Coarse scan blamed agent syntax errors (`Syntax [103]
GROUP BY`, `[223] *`), but those were incidental — the real obstacle is **SILENT**: every
natural Trilogy form returns **155**, not 107, and the agent could never reconcile it.

## What the agent saw (the churn)

The agent's grouping idea was correct and stable: group `all_sales` by
`(billing_customer.last_name, billing_customer.first_name, date.date)`,
keep tuples with `count_distinct(channel) = 3`, count them. But the numbers were irreconcilable:

- `count(per_combo.last_name)` → **0**  (idx 31)  — all matched tuples have NULL last_name
- `count(per_combo.c_date)`    → **155** (idx 106+, final submission)
- add `billing_customer.last_name is not null` + `having channels=3` → **0 rows** (idx 91)
- debug `query38_check`: total=155, **with_name=0** (idx 124) — *every* match is NULL-named

So the agent oscillated between 0 and 155, saw that "all matches are nameless," tried the
per-channel single-import form, hit two `Syntax [103]` GROUP BY rejections, and exhausted its
budget. Neither 0 nor 155 is the reference 107, and nothing in the tool output explains why.

## Root cause — default LEFT OUTER join to a nullable-FK dimension

`billing_customer.id` is a **nullable** FK in `all_sales` (`?billing_customer.id` /
`SS_CUSTOMER_SK`, `CS_BILL_CUSTOMER_SK`, `WS_BILL_CUSTOMER_SK`). When the query references
`billing_customer.last_name/first_name` with **no** null predicate, the planner joins the
`customer` dimension with **LEFT OUTER** to preserve fact rows:

`trilogy/core/processing/join_resolution.py:138-139`
```python
if left_is_nullable:
    return JoinType.LEFT_OUTER   # left (fact) side has nullable connecting key
```

TPC-DS leaves ~4-5% of `*_customer_sk` NULL. Under the LEFT join those NULL-key sales rows
from **all three channels** survive with `last_name=NULL, first_name=NULL` and collapse into a
single phantom `(NULL, NULL, date)` group, which trivially satisfies "appears in all 3
channels." For every date that has a null-customer sale in each channel, that phantom group is
counted once → the answer inflates **107 → 155** (delta = 48 such dates).

Generated SQL (no null filter): `LEFT OUTER JOIN customer ... GROUP BY last,first,date HAVING
count(distinct channel)=3`. Verified against raw DuckDB on the same db:

| customer join | result |
|---|---|
| INNER (reference / canonical) | **107** |
| LEFT OUTER (Trilogy's natural plan) | **155** |

Trilogy is *faithfully* executing the LEFT-join plan — 155 is the correct execution of the
wrong-for-this-question join shape.

## Why the agent could never get 107

Both natural shapes give 155:
- `count_distinct(channel) = 3` grouped by the name tuple → **155**
- per-channel presence flags `sum(case when channel='STORE'...)>0 and ... > 0` (no id guard) → **155**

Adding `billing_customer.last_name is not null` flips the join LEFT→INNER (a `col IS NOT NULL`
on the right table promotes the join), which **drops the null-SK rows entirely** → **0 rows**
(true non-null count is genuinely 0; no real-named customer bought in all 3 channels on the
same date). So the only two states reachable by adding/removing a null filter are **155** and
**0** — the reference **107** sits between them and is reachable *only* by the non-obvious
workaround the canonical model embeds: an explicit `billing_customer.id is not null` guard
*inside each per-channel CASE* (keeps real-but-null-named customers, drops null-SK rows).
Verified: canonical `query38.preql` (with the id guards) → 107; the same presence-flag form
with the guards removed → 155.

This is the token sink: a SILENT correctness trap with no diagnostic. The natural, obvious
query runs clean and returns a plausible single number that is simply wrong, the result is
unstable under a cosmetic null filter (155 ↔ 0), and recovering the reference requires guarding
the FK *id* (not the name) inside the aggregate — something no error message hints at.

## Classification

- **SILENT wrong-rows** (155 vs expected 107) — primary.
- **Ergonomic / irreproducibility trap** — no natural single-form query yields the reference;
  result flips 155↔0 on a cosmetic `is not null`; only an `id is not null`-inside-CASE guard works.
- Not a crash; not the agent's syntax slips. The LEFT-join-on-nullable-FK default
  (`join_resolution.py:138-139`) lumping all NULL/unmatched fact rows into one phantom group
  during distinct-tuple aggregation is the framework-level obstacle.

## Minimal repro (workspace `…/results/20260629-013151/workspace`, `trilogy run`)

```preql
import raw.all_sales as s;
with triples as
where s.date.year = 2000
select s.billing_customer.last_name as c_last,
       s.billing_customer.first_name as c_first,
       s.date.date as c_date,
       count_distinct(s.channel) as channel_count
having channel_count = 3;
select count(triples.c_date) as unique_combinations limit 100;   -- 155, expected 107
```
Add `and s.billing_customer.last_name is not null` to the `where` → 0 rows (join flips to INNER).
Add `and s.billing_customer.id is not null` instead, with per-channel CASE flags → 107.

## Suggested direction (not applied — READ-ONLY)

When a nullable-FK dimension is LEFT-joined *and* its attributes are used as **grouping keys**
(not just projected), NULL-key fact rows form a phantom merged group that corrupts
distinct/COUNT aggregates. Options: (a) when grouping by a LEFT-joined nullable dimension's
attributes, exclude NULL-FK rows (treat unmatched as no-group rather than one shared NULL
group); or (b) surface a planner note/diagnostic that a nullable dimension is contributing a
NULL grouping bucket, so the silent 107→155 drift is visible. The reference works around it
manually with `billing_customer.id is not null` inside each CASE — that workaround should be
discoverable.
