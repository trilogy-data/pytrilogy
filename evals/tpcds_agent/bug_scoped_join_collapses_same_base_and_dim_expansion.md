# Bug: query-scoped JOIN over-collapses — drops dim expansion and self-join predicates

**Status:** Bug A FIXED 2026-06-08 (commit `rowset_fixes`); **Bug B still OPEN.**
Found 2026-06-08 while converting the tpc_ds_duckdb multiselect queries to the new
`join` clause to validate "rowsets + joins replace multiselect".

The query-scoped `inner|left join a = b` clause merges the two keys into a single
concept via pseudonym substitution (`augment_pseudonyms_for_scoped_joins` /
`_build_scoped_merge_index` in `trilogy/core/models/build.py`). That collapse is
**too aggressive** and surfaces as two distinct planner failures. Both block
converting alignment-style multiselects to the join clause.

---

## Bug A — dim attributes unresolvable after joining a rowset FK to the dim key  (FIXED)

Fixed by the `rowset_fixes` commit. query46/query68 now use the clean
single-rowset form below (FK rowset + `inner join … = dim.key`, selecting dim
attributes directly) and pass `test_forty_six`/`test_sixty_eight`. The two-rowset
workaround is no longer needed. Original report retained below.

The user flagged this one directly. Joining a rowset's foreign key to a freshly
imported dimension key, then selecting the dimension's *attributes*, fails to
resolve — the planner sources the merged key straight from the rowset and never
expands the dimension's own datasource.

### Repro (q46, the form that FAILS)
```trilogy
import physical_sales as physical_sales;
import customer as customer;

rowset bought <- where
    physical_sales.date.year in (1999, 2000, 2001)
select
    physical_sales.customer.id,
    physical_sales.sale_address.city as bought_city,
    physical_sales.ticket_number,
    sum(physical_sales.coupon_amt) as amt,
;

inner join bought.physical_sales.customer.id = customer.id
select
    customer.last_name,         -- UNRESOLVABLE
    customer.first_name,        -- UNRESOLVABLE
    customer.address.city,      -- UNRESOLVABLE
    bought.bought_city,
    bought.amt,
;
```

Discovery loop ends with:
```
[DISCOVERY LOOP] Could not resolve concepts ['customer.last_name',
'customer.first_name', 'customer.address.city', ...], outcome INCOMPLETE,
missing {'customer.first_name', 'customer.address.city', 'customer.last_name'}
```
It *has* `customer.id` (served from the rowset's `SS_CUSTOMER_SK`) but won't join
the `customer` datasource to enrich `last_name`/`first_name`/`address.city`.

### Workaround (the form that WORKS — currently shipped in query46/query68)
Give the dimension its OWN rowset and join two rowsets (the proven q54 pattern):
```trilogy
rowset cust <- select
    customer.id, customer.last_name, customer.first_name,
    customer.address.city as current_city,
;
inner join bought.physical_sales.customer.id = cust.customer.id
select cust.customer.last_name, cust.current_city, bought.bought_city, ... ;
```

### Expected
`inner join rowset.fk = dim.key` then selecting `dim.attr` should behave exactly
like the model-level `merge rowset.fk into dim.key` does — i.e. join the `dim`
datasource on the merged key to enrich its attributes. (The model-merge form
resolves; only the scoped-join form fails.)

---

## Bug B — self-join equi-join predicate collapses to a cross join

When **both** join keys trace back to the *same* base concept (a self-join /
year-over-year comparison), the pseudonym-merge unifies them, so the equi-join
predicate `a = b` degenerates to `b = b` → `1=1`, i.e. a cross join.

### Repro (q75 join form)
```trilogy
import all_sales as sales;
rowset deduped <- where sales.item.category = 'Books' and sales.date.year in (2001, 2002)
select sales.date.year, sales.item.brand_id, sales.item.class_id,
       sales.item.category_id, sales.item.manufacturer_id,
       (sales.quantity - coalesce(sales.return_quantity,0)) as cnt_per_row, ... ;

rowset curr <- where deduped.sales.date.year = 2002
select deduped.sales.item.brand_id as brand_id, ..., sum(deduped.cnt_per_row) as curr_cnt ;
rowset prev <- where deduped.sales.date.year = 2001
select deduped.sales.item.brand_id as brand_id, ..., sum(deduped.cnt_per_row) as prev_cnt ;

inner join curr.brand_id = prev.brand_id
inner join curr.class_id = prev.class_id
inner join curr.category_id = prev.category_id
inner join curr.manufact_id = prev.manufact_id
select curr.brand_id, prev.prev_cnt, curr.curr_cnt, ... ;
```

Generated SQL (note the `1=1` and that the `curr` CTE doesn't even project its
join keys):
```sql
...
SELECT 2001 as prev_year, ...,
   "concerned"."curr_curr_cnt" - "juicy"."prev_prev_cnt" as "sales_cnt_diff", ...
FROM "juicy"
  FULL JOIN "concerned" on 1=1          -- <-- equi-join predicate lost
WHERE cast("concerned"."curr_curr_cnt" as numeric(17,2))
    / cast("juicy"."prev_prev_cnt" as numeric(17,2)) < 0.9
```
Result: a cartesian product of all (curr × prev) item-attribute groups → wrong
rows. `curr.brand_id` and `prev.brand_id` both resolve to
`deduped.sales.item.brand_id`, so the merge makes them one concept and the
predicate vanishes.

### q64 variant
Same shape (1999 vs 2000 store_sales slices joined on item/store-name/zip), but
the two aggregate rowsets have asymmetric grains, so instead of `1=1` the query
fails to resolve at all:
```
ValueError: Cannot resolve query. No remaining priority concepts,
have attempted {'agg99.b_city_99', 'agg00.cnt_00'} ...
```

### Contrast — the case that WORKS (q54)
`inner join cust_ss.ss_cust_county = stores_cs.scs_county` joins two *different*
base concepts (customer-address county vs store county). Predicate is preserved:
```sql
INNER JOIN "sparkling"
  on "concerned"."cust_ss_ss_cust_county" = "sparkling"."stores_cs_scs_county"
 AND "concerned"."cust_ss_ss_cust_state" = "sparkling"."stores_cs_scs_state"
```
So the bug is specific to **same-base-concept** joins.

### Expected
A scoped equi-join between two rowsets must emit a real join predicate even when
both keys derive from the same base concept — the two rowsets are distinct
filtered row sets (a self-join). The multiselect `align` handles exactly this by
minting distinct per-arm concepts (`brand_id_curr` vs `brand_id_prev`) and a real
`young.i_brand_id is not distinct from vacuous.i_brand_id` join; the join clause
should produce equivalent SQL.

---

## Root cause (shared)
`_build_scoped_merge_index` collapses `source → target` addresses, then
pseudonym substitution rewrites the source concept to *be* the target. That:
- (A) lets the planner satisfy the merged key from the source rowset and skip the
  target dim's datasource (no enrichment join), and
- (B) erases the predicate for same-base self-joins (`b = b`).

A fix likely needs the scoped join to keep the source key as a *distinct*
concept that is **equated at the join boundary** (emit the ON predicate, keep
both sides independently sourceable) rather than fully unifying addresses — i.e.
behave like `align` does for the multiselect arms.

## Impact / who hit it
Converting the tpc_ds_duckdb multiselect suite to the join clause:
- q46, q68 — converted to the clean single-rowset form once Bug A was fixed.
- q64, q75 — **NOT convertible** because of Bug B (still open); left as multiselects.

## Validation harness
- Each query is row-for-row checked against `PRAGMA tpcds(N)` in
  `tests/modeling/tpc_ds_duckdb/test_queries.py` (q46→`test_forty_six`,
  q75→`test_seventy_five`, q64→`test_sixty_four` at sf=0.01).
- `run_query` writes `zquery<N>.log` (the generated SQL) **only after** the
  row-match assertion passes — a failed run leaves a STALE log. To inspect SQL
  for a failing rewrite, call `engine.generate_sql(text)[-1]` directly (data
  independent) instead of reading the log.
