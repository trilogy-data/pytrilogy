# Resolved: enrich a property off a scoped-join key — chain the base key in

**Status:** RESOLVED (not a bug). Enriching a property keyed on a base key, off a
scoped self-join whose keys were *renamed*, works — you must chain the base key
into the join group: `join sk_a = sk_b = store_id`. Tests:
`tests/test_scoped_join_property_enrichment.py` (chained passes, unchained
documented unresolvable).

## The shape

Two aggregate rowsets over one fact, split by year, each renames the store key so
the self-join can distinguish the arms:

```sql
rowset agg_a <- where year = 2001 select store_id as sk_a, count(sale_id) as cnt_a;
rowset agg_b <- where year = 2002 select store_id as sk_b, count(sale_id) as cnt_b;
```

`join sk_a = sk_b` forms an equivalence group `{sk_a, sk_b}`. That group does NOT
contain `store_id` — the rename deliberately broke the identity so the two arms
are distinguishable. So a property keyed on `store_id` (`name`) has no declared
path from the join group and **cannot** be sourced. That is correct: the planner
should not silently re-identify `sk_a` with `store_id` (that is the whole point of
the rename).

## The idiom: chain the base key into the join group

```sql
# WORKS -> ('Alpha', 1, 1)
inner join agg_a.sk_a = agg_b.sk_b = store_id
select name as store_name, agg_a.cnt_a, agg_b.cnt_b
order by store_name asc;
```

Chaining `= store_id` puts the base key in the group, so `name` (grain
`store_id`) sources off it. `a = b = c` chained joins forming one equivalence
group is the documented scoped-join blend semantic.

```sql
# DOES NOT RESOLVE (group is {sk_a, sk_b}; store_id / name unreachable)
inner join agg_a.sk_a = agg_b.sk_b
select name as store_name, agg_a.cnt_a, agg_b.cnt_b ... ;
```

## Fully self-contained repro (copy-paste, no tpcds, no external tables)

```sql
key store_id int;
property store_id.name string;
key sale_id int;
property sale_id.year int;

datasource sales (
    sale_id: sale_id,
    store_id: store_id,
    yr: year,
)
grain (sale_id)
query '''
select 1 sale_id, 1 store_id, 2001 yr
union all select 2 sale_id, 1 store_id, 2002 yr
union all select 3 sale_id, 2 store_id, 2001 yr
''';

datasource stores (
    store_id: store_id,
    nm: name,
)
grain (store_id)
query '''
select 1 store_id, 'Alpha' nm
union all select 2 store_id, 'Beta' nm
''';

rowset agg_a <- where year = 2001 select store_id as sk_a, count(sale_id) as cnt_a;
rowset agg_b <- where year = 2002 select store_id as sk_b, count(sale_id) as cnt_b;

inner join agg_a.sk_a = agg_b.sk_b = store_id    -- chain the base key in
select name as store_name, agg_a.cnt_a, agg_b.cnt_b
order by store_name asc;
```

Store 1 is the only one with sales in both years, so the inner self-join keeps
just it; result `('Alpha', 1, 1)`. Drop the `= store_id` and it dead-ends.

## The one real (minor) planner opportunity: a clearer error

The un-chained form raises the internal
`ValueError: Cannot resolve query. No remaining priority concepts` — opaque for a
user who just forgot the chain. A nicety (not a correctness bug) would be to
detect "a rowset-renamed key whose lineage content is a base key, and a property
of that base key is requested but the base key is not in any join group" and
raise a targeted error suggesting `join ... = <base_key>`. The recoverable signal
is the rowset key's lineage content (`agg_b.sk_b.lineage.content` ->
`local._agg_b_sk_b`, whose `pseudonyms == {store_id}`).

Do NOT try to *auto*-source it (re-identify `sk` with `store_id` silently) — that
defeats the rename and, attempted via `_pseudonym_bridge_keys` in
`rowset_node.py`, produced a `FULL JOIN ... ON 1=1` cartesian with wrong rows
(verified and reverted). The correct behavior is the explicit chain; only the
*error message* is worth improving.

## Applying this to q64 (`tests/modeling/tpc_ds_duckdb/query64.preql`)

The "aggregate on keys, join descriptive text later" form is therefore writable —
group `agg_99`/`agg_00` on the id keys (`item.id`, `store.id`, `sale_address.id`,
`customer.address.id`), chain each join key to its base
(`agg_99.item_sk_99 = agg_00.item_sk_00 = item.id`, etc.), then enrich
`item.product_name` / `store.name` / address text in the outer select. Not yet
applied to `query64.preql` (it currently carries the text dims in the aggregate
grain, which also works); a follow-up could switch it to the chained enrich-later
form for a narrower aggregate. Verify against PRAGMA tpcds(64) at sf=1.

## Perf constraint that overrides "fewer rowsets" for q64 (independent of the above)

Pushing q64's
`customer_demographic.marital_status != customer.demographics.marital_status`
inequality into the row-level filter (and collapsing the `ss_rows_*` row-grain
rowsets away) blows the **sf=1** plan past the 3GB cap and 5+ minutes — the
LEFT->INNER customer_demographics upgrade DuckDB plans catastrophically (PR #549,
"0.05s -> 18s"). The extra `ss_rows_*` rowset isolates the marital comparison and
keeps the plan fast. Keep marital deferred via the row-grain rowset even when
adopting the chained enrich-later form. `query64.preql` carries this note.

A 3GB DuckDB `memory_limit` was added to the tpc_ds test harness
(`tests/modeling/tpc_ds_duckdb/conftest.py`) so a pathological plan errors out
instead of taking the machine down — keep it.

## Related

- `handoff_q64_join_grain_resolution.md` — the scoped-join *key* sourcing fix
  (2026-06-09, RESOLVED). This doc is the *property enrichment* follow-on.
- memory: `project_scoped_join_mirrors_merge_concept`,
  `reference_scoped_join_blend_semantics` (the `a=b=c` group semantics),
  `project_multiselect_to_join_conversion`.
