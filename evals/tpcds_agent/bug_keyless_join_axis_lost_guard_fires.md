# **P0** the keyless-join guard fires on two live query shapes (q04, q05)

**Filed 2026-08-16 against `a65b13c9c`.** Supersedes
`handoff_q04_all_sales_pivot_600s_hang.md` and
`handoff_q05_rollup_rowset_binding.md`, both deleted: their live residue is
this one category, and the q04 handoff's open question ("where did the 600s
go?") is answered below.

## What this is

`join_resolution._raise_if_keyless_row_bearing_join` landed in `a65b13c9c` as
the category guard for the q30 keyless-FINAL-merge fix. Its contract is
explicit: it only raises when the two sides **share a projectable join axis** —
"the axis existed, the planner could have joined on it, and it dropped it" — and
its own doctrine says a firing means the fix belongs upstream in the
demand/contract passes, never in relaxing the check.

It fires on two shapes that are not covered by any test. Both are genuine
pre-existing axis-loss bugs that the guard newly makes loud.

## Shape 1 — `all_sales` year-over-year pivot (was the q04 600s hang)

Repro: canonical `tests/modeling/tpc_ds_duckdb/all_sales.preql`, generation only.

```trilogy
import all_sales as sales;

auto line_value <- sales.ext_list_price - sales.ext_wholesale_cost - sales.ext_discount_amount + sales.ext_sales_price;

with annual as
where sales.ext_sales_price is not null
select
    sales.billing_customer.id as cid,
    sales.channel as channel,
    sales.sale_date.year as yr,
    sum(line_value) as annual_value
;

with cust as
select
    annual.cid as cid,
    sum(annual.annual_value ? annual.channel = 'STORE' and annual.yr = 2001) as store_2001,
    sum(annual.annual_value ? annual.channel = 'STORE' and annual.yr = 2002) as store_2002,
    sum(annual.annual_value ? annual.channel = 'CATALOG' and annual.yr = 2001) as cat_2001,
    sum(annual.annual_value ? annual.channel = 'CATALOG' and annual.yr = 2002) as cat_2002,
    sum(annual.annual_value ? annual.channel = 'WEB' and annual.yr = 2001) as web_2001,
    sum(annual.annual_value ? annual.channel = 'WEB' and annual.yr = 2002) as web_2002
;

where sales.channel = 'STORE'
    and sales.sale_date.year = 2002
    and sales.ext_sales_price is not null
select
    cust.cid,
    sales.billing_customer.first_name as first_name,
    sales.billing_customer.last_name as last_name,
    sales.billing_customer.preferred_cust_flag as preferred_cust_flag
subset join cust.cid = sales.billing_customer.id
having
    cust.store_2001 > 0 and cust.cat_2001 > 0 and cust.web_2001 > 0
    and cust.store_2002 is not null and cust.cat_2002 is not null and cust.web_2002 is not null
    and cust.cat_2002 / cust.cat_2001 > cust.store_2002 / cust.store_2001
    and cust.cat_2002 / cust.cat_2001 > cust.web_2002 / cust.web_2001
order by cust.cid asc nulls first
limit 100;
```

At `a65b13c9c`: `UnresolvableQueryException`, axis lost between the
`billing_customer x sale_date x store_sales`-unified contributor and its
sibling.

**A/B against `0e6c33f2e`** (the commit before the guard, run from a worktree
with the same venv): the query **plans**, 28,410 chars, containing exactly one
`on 1=1`. That cross join is the 600s hang the eval agent hit — the handoff's
"compile vs execute" question is settled: planning is fast, the emitted SQL
cross-joins.

## Shape 2 — rollup over union-joined rowsets (the old q05 binder repro)

Repro (kept, still checked in):
`evals/tpcds_agent/repro_q05_rollup_rowset_binding.{py,preql}` against
`tests/modeling/tpc_ds_duckdb`. The script's assertion is stale — it now dies
inside `generate_sql` instead.

At `a65b13c9c`: `UnresolvableQueryException` from the guard.
At `0e6c33f2e`: plans, 10,946 chars, one `on 1=1`.

The shape is two channel rowsets aggregated at `(channel, entity)`, combined
with two scoped `union join` clauses, normalized by `coalesce` in the final
select, then `by rollup (channel)`. Note the narrower `tests/engine/test_duckdb_rollup_rowset_binding.py`
(3 tests, the 2026-08-06 fix's regression coverage) still passes — the guard
fires only on the fuller shape in the repro preql.

## Why this is one bug, not two

Both are FINAL merges whose contributor lost the join axis at the demand/contract
layer, exactly the q30 category. Neither is a new regression: both emitted silent
cartesians before the guard existed. The guard converted "wrong rows / 10-minute
hang" into "loud error", which is the intended trade, but the upstream axis loss
is unfixed and these two shapes are now **hard-blocked**.

## Fix direction

Same placement rule as q30: the passes that decide which keys a contributor must
expose have to identify them here too (`group_graph._group_final_grain_contribution`
/ `_lineage_pinned_grain`, `strategy_builder._wrap_for_grain`'s FK-hop axis
resolution, `group_graph._compute_concept_sets` rowset-grain resolution). Do not
relax `_raise_if_keyless_row_bearing_join`.

Both shapes need row-asserting tests, not planning-status tests — the q30 record
already documents why (`test_v4_parity_cases` checks planning status only).
