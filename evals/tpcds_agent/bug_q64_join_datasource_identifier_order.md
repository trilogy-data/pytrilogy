# q64 token sink — "Could not find CTE for datasource" (join-datasource identifier order mismatch)

Run: `evals/tpcds_agent/results/20260708-135136_enriched` — q64 burned 1,145,058 tokens (status PASS, heavy churn). Signature: one `Unexpected error`.

## Symptom

Agent (message 40) ran a q64 spelling that raised, verbatim:

```
Unexpected error in query64.preql: Could not find CTE for datasource
ss.item.items_join_cr.catalog_returns_..._qualifying_item_sk_..._at_ss_item_sk_filtered_by_8610126078781587;
have {'ss.item.items',
      'cr.catalog_returns_..._qualifying_item_sk_..._join_ss.customer.address...ss.store_sales..._grouped_by_...'
      'cr.catalog_returns_..._qualifying_item_sk_..._join_ss.item.items_at_ss_item_sk_filtered_by_8610126078781587'}
```

An unhandled `ValueError` escaping `generate_sql` is a framework bug (never acceptable). The
agent then thrashed (rewrote to inline subselects, dropped/re-added the `auto` concept) → the token blowup. Reproduced deterministically via `generate_sql` (no execution needed).

## Reproduced

The failing message-38 spelling reproduces the identical `ValueError` at the same file:line.

Minimal repro (`generate_sql` raises `ValueError: Could not find CTE for datasource ss.item.items_join_...`):

```trilogy
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

with cat_items as
select cs.item.sk as item_sk,
    sum(cs.ext_list_price) as cat_list_price_sum,
    sum(coalesce(cr.refunded_cash,0)) as cat_refund_sum
union join cs.item.sk = cr.item.sk
union join cs.order_number = cr.order_number;

auto qualifying_item_sk <- cat_items.item_sk ? cat_items.cat_list_price_sum > 2 * cat_items.cat_refund_sum;

with agg_sales as
where ss.item.sk in qualifying_item_sk
select ss.item.sk, ss.item.product_name, ss.date.year,   -- NOTE product_name (2nd item attr)
    count(ss.line_item) as sale_line_count;

with yr1999 as where agg_sales.ss.date.year = 1999
select agg_sales.ss.item.sk, agg_sales.sale_line_count;

with yr2000 as where agg_sales.ss.date.year = 2000
select agg_sales.ss.item.sk, agg_sales.sale_line_count;

select yr1999.ss.item.sk, yr1999.sale_line_count as c1999, yr2000.sale_line_count as c2000
subset join yr1999.ss.item.sk = yr2000.ss.item.sk;
```

## Trigger matrix

Three ingredients are jointly required; drop any one and it builds:

| Variant | Result |
|---|---|
| Full message-38 q64 spelling | EXC (matches agent) |
| Minimal above | EXC |
| Minimal, drop `ss.item.product_name` (only join-key `item.sk` in grain) | OK |
| Minimal, replace `where ss.item.sk in qualifying_item_sk` with a plain scalar filter | OK |
| Minimal, single downstream consumer (no yr1999/yr2000 split + subset self-join) | OK |
| `in qualifying_item_sk` + 2 item attrs, single flat select (no stacked rowsets) | OK |
| Canonical `tests/modeling/tpc_ds_duckdb/query64.preql` | OK (builds, 1 stmt) |

Necessary and sufficient:
1. A **semijoin `ss.item.sk in qualifying_item_sk`**, where `qualifying_item_sk` is a rowset-derived, filtered `auto` concept (`cat_items.item_sk ? <agg cond>`).
2. The semijoin-anchored rowset (`agg_sales`) carries **a second attribute from the same base datasource as the join key** (`ss.item.product_name` alongside `ss.item.sk`, both from `ss.item.items`) — this forces `ss.item.items` into the semijoin's join graph.
3. That rowset is **fanned into two stacked consumers and re-joined** (`yr1999`/`yr2000` + `subset join`).

## Root cause — file:line

Raised at `trilogy/core/query_processor.py:150` in `get_datasource_cte` (nested in `base_join_to_join`), reached from `datasource_to_cte` (`:421`) → `base_join_to_join` (`:167`, resolving a join's `existing_datasource`/`right_datasource`) → `process_query` (`:1229`).

`base_join_to_join` matches a join operand to an already-built parent CTE **by `datasource.identifier` string equality** (`:141`/`:147`). The lookup fails because the same logical join-datasource has two different identifiers.

The real defect is in `QueryDatasource._compute_identifier`, **`trilogy/core/models/execute.py:1300-1305`**:

```python
return (
    "_join_".join([d.identifier for d in self.datasources])   # <-- LIST ORDER, not sorted
    + group
    + (f"_at_{grain}" if grain else "_at_abstract")
    + (f"_filtered_by_{filters}" if filters else "")
)
```

The grain (`:1291-1293`) and filter (`:1290`) suffixes are canonicalized (sorted / hashed), but the member-datasource join prefix is **not sorted** — it is emitted in `self.datasources` list order. When `ss.item.items` participates in the qualifying semijoin (which only happens once `product_name` pulls `ss.item.items` into that join's grain), one construction path assembles the operand list as `[ss.item.items, cr…qualifying…]` and another as `[cr…qualifying…, ss.item.items]`. The two yield identifiers `ss.item.items_join_cr…` vs `cr…_join_ss.item.items…` with an **identical `_at_ss_item_sk_filtered_by_8610126078781587` suffix** — same logical datasource, different string. `get_datasource_cte` therefore can't find the built CTE and raises.

In the error dump the eligible set contains `cr…qualifying…_join_ss.item.items_at_ss_item_sk_filtered_by_8610126078781587` (operands B,A); the join asks for `ss.item.items_join_cr…qualifying…_at_ss_item_sk_filtered_by_8610126078781587` (operands A,B) — the swap is visible directly.

## Fix direction (NOT applied)

Canonicalize the member-datasource ordering when composing the join-datasource identifier (e.g. sort `[d.identifier for d in self.datasources]` in `_compute_identifier`, execute.py:1301) so `A_join_B` and `B_join_A` collapse to one identity — mirroring how grain/filter are already order-normalized. Alternatively make `get_datasource_cte` match on an order-independent key (set of member identifiers + grain + filter). Verify no renderer relies on join operand order in the identifier (union arm identifiers at `:1289` intentionally preserve order for EXCEPT — keep that path untouched).

## Canonical status

`tests/modeling/tpc_ds_duckdb/query64.preql` builds cleanly (`generate_sql` → 1 statement). It sidesteps the bug via a `rowset cs_ui` + single-anchor structure that never puts `ss.item.items` inside the semijoin join in two different orders.
