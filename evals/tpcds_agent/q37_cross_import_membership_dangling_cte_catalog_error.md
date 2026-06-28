# Bug: cross-import membership `a.item.id in b.item.id` (two imports of the same dim) emits a dangling CTE → DuckDB CatalogException "Table cs_item_items does not exist" (q37)

**Status:** FIXED 2026-06-27 — `_check_parent` existence-source promotion in `predicate_pushdown.py`
now propagates an already-**inlined** datasource (not just regular dependency CTEs) onto the target
base CTE, so the pushed membership subselect renders `from items as cs_item_items` instead of a phantom
`from cs_item_items`. Bails cleanly (no push) if a source name resolves to neither a dependency nor an
inlined datasource. Regression test: `tests/optimization/test_cross_import_membership_inlined_existence.py`.
Originally OPEN — deterministic minimal repro + clean bisection. Surfaced in the full-99 rebaseline.
**Surfaced by:** TPC-DS q37 (run `20260627-181845`). `generate_sql` succeeds; **execute** throws an
uncaught `(_duckdb.CatalogException)` → `Unexpected error`, no clean Trilogy message.
**Severity:** HIGH — invalid SQL DuckDB rejects (references a table/CTE that was never emitted).

## Symptom

```
(_duckdb.CatalogException) Catalog Error: Table with name cs_item_items does not exist!
Did you mean "item"?
LINE 10: ..."inv_item_sk" in (select cs_item_items."cs_item_id" from cs_item_items where cs_item_items."cs_item_id" is not null)
```

The existence/membership subquery is rendered as `... in (select cs_item_items."cs_item_id" from
cs_item_items ...)`, but **no CTE named `cs_item_items` is ever defined** in the WITH block — the
membership-set CTE is dropped, leaving a dangling reference.

## Trigger (bisected)

Two imports of the **same underlying dimension** (`item`) under different aliases, then a WHERE
membership that compares one import's `item.id` against the other's `item.id`:

```trilogy
import raw.inventory as inv;
import raw.catalog_sales as cs;

where
    inv.item.current_price between 68 and 98
    and inv.item.manufacturer_id in (677, 940, 694, 808)
    and inv.quantity_on_hand between 100 and 500
    and inv.date.date between '2000-02-01'::date and '2000-04-01'::date
    and inv.item.id in cs.item.id          -- <-- membership across the two item imports
select inv.item.text_id as item_code, inv.item.desc as description, inv.item.current_price
order by item_code limit 100;
```

Both `inv.item` and `cs.item` resolve to the same physical `item` table; the membership set
(`cs.item.id` reachable only through `catalog_sales`) needs its own CTE but the planner names it
`cs_item_items` and then fails to materialize it. The leaf dim collides with the already-present
`item` CTE.

## Likely fix area

The existence-set sourcing for a cross-import membership where the set concept is a **shared
dimension key reachable through a different fact import**. Same neighborhood as the existence-check
machinery touched in the FIXED q2/q64 cross-rowset cases
([[project_q2_expr_join_filtered_div_membership_invalid_ref]],
[[project_q64_membership_in_cross_rowset_join_set]]) — `append_existence_check` / existence sourcing.
The membership CTE for `cs.item.id` must actually be emitted (and aliased consistently) before the
outer query references it; right now the reference survives but the CTE is pruned. At minimum, raise
a clean error instead of emitting SQL that names a non-existent table.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-181845/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
q = '''import raw.inventory as inv;
import raw.catalog_sales as cs;
where inv.item.current_price between 68 and 98
  and inv.item.manufacturer_id in (677, 940, 694, 808)
  and inv.quantity_on_hand between 100 and 500
  and inv.date.date between '2000-02-01'::date and '2000-04-01'::date
  and inv.item.id in cs.item.id
select inv.item.text_id as item_code, inv.item.desc as description, inv.item.current_price
order by item_code limit 100;'''
sql = eng.generate_sql(q)[-1]   # succeeds
eng.execute_raw_sql(sql)        # CatalogException: Table cs_item_items does not exist
```
