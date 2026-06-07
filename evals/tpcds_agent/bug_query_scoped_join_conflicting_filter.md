# Bug: query-scoped join across conflicting WHERE filters → planner `Have {…}` crash

**Status:** FIXED 2026-06-06 (found same day, enriched eval q64).

## Resolution

A rowset is a self-contained query scope: its own WHERE stays inside it and is
materialized as a CTE that's joined into the outer query (verified against
existing behavior — a `year=2000` rowset joined into a no-WHERE outer query
keeps the rowset filtered and the outer scan *unfiltered*; the rowset's filter
does not leak out, and by symmetry the outer's filter does not leak in). The
crash was purely a discovery-validation bug: the condition-completion check
demanded that *every* node in the stack carry (or imply) the outer WHERE, but a
rowset node legitimately carries its own, different (or `None`) condition. This
also broke *non*-conflicting outer filters (e.g. an outer `store=1` over a
`year=2000` rowset) — any outer WHERE differing from the rowset's crashed.

Fix: a new `_is_independent_scope(node)` helper (node whose visible outputs are
all `Derivation.ROWSET`) in `discovery_validation.py`, added as an exemption
alongside `_is_scalar_only` in `_conditions_met` / `_condition_atom_met`, and in
`generate_loop_completion`'s `condition_required` gate
(`concept_strategies_v3.py`) so the outer condition isn't wrongly re-applied to
the merge. The repro now compiles to two independently-filtered base scans
(rowset `year=2000`, outer `year=1999`) joined by item — a meaningful
year-over-year result, not empty.

Regression tests: `tests/engine/test_duckdb_rowset.py::test_rowset_query_scoped_join_conflicting_filter`
(end-to-end, asserts correct non-empty results) and
`tests/core/processing/test_discovery_validation.py` (`TestIsIndependentScope`,
`TestConditionsMetIndependentScope`).

## Original report (for context)

**Was:** OPEN (found 2026-06-06, enriched eval q64).
**Severity:** high — a natural year-over-year self-join phrasing crashes with an opaque internal
planner assertion, not a user-actionable error. Top token/iteration sink on q64 (58 calls).

## Symptom

```
SyntaxError: Have {'GroupNode<yr2000.cnt_2000,yr2000.item_id>': store_sales.date.year = 2000,
                   'GroupNode<local.cnt_1999,store_sales.item.product_name>': store_sales.date.year = 1999}
             and need store_sales.date.year = 1999
```

The planner ends up with two candidate `GroupNode`s carrying *different* WHERE conditions
(`year = 2000` from the joined rowset, `year = 1999` from the main query) and cannot decide which
satisfies the "needed" condition. It raises the internal `Have {…} … need …` assertion instead of
either resolving the scopes independently or rejecting the query with a clear message.

## Deterministic reproduction (checked-in enriched model)

Model: `tests/modeling/tpc_ds_duckdb` (`physical_sales.preql`). No data needed — fails at SQL
generation. Driver:

```python
from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment
env = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
eng = Dialects.DUCK_DB.default_executor(environment=env)
eng.generate_sql(Path("repro.preql").read_text())[-1]
```

### Minimal repro (`repro.preql`)
```trilogy
import physical_sales as store_sales;

rowset yr2000 <-
    where store_sales.date.year = 2000
    select
        store_sales.item.id as item_id,
        count(store_sales.line_item) as cnt_2000;

where store_sales.date.year = 1999
inner join yr2000.item_id = store_sales.item.id
select
    store_sales.item.id as iid,
    count(store_sales.line_item) as cnt_1999,
    yr2000.cnt_2000 as cnt_2000
limit 100;
```

## Minimization findings

The crash needs exactly two things; peel either and it compiles:

- **A query-scoped `inner join` to a rowset** (`inner join yr2000.item_id = store_sales.item.id`).
- **Conflicting WHERE filters on the same dimension across the two scopes** — rowset filters
  `year = 2000`, main query filters `year = 1999`. Make both sides the SAME year → **compiles OK**.

NOT required (verified — crash persists without them):
- `having` (crashes with and without it; the original q64 had `having cnt_2000 <= cnt_1999`).
- A second join key.

So the trigger is: **a query-scoped join whose two operand scopes carry contradictory
single-dimension WHERE filters.** The planner appears to unify the two scopes' conditions into one
"needed" set instead of keeping each join operand's filter local to its own subquery.

## Provenance

Enriched eval q64 (store sales: items whose count in 1999 ≥ count in 2000, by item/store, with a
catalog-refund qualifying filter). The agent's natural phrasing — aggregate one year in a `rowset`,
join it to the other year in the main query — hits this. The agent then thrashed across ~17
rebuilds (rowset → query-scoped join → aligned multi-select → …) trying to escape it. The original
crashing query (full version) is preserved at
`evals/tpcds_agent/results/20260606-222624/workspace/` (the `query64_test*.preql` attempts +
`raw/` model + `tpcds.duckdb`).

## Related codegen bug in the SAME query (separate, also OPEN): invalid GROUP-BY SQL

When the agent instead expressed q64 as an **aligned multi-select** (the now-supported
`align … derive … having` form), it generated SQL DuckDB rejects:

```
BinderException: column "product_name" must appear in the GROUP BY clause …
GROUP BY 1,2,3,4,5
ORDER BY "kaput"."product_name", "macho"."cnt_2", "macho"."sum_wc_2"   -- inner-CTE cols, not grouped
```

Root cause: the `derive` output **reuses an arm column's name**
(`coalesce(product_name, product_name_2) as product_name`, `… as cnt_2`), so `order by product_name`
binds to the *arm's* concept (rendered inner-CTE-qualified, e.g. `kaput.product_name`) rather than
the derived output alias — and that inner column is absent from the `having`-induced outer
`GROUP BY`. This is the same family as the order-by-derived binding bug fixed in
`multiselect_rules.py` (registration order), but here the **name collision** re-routes the binding
to the wrong concept. Two avenues for the fix:
- the multi-select should reject (or rename) a `derive` output that collides with an arm output
  address, OR
- the outer GROUP BY should not be emitted for a `having` with no aggregates (see the separate
  multiselect-`having`-forces-GROUP-BY investigation), which also removes the violated constraint.

Reproduce with the full q64 attempt body in the workspace above (the `align … derive
coalesce(product_name, product_name_2) as product_name … having cnt_2 <= cnt_1` variant).

## Not a bug (for context)

The agent also hit `Could not resolve connections for query with output [...] models that are not
connected in the current graph: cat_returns … merge their keys` — that message is **correct and
actionable** (it aggregated `cat_returns` without a `merge` to the rest). Left as-is.
