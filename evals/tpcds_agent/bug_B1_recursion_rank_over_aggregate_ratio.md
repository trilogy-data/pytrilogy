# Bug B1: Planner recursion crash on rank/window over a projected aggregate-ratio

**Status:** FIXED (2026-06-07).
- Shape 1 (rank/ratio, build-time RecursionError) was already resolved by the
  `_abstract_resolution_grain` fix in `build.py`; the select grain now uses the
  window keys (item, sales_channel). Regression:
  `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_rank_over_projected_aggregate_ratio_no_recursion`.
- Shape 2 (q77 filtered-aggregate difference, discovery-time RecursionError) is a
  *different* recursion — in node discovery, not in `__build_concept`. Root cause:
  the named flag `f1` is both a derived row-argument of `where (f1 or f2)` and a
  concept that must be built (the `?` filter operand). The loop forces the
  condition to be pushed into a complex input (`net_profit ? f1`) that itself
  depends on the flag `f1`; sourcing it re-introduces `f1` with the condition
  still attached (`initialize_loop_context`), re-forcing evaluation — a loop.
  Fix (stateless/local) in `discovery_utility.get_loop_iteration_targets`: gate
  `force_pushdown_to_complex_input` so a complex pushdown target only counts when
  it does NOT transitively depend on a DERIVED row-argument of the condition. You
  can't push a condition into something that needs the condition's own derived
  input; that input is built first and the condition applied above (over the
  joined rows). Raw-column row-args still get normal datasource pushdown. Verified
  the `(f1 or f2)` WHERE survives in the rendered SQL and the query executes
  end-to-end. Regression:
  `...::test_or_filter_over_differently_filtered_aggregates_no_recursion`.

**Status (original):** OPEN (found 2026-06-06)
**Severity:** high — an opaque `RecursionError` (Python stack overflow), NOT a clean rejection.
A natural "rank items by a ratio of two aggregates" query crashes at build time. Burned the
whole iteration budget on enriched q44 and q49 (two of the top token outliers in the 99-query
baseline).
**Area:** `trilogy/core/models/build.py` — `Factory._build_concept` / `__build_concept`
(the wrapper that re-raises at line ~2523).

## Symptom

```
Unexpected error: Recursion error building concept local.return_quantity_ratio with grain
Grain<s.item.id,s.order_id,s.sales_channel> and lineage
divide(sum(ref:s.return_quantity)<abstract>,sum(ref:s.quantity)<abstract>).
This is likely due to a circular reference.
```

Raised from `build.py:2523`, which wraps a Python `RecursionError` thrown inside
`__build_concept`. There is *no* actual user-written self-reference — this is a planner cycle,
distinct from the clean `"SELECT output X is defined by an expression that references X itself"`
rejection (see memory `reference_planner_keys_by_address`).

## Deterministic reproduction (checked-in enriched model)

Model: `tests/modeling/tpc_ds_duckdb` (needs `all_sales.preql`). No data — fails at build.

Driver:
```python
from pathlib import Path
from trilogy.core.query_processor import process_query
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parsing.parse_engine_v2 import parse_text
ROOT = Path("tests/modeling/tpc_ds_duckdb")
text = Path("repro.preql").read_text()
env, parsed = parse_text(text, root=ROOT)
DuckDBDialect().compile_statement(process_query(env, parsed[-1]))   # -> RecursionError
```

### Minimal repro (`repro.preql`) — single rank, isolates the trigger
```trilogy
import all_sales as s;
where s.return_amount > 10000
select
    s.item.id as item,
    sum(s.return_quantity) / sum(s.quantity) as return_quantity_ratio,
    rank(s.item.id) over (partition by s.sales_channel order by sum(s.return_quantity) / sum(s.quantity) asc) as rank_a
limit 100;
```

## Minimization findings — the trigger is a specific 3-way combination

All three must be present (peeling any one yields a clean compile):

1. **A projected aggregate ratio** `sum(a)/sum(b) as r` in the SELECT.
2. **A `rank()` (window) whose `order by` is the *same* `sum(a)/sum(b)` expression** — i.e. an
   expression address-identical to the projected ratio (planner keys concepts by address →
   they collapse to one node).
3. **The window's `partition by` column is NOT in the SELECT projection** (here
   `partition by s.sales_channel` with no `s.sales_channel` output).
4. A top-level `where` is present.

Empirically confirmed:
- Drop the `partition by` → compiles OK (`M4`).
- **Project the partition column** (`s.sales_channel as channel`) → compiles OK (`MIN6`).
  This is the strongest clue: when the partition key is independently materialized as an
  output, the cycle disappears.
- Drop the projected ratio (keep only the rank) → compiles OK (`M5`/`M1`).
- Drop the `where` → compiles OK (`MIN`).

The original q49 form (two ranks + `having rank<=10 or rank<=10`) also crashes; the second rank
and HAVING are *not* required — they were just how the agent first hit it.

## Likely cause / where to look

`__build_concept` recurses while resolving the window: the `order by sum(a)/sum(b)` builds the
same `return_quantity_ratio` node that the projection is building, and the unmaterialized
partition key sends the resolver back through the same address. There is already a
`self._building` address stack maintained around `__build_concept` (build.py ~2517) — it can be
used to detect the cycle and either reuse the in-progress/already-built node or raise the clean
self-reference rejection instead of overflowing the stack.

## Provenance

TPC-DS agent eval, enriched leg, q44 and q49 (top token outliers). q49 eventually *passed*
after the agent rewrote each ratio operand as a distinct named `by`-grained `auto` (distinct
addresses → no collapse); q44 also eventually produced output. Pure budget burn, no wrong
answer. The companion ingest-leg crash for the same shape is a *different* failure (SQL-render
`INVALID_REFERENCE_BUG`) — see `bug_B2_invalid_reference_codegen.md`.

---

## NEW confirmed trigger shape (2026-06-07, enriched q77) — FIXED 2026-06-07

A second surface that hits the same `RecursionError` — **not** rank/ratio, so the B1 fix (if/when
landed) must cover it too. Trigger: **a derived measure that is a *difference of two inline-`?`-
filtered aggregates with DIFFERENT filter flags*, aggregated, while a `where` clause references
those same flags.**

### Minimal repro — REPRODUCES on the checked-in enriched model (`tests/modeling/tpc_ds_duckdb`)
```trilogy
import all_sales as sales;
auto f1 <- sales.date.date between '2000-08-23'::date and '2000-09-22'::date;
auto f2 <- sales.return_date.date between '2000-08-23'::date and '2000-09-22'::date;
auto m <- coalesce(sales.net_profit ? f1, 0) - coalesce(sales.return_net_loss ? f2, 0);
where sales.channel_dim_id is not null and (f1 or f2)
select sales.channel_dim_id, sum(m::numeric(15,2)) as t
limit 100;
```
→ `RecursionError` at `generate_sql` (same opaque Python stack overflow as the rank/ratio shape).

### Minimization (which pieces are load-bearing)
- **Different filter flags** on the two operands (`? f1` vs `? f2`) — using the SAME flag for both
  → compiles OK.
- **A `where` referencing those flags** (`where … (f1 or f2)`) — dropping the `where` → compiles OK.
- `by rollup` and the `def`-macro wrapper are NOT required (drop either → still crashes); they were
  just how the q77 agent first hit it.

So the cycle is the same root (an aggregate whose lineage nests other aggregates that share
addresses with a WHERE-scoped filter), surfacing through filtered-aggregate *differences* rather
than ratios. The `self._building` address-stack guard proposed above should resolve both shapes —
verify against this repro as well as the rank/ratio one.

Provenance: enriched q77 (multi-channel period sales/returns/profit rollup); the agent's natural
`period_profit <- coalesce(net_profit ? sale_in_period,0) - coalesce(return_net_loss ?
return_in_period,0)` definition hit this. 2.45M tokens.
</content>
