# Bug B1: Planner recursion crash on rank/window over a projected aggregate-ratio

**Status:** OPEN (found 2026-06-06)
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
</content>
