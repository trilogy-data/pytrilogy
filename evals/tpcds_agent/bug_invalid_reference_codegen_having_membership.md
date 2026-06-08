# Bug: "Invalid reference string" codegen crash (dangling CTE) on membership-in-HAVING + window

**Status:** OPEN (found 2026-06-06)
**Severity:** medium — an internal invariant fires ("this should never occur"); a valid-looking
query crashes at SQL render instead of being cleanly rejected or run.
**Area:** `trilogy/dialect/base.py` (compile_statement, ~line 2315) — generated SQL references a
CTE that was never defined.

## Symptom

```
ValueError: Invalid reference string found in query:
WITH
cheerful as ( SELECT "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as ... ),
...
, this should never occur. Please report this issue.
```

Raised at `trilogy/dialect/base.py:2315` during `compile_statement`. The rendered SQL contains a
`FROM`/`JOIN` reference to a CTE name (the random-word CTE labels, e.g. `thoughtful`, `vacuous`,
`cheerful`) that is not present in the `WITH` list — a dangling reference. DuckDB would also reject
it (`Binder Error: Referenced table "thoughtful" not found`) when run via the CLI.

## Deterministic reproduction

Needs the enriched TPC-DS model (`tests/modeling/tpc_ds_duckdb`, which has `all_sales.preql`).
No data required — it fails at SQL generation, before execution.

```trilogy
import all_sales as all_sales;

rowset ws_2001 <- select all_sales.date.week_seq
    where all_sales.sales_channel != 'STORE' and all_sales.date.year = 2001;
auto wk_sun <- sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0);

select
    all_sales.date.week_seq as ws,
    --ws in ws_2001.all_sales.date.week_seq as in_2001,
    wk_sun as sun,
    lead(wk_sun, 53) over (order by all_sales.date.week_seq) as next_sun
where all_sales.sales_channel != 'STORE'
    and (all_sales.date.week_seq in ws_2001.all_sales.date.week_seq
         or all_sales.date.week_seq - 53 in ws_2001.all_sales.date.week_seq)
having ws in ws_2001.all_sales.date.week_seq
order by ws nulls first
limit 55;
```

Driver:
```python
from pathlib import Path
from trilogy.core.query_processor import process_query
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parsing.parse_engine_v2 import parse_text
ROOT = Path("tests/modeling/tpc_ds_duckdb")
env, parsed = parse_text(Path("repro.preql").read_text(), root=ROOT)
DuckDBDialect().compile_statement(process_query(env, parsed[-1]))   # -> ValueError
```

## Minimization notes

The crash needs the FULL combination — peeling any piece off degrades it to a *clean* error
instead of the invalid-SQL crash:
- drop the `having ws in ws_2001...` membership → resolves OK.
- drop the `lead(...)` window / `wk_sun` aggregate / the channel filter → `UnresolvableQueryException`
  or `InvalidSyntaxException: HAVING references 'ws_2001...'` (a clean rejection).

So the trigger is roughly: a `lead` window over a derived aggregate, PLUS a rowset-membership in
WHERE, PLUS a rowset-membership in HAVING (and a hidden projected-membership field), over the
`all_sales` partial-datasource model.

## Likely cause

`x in <rowset>` used in HAVING is a known-unsupported pattern (see the memory note
`project_membership_in_having_unsupported` — "membership in HAVING collapses/errors; only top-level
WHERE works"). Normally that is rejected up front. In this specific combination the HAVING
membership is NOT rejected and instead survives into SQL rendering, where its subselect CTE is
referenced but never emitted — tripping the `base.py:2315` invariant. The fix is either to reject
membership-in-HAVING consistently in this shape, or to render its CTE.

## Provenance

Surfaced by the TPC-DS agent eval, query 02 (enriched leg), where an agent reached for this
lead/lag weekly-sales shape and put the rowset membership in HAVING. The crash burned its whole
iteration budget retrying (one of the top token outliers in the 99-query baseline). It is NOT
related to the in-query JOIN work — the query uses no scoped join and reproduces independently.
