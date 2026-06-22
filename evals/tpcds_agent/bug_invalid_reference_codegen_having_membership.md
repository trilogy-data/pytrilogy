# Bug: "Invalid reference string" codegen crash (dangling CTE) on membership-in-HAVING + window

**Status:** FIXED 2026-06-19 (v3); FIXED 2026-06-22 (v4 discovery) (found 2026-06-06)

## v4 discovery fix (2026-06-22)

The v3 fix never reached the v4 planner, so under `CONFIG.use_v4_discovery` the
membership-in-HAVING tests regressed (`test_membership_in_having_*`, untracked).
Two v4-only gaps, both mirroring existing v3 behavior:

1. `query_processor._get_query_node_v4` applied the HAVING by wrapping a
   `SelectNode` but never called `append_existence_check` — so the HAVING
   subselect rendered a dangling CTE. Added the same idempotent call the v3 path
   uses.
2. A *projected* membership flag (`--x in set as flag`, a BASIC concept whose
   lineage is a `BuildSubselectComparison`) had its existence set dropped:
   `strategy_builder._group_existence_concepts` only collected existence args
   from injected WHERE atoms and `BuildFilterItem` lineage. Added a
   `BuildConceptArgs` branch (mirrors v3 `gen_basic_node`) so the set is wired as
   the group's existence parent.

## Fix (2026-06-19)

Root cause: `query_processor.get_query_node` (v3 path) applies the top-level HAVING AFTER discovery
(`add_condition`/wrapper) and never sourced the predicate's existence (`x in <set>`) args — unlike
the WHERE path, which routes them through `append_existence_check` inside discovery. So the node
carrying the HAVING had no `existence_source_map` entry for the set, and its subselect rendered a
dangling CTE reference -> `INVALID_REFERENCE_BUG`.

This interacted with two sibling bugs and was fixed in three coordinated steps:
1. **Classification** (`bug_membership_in_having_misclassified.md`): `_substitute_having_aggregates`
   was downgrading the HAVING `SubselectComparison` to a plain `Comparison`, so it had no existence
   args at all. Fixed by preserving the concrete class (`type(node)`).
2. **Existence sourcing** (this bug): after applying the HAVING (fold onto a Merge/Select node, else
   wrap), call `append_existence_check(ds, build_environment, graph, having_clause, history)` —
   the same helper the WHERE path uses — to source the set onto whichever node carries the
   predicate. Made `append_existence_check` idempotent (skip if the set is already in
   `input_concepts` OR `existence_concepts`) so it is safe to call uniformly.
3. This replaced an earlier narrow special-case (folding onto a `WhereSafetyNode` that happened to
   already carry the set). The general helper subsumes it and also fixes the auto-concept /
   GroupNode shapes the narrow fix missed (e.g. `auto set <- …; select x as a … having a in set`).

Regression tests in `test_non_benchmark_queries.py`:
`test_membership_in_having_over_window_renders_valid_subselect` (CTE-handle/WhereSafetyNode shape) and
`test_membership_in_having_auto_concept_renders_valid_subselect` (auto-concept/GroupNode shape).

Still open: the rowset + window + filtered-aggregate shape degrades to a DIFFERENT discovery-side
crash (`['local.ws']`) that fires BEFORE the HAVING block — tracked in
`bug_membership_in_having_hidden_flag_discovery_crash.md` (#3). It needs the validator to stop
forcing the membership into the projection (so no hidden flag), which is the next step.

---

**Original status:** OPEN (found 2026-06-06)
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
