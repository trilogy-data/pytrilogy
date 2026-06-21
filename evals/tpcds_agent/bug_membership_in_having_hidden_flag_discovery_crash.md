# Bug #3: HAVING-membership hidden flag crashes discovery (`['local.ws']` missing parent)

**Status:** FIXED 2026-06-19
**Severity:** medium — a valid-looking query raises an internal `ValueError` during planning
instead of running.
**Area:** `concept_strategies_v3` discovery (`generate_loop_completion`) + `GroupNode` /
`base_node.validate_inputs`.

## Fix (2026-06-19)

The crash only happened because the validator forced the membership to be projected as a hidden
`--… as flag`, and building THAT flag node in the windowed/filtered-agg shape is what blew up
discovery. With #2 (classification) and #1 (existence sourcing) already in, the flag is unnecessary:
relaxed `select_finalize._validate_syntax` to check `select.having_clause.row_arguments` instead of
`concept_arguments`, so a membership's existence RHS (the set) no longer must appear in the
projection — only the row side (the LHS). With the `--… as flag` line dropped, discovery never
builds the offending node and the membership renders via the #1 path. The LHS and plain
(non-membership) HAVING refs are still required in the projection (row_arguments surfaces them).

Render regression test:
`test_non_benchmark_queries.py::test_membership_in_having_no_projected_flag_renders_valid_subselect`.
Validator tests: `test_parse_engine_v2.py::test_having_membership_does_not_require_set_in_projection`
and `::test_having_membership_still_requires_row_side_in_projection`.

## Symptom

```
ValueError: Invalid input concepts to node! ['local.ws'] are missing non-hidden parent nodes;
have {...} and hidden {'local.ws'} from root {...}
```

Raised at `trilogy/core/processing/nodes/base_node.py:244` (`validate_inputs`). Crucially this
fires at `query_processor.get_query_node:683` (`source_query_concepts`) — i.e. inside DISCOVERY,
**before** the HAVING block (~line 695) is ever reached. Filtered traceback:

```
query_processor.py:683 get_query_node -> source_query_concepts
  ... search_concepts recursion ...
  group_node.py:642 gen_group_node
  group_node.py:328 _source_parent_concepts
  concept_strategies_v3.py:461 generate_loop_completion
  group_node.py:54 __init__
  base_node.py:244 validate_inputs   # raises
```

## Minimal reproduction

Needs the enriched TPC-DS model (`tests/modeling/tpc_ds_duckdb`). No data required.

```trilogy
import all_sales as all_sales;

rowset ws_2001 <- select all_sales.date.week_seq
    where all_sales.channel != 'STORE' and all_sales.date.year = 2001;
auto wk_sun <- sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0);

select
    all_sales.date.week_seq as ws,
    --ws in ws_2001.all_sales.date.week_seq as in_2001,
    wk_sun as sun,
    lead(wk_sun, 53) over (order by all_sales.date.week_seq) as next_sun
having ws in ws_2001.all_sales.date.week_seq
;
```

Here the HAVING IS classified as a `SubselectComparison` (rowset-handle RHS, unlike bug #2), so
this is a distinct failure mode.

## Minimization notes (each element is load-bearing — drop one → different/clean outcome)

- `lead(...)` window — required (drop → `INVALID_REFERENCE_BUG`)
- filtered aggregate `sum(... ? ...)` — required (plain `sum` → `INVALID_REFERENCE_BUG`)
- rowset's own `channel != 'STORE'` filter — required (year-only → `INVALID_REFERENCE_BUG`)
- output alias `... as ws` referenced in HAVING — required (reference `all_sales.date.week_seq`
  directly → `INVALID_REFERENCE_BUG`)
- hidden `--... as in_2001` flag — required to pass the HAVING-in-projection validator
- the WHERE-membership — NOT needed

## Likely cause

The hidden projected membership flag (`--ws in ws_2001... as in_2001`) is forced into the SELECT
output only to satisfy `_validate_syntax` (HAVING refs must be projected). In the
windowed + filtered-aggregate shape, a group-completion node (`generate_loop_completion`) is built
whose inputs include the aliased output `ws` (`local.ws`) but it arrives HIDDEN, so
`validate_inputs` rejects it. This is a discovery-side interaction between the hidden
derived-membership flag and the group-completion grain, not the HAVING application itself.

## Dependency on bugs #1 + #2 (both now LANDED 2026-06-19)

The hidden flag exists ONLY because the validator forces the HAVING membership into the projection.

- #2 (`bug_membership_in_having_misclassified.md`) — DONE: membership now classifies as
  `SubselectComparison` in HAVING.
- #1 (`bug_invalid_reference_codegen_having_membership.md`) — DONE: HAVING existence args are now
  sourced via `append_existence_check`, so a membership-in-HAVING that reaches the HAVING block
  renders a valid subselect (auto-concept and CTE-handle shapes verified).

This repro still crashes because it fails in DISCOVERY (building the hidden-flag output node)
BEFORE the HAVING block — #1/#2 don't touch that path. The remaining fix is to stop forcing the
membership into the projection: relax `_validate_syntax` (HAVING projection check) to use
`row_arguments` instead of `concept_arguments`, so an existence RHS need not be projected (mirroring
WHERE). With no hidden `--… as flag` required, discovery never builds the offending group-completion
and this crash dissolves. That validator change was deliberately deferred until #1 landed (relaxing
earlier would have turned a clean "add it to the projection" error into a render crash); #1 is now
in, so this is the active next step. Re-check this exact repro (drop the `--… as in_2001` line)
after the validator change.
