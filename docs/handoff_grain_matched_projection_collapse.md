# A rename-only CTE survives optimization: grain-matched aggregates block the collapse

## Status: INVESTIGATED 2026-09-28, no change made. Pick up AFTER the current round.

Another session is changing the FINAL merge's GROUP BY
(`rows_unique_at_outputs` in `grain_utility.py`, `merge_node.py`,
`tests/engine/test_projected_row_identity.py`); it was uncommitted when this
was written. That work changes adhoc04's final CTE, so re-run the reproduction
below on top of it before changing anything; the CTE names and the final
`GROUP BY` will differ from what this doc quotes.

## Symptom

`tests/modeling/thelook_duckdb/adhoc04.preql` compiles to a CTE that only
renames its single parent's columns and still survives optimization:

```sql
questionable as (
SELECT
    "cooperative"."id" as "id",
    "cooperative"."item_margin" as "margin",
    "cooperative"."order_id" as "order_id",
    "cooperative"."order_line_revenue" as "total_order_revenue",
    "cooperative"."product_id" as "product_id",
    "cooperative"."sale_price" as "revenue",
    "cooperative"."user_id" as "user_id"
FROM
    "cooperative")
```

`cooperative` has no other consumer, and `questionable` has no WHERE, join,
GROUP BY or limit. Folding it into `cooperative` is exactly what
`CollapseSingleParent` exists for.

## Reproduce

```bash
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/thelook_duckdb/adhoc04.preql \
  --rows --setup tests.modeling.thelook_duckdb.db_build:seed --open
```

In the viewer, the "CTEs after optimization" step (step 39 here) shows
`questionable` tagged with its group, `aggregate:d0:local.id|order.id:input:local.id`.
The removed-CTE tombstones below it show what the optimizer did remove, and
`questionable` is not among them.

## Root cause

`CollapseSingleParent.optimize`
(`trilogy/core/optimizations/collapse_single_parent.py` ~line 436) returns
immediately, with no log line, when `get_merge_mode(cte)` (~line 219) is
`None`. For `questionable` it is `None` because:

- **Not AGGREGATE**: `group_to_grain` is False and `source.source_type` is
  `SELECT`, not `GROUP`.
- **Not BASIC**: no output column has `Derivation.BASIC`.
- **Not PASSTHROUGH**: `is_projection_shape` (~line 62) rejects any output
  column with no `source_map` entry whose derivation is AGGREGATE (or whose
  lineage is an aggregate/window wrapper). `revenue = sum(sale_price)`,
  `margin = sum(item_margin)` and
  `total_order_revenue = sum(order_line_revenue)` are all AGGREGATE concepts
  rendered locally.

The disqualifier assumes a locally rendered aggregate is a real aggregation.
In a CTE that does not group, it is not. `BaseDialect.render_expr`
(`trilogy/dialect/base.py` ~line 2364) renders an aggregate function with
`FUNCTION_MAP` only when `cte.group_to_grain`; otherwise it uses
`FUNCTION_GRAIN_MATCH_MAP`, whose aggregate entries (`AGGREGATE_GRAIN_MATCH_MAP`,
~line 731) are the single-row forms: `sum(x) -> x`,
`count(x) -> CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END`, etc. The planner put
the aggregate at the grain its input already has (`local.id`), so the group
node groups nothing. The resulting CTE is a scalar row projection:
`sum(sale_price)` renders as `sale_price`, which is why the SQL above is pure
renames.

The upstream half: the planner emits a group node for aggregates whose grain
equals their input's grain, and it resolves to a non-grouping CTE. That is
correct (no GROUP BY is needed); the optimizer just doesn't recognize what it
produced.

## Experiment (validated, not landed)

Classify a non-grouping CTE whose locally rendered columns are aggregates as
BASIC. That is the scalar-projection mode the rule already folds, including
into MERGE parents. Monkeypatched in a script:

```python
from trilogy.core.enums import Derivation, SourceType
from trilogy.core.optimizations import collapse_single_parent as csp

orig = csp.get_merge_mode

def patched(cte):
    mode = orig(cte)
    if (
        mode is None
        and not cte.group_to_grain
        and cte.source.source_type != SourceType.GROUP
        and any(
            c.derivation == Derivation.AGGREGATE and not cte.source_map.get(c.address)
            for c in cte.output_columns
        )
    ):
        return csp.MergeMode.BASIC
    return mode

csp.get_merge_mode = patched
```

Result on adhoc04, executed against the seeded thelook data
(`tests.modeling.thelook_duckdb.db_build:seed`; set `executor.environment` to
the statement's environment before `generate_sql`):

- `questionable` folds into `cooperative`, which now renders
  `"thoughtful"."sale_price" as "revenue"`,
  `"thoughtful"."sale_price" - "thoughtful"."product_cost" as "margin"` and
  `"highfalutin"."order_line_revenue" as "total_order_revenue"`.
- The final SELECT reads `cooperative` directly.
- **Rows are identical: 940 rows, same md5 of the sorted rows with and
  without the patch.**

## Why the fold should be sound

- The child and parent have the same rows: BASIC folds only a projection with
  no WHERE (a conditioned BASIC child is rejected unless it is the statement's
  final filtered projection), no join and no regroup. The parent is not
  grouping either, so every aggregate still renders in its single-row form
  there.
- **A GROUP parent is already refused.** For a BASIC child,
  `basic_fold_into_group_is_safe` (~line 289) rejects any AGGREGATE-derivation
  or aggregate-lineage output the parent doesn't already expose.
- **A later aggregate merge into the widened parent is already refused**
  ("Parent ... renders inline aggregate", ~line 607 via
  `lineage_contains_aggregate`), so the folded columns can't end up nested
  inside a real `sum(...)`.
- `parent_is_ineligible` keeps BASIC out of WINDOW / SUBSELECT / UNNEST
  parents.

Worth checking explicitly, since the guards were written for BASIC columns,
not grain-matched aggregates:
- **grouping-sets/rollup aggregates**: `child_has_merge_blockers` covers
  nonstandard grouping only in AGGREGATE mode, so check
  `has_nonstandard_aggregate_grouping` for BASIC too, or exclude those columns
  from the new branch;
- aggregates with `by` or a filter (`sum(x ? cond)`): confirm the
  single-row render is what the child emitted;
- `passthrough_only` mode (`merge_aggregate` off; `optimization.py` ~line 427)
  returns early for BASIC, so the new branch is inert there. Probably fine, but
  a pure-rename CTE is arguably PASSTHROUGH-worthy even then.

## Where to fix

Two options:

1. **Optimizer (the experiment):** extend `get_merge_mode`, or better,
   `is_projection_shape`'s disqualifier, so an aggregate column counts as local
   computation only when the CTE groups. Small, local, and measurable. Also
   log the `merge_mode is None` exit, so the next rejection is visible
   (today nothing records why a CTE was skipped).
2. **Planner:** don't emit a group node for an aggregate whose grain equals its
   input grain, and project the grain-matched expressions on the parent node
   instead. Larger; touches how aggregate groups resolve (`GroupNode` and its
   `group_required` decision) and every consumer of that node shape.

Recommend 1, with the log line.

## Verification plan

1. Optimizer tests: `tests/optimization`.
2. Full suite (`-m "not adventureworks_execution"`; locally also exclude
   `clickhouse_server`).
3. A before/after comparison of generated SQL across the modeling corpora:
   diff the `zquery_*.log` files (or blob-hash them) before and after the
   change. Every diff should be a removed rename-only CTE; any other diff,
   and any row change, is a bug.
4. `local_scripts/plan_debugger/trace_query.py` on adhoc04: the CTE step should
   show `questionable` as a tombstone ("collapse_single_parent ·
   CollapseSingleParent · merged into cooperative").

## Related

- `docs/handoff_duplicate_source_requests.md`: the same adhoc04 plan, from
  the discovery side.
- The group node that becomes this CTE is the line-level aggregate
  `grp:aggregate:d0:local.id|order.id:input:local.id`. The viewer's group
  chip on the CTE card links back to its build step.
