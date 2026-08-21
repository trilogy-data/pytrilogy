# union(...) TVF outputs lose arm nullability; sibling-aggregate rejoin drops real NULL groups

Filed 2026-08-20 against HEAD bb1a0980d, from run `20260820-153007` where q66
failed identically in BOTH the `enriched` and `enriched_docs` legs (agents
wrote semantically correct queries; the engine silently dropped a row).
Distinct from the previously-resolved "q66 derived-over-rowset disconnect"
theory; this is a fresh repro with a different mechanism. READ-ONLY triage
session; not fixed here.

## Symptom

Two filtered aggregates over DIFFERENT measures of a `union(...)` rowset,
grouped by a key set that includes a genuinely-NULL-valued column, silently
drop every NULL group. In q66 the warehouse with `w_warehouse_name = NULL`
(sk=5) vanishes: 4 rows returned instead of 5, all other rows exact. Both
legs' agent answers are correct Trilogy; the reference SQL keeps the row.

## Minimal repro (tpc-ds eval workspace, scoring engine)

```trilogy
import raw.web_sales as w;
import raw.catalog_sales as c;

with combined as union(
    ( where w.sale_date.year = 2001 and w.warehouse.sk is not null
      select
        w.warehouse.sk as wh_sk,
        w.warehouse.name as wh_name,   # string? in the model
        w.sale_date.year as yr,
        w.sale_date.month_of_year as mon,
        sum(w.quantity * w.ext_sales_price) as sales,
        sum(w.quantity * w.net_paid) as net, ),
    ( where c.sale_date.year = 2001 and c.warehouse.sk is not null
      select
        c.warehouse.sk as wh_sk,
        c.warehouse.name as wh_name,
        c.sale_date.year as yr,
        c.sale_date.month_of_year as mon,
        sum(c.quantity * c.sales_price) as sales,
        sum(c.quantity * c.net_paid_inc_tax) as net, )
) -> (wh_sk, wh_name, yr, mon, sales, net);

select
    combined.wh_sk,
    combined.wh_name,
    combined.yr,
    coalesce(sum(combined.sales ? combined.mon = 1), 0) as jan_sales,
    coalesce(sum(combined.net ? combined.mon = 1), 0) as jan_net
order by combined.wh_sk asc;
```

Returns 4 rows; warehouse sk=5 (NULL name) is missing. Expected 5. The full
date/time/carrier filters from q66 are not needed. sk visible vs hidden does
not matter. Trigger boundary:

| shape | result |
|---|---|
| one aggregate, any filter, any grouping | OK (5 rows) |
| two filtered aggs over the SAME measure (`sales ? mon=1`, `sales ? mon=2`) | OK |
| filtered agg + unfiltered agg over the same measure | DROPS NULL group |
| filtered aggs over TWO measures (`sales ? mon=1`, `net ? mon=1`) | DROPS NULL group |
| same, but signature declares `wh_name string?` | OK (5 rows) |

The last line is both the workaround and the proof of mechanism.

## Mechanism

The planner computes the two aggregates as sibling group nodes over the union
and rejoins them on the shared group keys. The rendered join is

```sql
INNER JOIN "premium" on "rambunctious"."combined_wh_name" = "premium"."combined_wh_name"
  AND ...wh_sk... AND ...yr...
```

Plain `=` on `combined_wh_name`, which is NULL for warehouse 5 on both sides.
GROUP BY treats NULL as a value; the rejoin must pair it null-safely
(`IS NOT DISTINCT FROM`) or omit the redundant name key (wh_sk is non-null and
determines it). It does neither because the union output concept is not seen
as nullable:

- `union_item_to_concept` (trilogy/parsing/common.py:1638, modifier set at
  :1701) stamps `Modifier.NULLABLE` ONLY from an explicit `?` in the output
  signature. It never inherits nullability from the arm concepts
  (`align.concepts` are `w.warehouse.name` / `c.warehouse.name`, both
  `string?`). So `combined.wh_name.is_nullable == False`.
- `get_modifiers` (trilogy/core/processing/join_resolution.py:611) then finds
  `side_nullable(...) == False` on both sides (instrumented: the union
  QueryDatasource's `nullable_concepts` does not cover the mangled output
  either) and returns `[]`, rendering plain `=`.

The MERGE ALIGN path compensates for exactly this by stamping every align
join key NULLABLE (`extra_align_joins`, see the docstring of
trilogy/core/optimizations/null_safe_join.py). The union TVF path has no
equivalent, so the nullability must come from the concept itself, and it
doesn't.

## FIXED 2026-08-20

`union_item_to_concept` (`trilogy/parsing/common.py`) now ORs the explicit
signature flag with the arms' own nullability, reusing the existing
`_expr_is_nullable` walk, so `combined.wh_name` is stamped NULLABLE and
`get_modifiers` renders `is not distinct from` on the rejoin. The explicit
`?` signature keeps working and is now redundant rather than load-bearing.

Gate: `tests/engine/test_duckdb_union_tvf_nullable_output.py` - the report's
four trigger-boundary shapes crossed with declared/inferred nullability. Two of
the four shapes (`two_measures`, `filtered_and_unfiltered`) drop the NULL group
without the fix, matching the table above. Whole-corpus render (132 tpc-ds +
tpc-h queries) is byte-identical.

## Suggested fix direction (as filed)

Infer nullability at concept construction (discovery-side, not a late guard):
in `union_item_to_concept`, OR the explicit signature flag with the arm
concepts' nullability (any arm output nullable, or a nullable-propagating
derivation over nullable inputs, mirroring `side_nullable`'s intrinsic
checks). `null_safe_join.py` already exists to strip the modifier again
wherever non-nullness is provable, so over-stamping is recoverable.

## Scoring impact

q66 fails in every leg/cohort that reaches a correct union-based answer; the
canonical tests/modeling/tpc_ds_duckdb/query66.preql dodges it by using the
all_sales model with per-channel pinned aggregates joined on
`warehouse.sk` + `year` only (no nullable key in the rejoin).
