# Named aggregate-lineage concept in WHERE renders as an inline aggregate (invalid SQL)

A named/derived concept whose lineage is a `Comparison`/`Conditional` wrapping
an aggregate (e.g. `auto f <- count(...) by item > 0;`), referenced in a
top-level `where`, is rendered as an **inline aggregate inside the SQL `WHERE`
clause** → DuckDB `Binder Error: WHERE clause cannot contain aggregates`.

Writing the *same* logic inline in the WHERE (instead of behind a concept name)
renders correctly as `HAVING`. So the defect is the **concept-reference
indirection**, not the aggregate-in-WHERE concept itself.

(The bare variant — `count(...) > 0`, no `by` — does **not** error; it instead
silently computes the aggregate globally and produces wrong rows. That global
mis-grain is the separate q83 issue / regrain work; this doc is specifically the
*inline-render* defect that bites the explicit-grain form.)

## Repro (in-engine)

```python
from trilogy import Dialects, Environment

MODEL = """
key rid int;
property rid.item int;
property rid.ch string;
datasource rows (rid: rid, item: item, ch: ch)
grain (rid)
query '''select 1 rid,100 item,'S' ch union all select 2,100,'W'
         union all select 3,200,'W' union all select 4,300,'S' ''';
auto f_byitem <- count(rid ? ch = 'S') by item > 0;
"""
ex = Dialects.DUCK_DB.default_executor(environment=Environment())
print(ex.generate_sql(MODEL + "where f_byitem select item order by item;")[0])
```

### Buggy output — named concept in WHERE (aggregate inlined into WHERE)

```sql
SELECT "rows"."item" as "item"
FROM (...) as "rows"
WHERE
    "rows"."ch" = 'S' and (count("rows"."rid") > 0) = True   -- aggregate in WHERE!
GROUP BY 1
ORDER BY "rows"."item" asc
```

Executing this raises:
`(_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!`

### Correct output — same logic written inline in WHERE

`where count(rid ? ch='S') by item > 0 select item order by item;`

```sql
SELECT "rows"."item" as "item"
FROM (...) as "rows"
WHERE "rows"."ch" = 'S'
GROUP BY 1
HAVING count("rows"."rid") > 0          -- correctly placed in HAVING
ORDER BY "rows"."item" asc
```

## Root cause

The renderer's WHERE/HAVING split in `trilogy/dialect/base.py` (~lines
2257-2275) routes a condition atom to `HAVING` when, for a grouped CTE, it is
**not** a scalar condition (`is_scalar_condition(x, materialized)` is False);
otherwise it goes to `WHERE`.

`is_scalar_condition` in `trilogy/core/processing/condition_utility.py`
(the `CONCEPT_TYPES` branch, ~lines 231-238) descends a concept's **lineage**
only when that lineage is an `AGGREGATE_TYPES` or `FUNCTION_TYPES`:

```python
elif isinstance(element, CONCEPT_TYPES):
    if materialized and element.address in materialized:
        return True
    if element.lineage and isinstance(element.lineage, AGGREGATE_TYPES):
        return is_scalar_condition(element.lineage, materialized)
    if element.lineage and isinstance(element.lineage, FUNCTION_TYPES):
        return is_scalar_condition(element.lineage, materialized)
    return True   # <-- BASIC boolean-of-aggregate lineage (Comparison) lands here
```

For `f_byitem` the lineage is a `Comparison` (`count(...) > 0`), which is neither
`AGGREGATE_TYPES` nor `FUNCTION_TYPES`, so the function falls through to
`return True` — the concept is judged **scalar**, the atom is placed in `WHERE`,
and `render_expr` then inlines the aggregate there. The inline form works
because `is_scalar_condition` sees the `AggregateWrapper` directly at the
comparison's left operand (`COMPARISON_TYPES` → `AGGREGATE_TYPES` → False).

## Fix direction

Extend the `CONCEPT_TYPES` branch so it also recurses into
`Comparison`/`Conditional`/`Between` lineages (so an aggregate wrapped in a
boolean/expression concept is seen). Equivalently, recurse into any lineage
type that `is_scalar_condition` already understands, not just aggregate/function.

## Verification after fix

- The repro: the named-concept query emits `HAVING count(rows.rid) > 0` and
  executes.
- Add a rendering test (style of `tests/rendering/test_rendering.py`) asserting a
  named `count(...) by k > 0` concept used in WHERE places the predicate in
  HAVING.
- Re-run `tests/modeling/tpc_ds_duckdb` / `tpc_h` (no regression).

## Relationship to other work

Independent of [the BASIC re-grain bug]
(`bug_basic_boolean_aggregate_not_regrained.md`) and of the WHERE-aggregate
regrain work (q83, `bug_q83_churn_030015.md`). Together, the BASIC re-grain fix
(correct grain for `count(...) > 0`) + this fix (route it to HAVING) unblock
q83's literal named-`count()>0`-in-WHERE form.
