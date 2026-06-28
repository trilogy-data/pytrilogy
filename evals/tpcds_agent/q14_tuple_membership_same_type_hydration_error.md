# Bug: multi-column tuple membership `(a, b, c) in (set.a, set.b, set.c)` → uncaught HydrationError "Tuple must have same type for all elements" (q14)

**Status:** FIXED 2026-06-27 — composite (row-wise) membership is now first-class. The crash and the
`HydrationError` are gone; `(a, b) in (set.a, set.b)` lowers to a multi-column existence semi-join.
**Surfaced by:** TPC-DS q14 (run `20260627-181845`). Was thrown during **`generate_sql`** as an uncaught
`HydrationError` → `Unexpected error`, no clean Trilogy message.
**Severity:** HIGH/MED — either support multi-column membership or give a clean author-time error;
today it's an internal crash.

## Fix (landed)

`(a, b) in (set.a, set.b)` (and `not in`) is now lowered to a row-wise semi-join rendering as
`(a, b) IN (select x, y from cte where x is not null and y is not null)`. Implementation:

- **New `FunctionType.ROW_TUPLE`** marker (enums.py; excluded from the create_function registry in
  functions.py) — an ordered tuple operand of composite membership, never a SQL array literal.
- **`rewrite_composite_membership`** (`parsing/common.py`): at the `in`/`not in` parse site, when BOTH
  sides are tuples, rewrites each into a ROW_TUPLE `Function` (components surface as concept arguments,
  so the existing `existence_arguments`/`row_arguments` machinery sources each column individually).
  A column-tuple left with a non-tuple right, or an arity mismatch, raises a clean
  `InvalidSyntaxException`. Scalar-left `x in (a, b, c)` value-lists are untouched. Wired into both v2
  rules (`comparison`, `subselect_comparison`) and the legacy `parse_engine.py`.
- **Element-wise type validation** (`SubselectComparison._validate_types`, author.py): zips the two
  ROW_TUPLE operands' arguments and checks `is_compatible_datatype` per position (int vs enum<int> is
  compatible — that was the original false "same type" rejection), with a clean arity error.
- **Build** (`build.py` `_build_subselect_comparison`): ROW_TUPLE right is NOT collapsed into a single
  array concept (which would have unnested it as a value list) — it stays a function so each component
  sources independently.
- **Render** (`dialect/base.py`): `render_composite_membership` + `_resolve_existence_column` emit the
  multi-column existence subquery; a `FUNCTION_MAP[ROW_TUPLE]` row-constructor fallback covers any
  projected position.

The misleading "same type" message came from `expr_tuple`'s strict set-equality on element datatypes;
that uniformity check was separately relaxed for ALL tuples via `reduce_tuple_element_datatypes`
(see [[handoff_tuple_literal_uniformity_compatible_types]]).

Tests: `tests/engine/test_duckdb_tuple_membership.py` (row-wise semantics vs per-column cross-product,
`not in`, arity-mismatch clean error, scalar value-list unchanged).
**Note:** this is a **different** q14 crash from the FIXED grouping-over-aliased-rollup-key binder bug
([[project_q14_grouping_over_aliased_rollup_key_binder]]). Same query, different idiom the agent
tried — composite-key membership against a rowset.

## Symptom

```
HydrationError: Tuple must have same type for all elements
```

## Trigger (bisected to a one-liner)

Membership of a **column tuple** against a tuple of rowset outputs:

```trilogy
import raw.all_sales as s;
with combos as
where s.date.year between 1999 and 2001
select s.item.brand_id, s.item.class_id;

where (s.item.brand_id, s.item.class_id) in (combos.brand_id, combos.class_id)   -- <-- crashes
select s.item.brand_id, sum(s.quantity) as q
limit 10;
```

The error fires regardless of the element datatypes (`brand_id`/`class_id` are both integers), so the
"same type" message is misleading — the tuple-membership lowering itself is unhandled. The
single-column form (`s.item.brand_id in combos.brand_id`) works; the **multi-column tuple** form is
what breaks.

## Why it matters (idiom)

q14-style queries qualify a `(brand_id, class_id, category_id)` combo against a set computed in a
rowset — the natural Trilogy spelling is composite-key membership. The agent's fallbacks (concatenated
string key `concat(a::string,':',b::string,...) in concat(...)`, or nested per-column `in`) are
awkward and error-prone, so first-class composite membership would remove a real token sink.

## Likely fix area

Tuple/composite membership lowering — the `(expr, expr, ...) in (concept, concept, ...)` parse path.
Either (a) lower it to a multi-key existence/semi-join (each left element matched against the
corresponding set column, ANDed, over the set's rows), or (b) reject it at author-time with a clean
message that points to the concatenated-key workaround. The current path reaches a `HydrationError`
deep in resolution, which means the construct is parsed but never properly planned.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-181845/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
q = '''import raw.all_sales as s;
with combos as
where s.date.year between 1999 and 2001
select s.item.brand_id, s.item.class_id;
where (s.item.brand_id, s.item.class_id) in (combos.brand_id, combos.class_id)
select s.item.brand_id, sum(s.quantity) as q
limit 10;'''
eng.generate_sql(q)   # HydrationError: Tuple must have same type for all elements
```
