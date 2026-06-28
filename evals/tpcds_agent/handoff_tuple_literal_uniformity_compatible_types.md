# Handoff: `expr_tuple` uniformity check rejects compatible (non-identical) element types

**Status:** FIXED 2026-06-27. Split out from the composite-tuple-membership work
(see [[project... composite membership]]) so that feature can stay focused. The membership feature
sidesteps this path (it represents a column tuple as an `ARRAY` `Function`, not a `TupleWrapper`), so
this bug is now **only** reachable for *literal* value tuples.

## Fix (landed)

New shared helper `reduce_tuple_element_datatypes(list[CONCRETE_TYPES]) -> (type, nullable)` in
`trilogy/core/models/core.py`: filters NULL (→ `nullable`), reduces the rest pairwise with
`is_compatible_datatype`, merging with `merge_datatypes`; raises `ValueError("Tuple elements have
incompatible types X and Y")` on a genuinely-incompatible pair. Wired into all three literal-tuple
construction sites:

- `expr_tuple` in `trilogy/parsing/v2/rules/subselect_rules.py` (PEST `EXPR_TUPLE` path; unified the
  literal + composite-membership `ConceptRef` branches through the helper).
- `tuple_to_wrapper` in `core.py` (LARK `tuple_lit` path — this was the *second* strict-set check the
  original write-up missed; the LARK grammar lowers `in (..)` to `tuple_lit`, not `EXPR_TUPLE`).
- `expr_tuple` + `_parse_simple_expr_tuple_text` in the legacy `trilogy/parsing/parse_engine.py`.

`TupleWrapper.type` annotation broadened `DataType` → `CONCRETE_TYPES` (merge can yield
`NumericType`/`EnumType`). Both active backends now feed v2 hydration (`parser.py` → `parse_engine_v2`),
PEST via `parse_pest`, LARK via `parse_lark`; `parse_engine.py`'s lark Transformer is legacy.

Note: `in (1, 2::numeric)` only builds on PEST — `::` cast inside a tuple is an orthogonal LARK
grammar gap, not the type check. Tests: `tests/engine/test_tuple_literal_compatible_types.py`.

## Symptom

A parenthesized literal tuple whose elements are **compatible but not identically-typed** raises:

```
Tuple must have same type for all elements
```

Examples that should be legal but currently fail:

```trilogy
where x in (1, 2.0)          -- INTEGER + FLOAT  (same numeric family)
where x in (1, 2::numeric)   -- INTEGER + NUMERIC
where c in (1, some_enum_val) -- INTEGER + EnumType(INTEGER)
```

This is the same root cause originally seen on TPC-DS q14, where `(s.item.brand_id, s.item.class_id)`
mixed `INTEGER` and `EnumType(INTEGER)`. (q14 itself is now handled by the composite-membership
feature, which never reaches `expr_tuple`'s `TupleWrapper` branch.)

## Root cause

Both parser backends compute the tuple datatype with an **exact-equality `set`** over element
datatypes and reject anything with more than one distinct member:

- `trilogy/parsing/v2/rules/subselect_rules.py` — `expr_tuple` (~L24):
  ```python
  datatypes = set(arg_to_datatype(x) for x in args)
  if len(datatypes) != 1:
      raise fail(node, "Tuple must have same type for all elements")
  ```
- `trilogy/parsing/parse_engine.py` — `expr_tuple` (~L4686): identical logic, `ParseError`.

`EnumType(INTEGER) != DataType.INTEGER` as set members even though
`is_compatible_datatype(INTEGER, EnumType(INTEGER))` is `True`, and `INTEGER != FLOAT` even though they
share a numeric family. So the check is stricter than the type system's own compatibility rules.

## Suggested fix

Replace the exact-set check with the same compatibility+merge logic used elsewhere for mixed-but-
compatible types (precedent: `get_coalesce_output_type` uses `is_compatible_datatype` +
`merge_datatypes` — see the coalesce-numeric-family-mixing fix). Roughly:

1. Reduce element datatypes pairwise with `is_compatible_datatype`; if any pair is incompatible, raise
   a clean error naming the two offending types (keep an author-friendly message, not the internal
   repr).
2. Use `merge_datatypes` to pick the `TupleWrapper.type` (e.g. `INTEGER` + `FLOAT` → `FLOAT`;
   `INTEGER` + `EnumType(INTEGER)` → `INTEGER`).

Apply to **both** backends (`subselect_rules.py` and `parse_engine.py`) so behavior matches regardless
of the active parser.

## Tests to add

- `x in (1, 2.0)` builds and executes (numeric-family mix).
- `x in (1, 2::numeric)` builds.
- a genuinely-incompatible mix (e.g. `(1, 'a')`) still raises a **clean** author-time error.
- a tuple of an enum-typed concept literal + its base int builds.

## Repro

```python
from trilogy import Environment, Dialects
env = Environment()
exec_ = Dialects.DUCK_DB.default_executor(environment=env)
exec_.generate_sql("const x <- 1; where x in (1, 2.0) select x;")  # raises today
```
