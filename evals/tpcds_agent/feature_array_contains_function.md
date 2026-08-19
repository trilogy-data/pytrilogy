# Feature: add an `array_contains(arr, elem)` function

Status: PROPOSED. Motivated by run `20260817-013108` (deepseek-v4-flash),
enriched q08: the agent wrote
`where array_contains(split(zips, ','), pref_zip.zip)` (standard DuckDB/Spark
spelling) and got a raw pest caret error, because no such function exists:

```
Parse error:
  --> 13:21
   |
13 | where array_contains(split(zips, ','), pref_zip.zip)
   |                     ^---
   = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND,
     dot_tail, bracket_tail, ...
```

The parser treats the unknown name as a concept identifier and dies at the
open paren, which reads as nonsense to an agent (see the related
error-message note at the bottom).

## Current state (verified 2026-08-16)

- No `array_contains` / `list_contains` anywhere in the repo.
- Existing array functions in `trilogy/core/enums.py` (FunctionType, lines
  286-293): `ARRAY_DISTINCT`, `ARRAY_SUM`, `ARRAY_SORT`, `ARRAY_TRANSFORM`,
  `ARRAY_TO_STRING`, `ARRAY_FILTER`, `GENERATE_ARRAY`; plus `ARRAY_AGG`.
- `CONTAINS` exists but is string containment; `REGEXP_CONTAINS` likewise.
- The membership idiom `x in <array-expr>` already covers the semantic need
  in WHERE (plans via the existence/unnest machinery), but it currently has a
  planning bug when projected next to an aggregate - see
  `bug_q08_split_membership_projection_with_aggregate.md`. A plain scalar
  `array_contains` renders as a simple boolean expression and would sidestep
  that machinery entirely, so it is both an agent-compat feature AND a
  workaround for that bug's shape.

## Proposed semantics

`array_contains(arr array<T>, elem T) -> bool` - DuckDB argument order
(list first, element second), matching DuckDB `array_contains`/`list_contains`
and Spark `array_contains`. Null handling: defer to native DuckDB behavior
(NULL arr -> NULL; NULL elem -> NULL unless the backend says otherwise);
document whichever we lock in and keep it identical across dialects.

## Wiring checklist (the established new-function path)

1. `FunctionType` enum member in `trilogy/core/enums.py` (ARRAY block).
2. `FunctionConfig` entry in `FUNCTION_REGISTRY` (`trilogy/core/functions.py`);
   import-time check requires an entry for every enum member. Arg types:
   `(array<T>, T) -> bool`.
3. pest rule + `_generic_functions` alternation
   (`trilogy/scripts/dependency/src/trilogy.pest`); needs
   `maturin develop -m trilogy/scripts/dependency/Cargo.toml --release`.
4. lark mirror (`trilogy/parsing/trilogy.lark`).
5. `SyntaxNodeKind` + name-map (`trilogy/parsing/v2/syntax.py`).
6. `SIMPLE_FUNCTION_DISPATCH` (`trilogy/parsing/v2/rules/function_rules.py`).
7. Author renderer (`trilogy/parsing/render.py`); default
   `enum_value(args)` fallback is fine here.
8. Per-dialect renders: duckdb native `array_contains`; check
   bigquery (`elem IN UNNEST(arr)`), snowflake (`ARRAY_CONTAINS(elem::variant,
   arr)` - REVERSED arg order), postgres (`elem = ANY(arr)`), presto
   (`contains(arr, elem)`), clickhouse (`has(arr, elem)`), sql_server (no
   arrays - raise unsupported), sqlite (raise unsupported).
9. Tests: dialect function-map dup-key scan
   (`tests/test_dialect_function_maps.py`) picks up the enum automatically;
   add an execution test beside the array function coverage.
10. Docs: `ai/constants.py` expression section + `syntax_examples.py` so
    `agent-info` surfaces it (the whole point is agents reaching for it).

## Related error-message fix (separate, cheap, high leverage)

Unknown-function-name calls should get a targeted diagnostic instead of the
raw caret dump: the grammar knows every function name, so `identifier(` where
identifier is not a registered function can say
`Syntax [NNN]: no function named 'array_contains'; did you mean ... ?
(for array membership, write 'elem in arr')`. In this run the raw error sent
the agent into an unnest workaround spiral. This helps every future unknown
function, not just this one.
