# BUG: `parameter <name> numeric` crashes — NUMERIC not supported as a parameter datatype

**Classification:** framework gap + opaque error. `parameter p numeric default 4305.50;` raises
`HydrationError: Unsupported datatype NUMERIC for parameter p`, which the CLI surfaces as
`Unexpected error in <file>: Unsupported datatype NUMERIC for parameter` (reads as a crash). NUMERIC
is a first-class datatype everywhere else in Trilogy (the enriched model uses `0::numeric(15,2)`),
so an author naturally reaches for it — and hits a wall.

**Status:** FIXED 2026-06-30. Needed two branches, not one: (1) NUMERIC branch in
`hydrate_parameter` coercing to `Decimal`, and (2) registering `Decimal` as a buildable
primitive in `Factory._build_dispatch` (`build.py`) — the parse layer produced a bare `Decimal`
constant that the build dispatcher had no handler for (`NotImplementedError: Cannot build
<class 'decimal.Decimal'>`). Tests: `tests/test_parsing.py::test_numeric_param` (parse) +
`tests/engine/test_duckdb.py::test_numeric_parameter` (execute), both `numeric` and `numeric(15,2)`.

**Impact:** a contributor to the TPC-DS **q14** token sink (3.76M prompt tokens in run
20260701-013044). The agent declared `parameter overall_avg_threshold numeric default 4305.50;`,
got an "Unexpected error", and thrashed (it reads as an internal bug to retry, not a fixable type
choice). Compounds with the parse-error mislabel bug (see below).

## Repro

Model-independent (no import needed):

```trilogy
parameter p numeric default 4305.50;
select p as x;                 -- HydrationError: Unsupported datatype NUMERIC for parameter p
```

| parameter type | result |
|---|---|
| `numeric` | ❌ HydrationError |
| `numeric(15,2)` | ❌ HydrationError (`Numeric(15,2)`) |
| `float` | ✅ OK |
| `int` / `string` / `bool` / `date` / `datetime` | ✅ OK |

## Root cause

`hydrate_parameter` (`trilogy/parsing/v2/rules/concept_rules.py:151-166`) coerces the raw value by
datatype, with explicit branches for INTEGER, FLOAT, BOOL, STRING, DATE, DATETIME, and an
`else: raise fail(...)`. There is **no NUMERIC branch**, so `numeric` / `Numeric(N,M)` falls
through to the error.

```python
if datatype == DataType.INTEGER:   parameter_value = int(raw)
elif datatype == DataType.FLOAT:   parameter_value = float(raw)
...
else:
    raise fail(node, f"Unsupported datatype {datatype} for parameter {name}.")   # <- line 166
```

## Fix direction

Add a NUMERIC branch that coerces to `Decimal` (matching how numeric literals are represented
elsewhere — check what `0::numeric` hydrates to and mirror it). Handle both bare `NUMERIC` and
parameterized `Numeric(precision, scale)` (the latter is a `NumericType` instance, not the
`DataType.NUMERIC` enum member — the branch must catch both, e.g. `isinstance(datatype, NumericType)
or datatype == DataType.NUMERIC`):

```python
from decimal import Decimal
...
elif datatype == DataType.NUMERIC or isinstance(datatype, NumericType):
    parameter_value = Decimal(str(raw))
```

Verify `parameter p numeric default 4305.50; select p as x;` and `numeric(15,2)` both build and
execute, and that the value compares correctly against numeric columns (the q14 use case is
`... > overall_avg_threshold`).

**Secondary (already tracked):** even if a type is genuinely unsupported, the `HydrationError` is
mislabeled "Unexpected error" instead of "Syntax error" — see
`bug_parse_error_mislabeled_unexpected.md` (add `ParseError` to the labeled branch in
`handle_execution_exception`). That fix turns this from a crash-looking message into a clear
authoring error even before the NUMERIC branch lands.

## Guardrail / test

Add to the parameter tests: `parameter p numeric default 4305.50; select p as x;` (and
`numeric(15,2)`) builds and returns the value; existing int/float/string/date param tests stay green.

## File pointers

- `trilogy/parsing/v2/rules/concept_rules.py:151-166` — `hydrate_parameter` (add NUMERIC branch).
- `trilogy/parsing/v2/rules/concept_rules.py:166` — the `fail(...)` fallthrough.
- Related: `evals/tpcds_agent/bug_parse_error_mislabeled_unexpected.md` (HydrationError → "Unexpected error" labeling).
