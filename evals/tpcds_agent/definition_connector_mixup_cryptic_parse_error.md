# Pattern: definition statements with the wrong connector/form fall through to a useless "expected EOI, block, or show_statement"

**Status:** OPEN — error-quality improvement (recurring, multiple instances).
**Surfaced by:** TPC-DS q02 + q05 enriched evals — agents repeatedly hit the same catch-all parse
error on small definition-syntax slips and got zero guidance.
**Severity:** Medium (agent-efficiency). These are the most common syntax mistakes agents make, and
the generic message turns a one-character fix into a dead-end rewrite.

## The pattern

When a statement starts with a definition keyword (`rowset`, `with`, `def`, `auto`) but uses the
wrong connector or form, the parser fails and reports the generic catch-all:

```
Parse error: --> N:1
N | rowset base as select ...
  | ^---
  = expected EOI, block, or show_statement
```

The caret points at the statement start and the message names internal grammar tokens — it never
says *what* about the statement is wrong.

## Confirmed instances (each 100% reproducible)

| Agent wrote | Problem | Valid form(s) |
|---|---|---|
| `rowset base as select …` (q05) | `rowset` uses `<-`, not `as` | `rowset base <- select …` **or** `with base as select …` |
| `def sun_sales -> sum(…)` (q02) | `def` needs ≥1 parameter | `def sun_sales(d) -> …` (called `@…(d)`) **or** `auto sun_sales <- sum(…)` |
| `def sun_sales() -> sum(…)` (q02) | empty params | same as above |

All three produce `expected EOI, block, or show_statement`.

## Suggested fix

In `trilogy/parsing/v2/errors.py`, when a parse fails and the failing region begins with a
definition keyword, emit a form-specific hint instead of the generic message. Mechanically: detect
the leading keyword (`rowset` / `with` / `def`) at the error position and the next token, then:

- `rowset <name> as …` → *"a `rowset` is defined with `<-`: `rowset <name> <- select …` (or use
  `with <name> as select …`)."*
- `def <name>` not followed by `(<params>)` → *"`def` is a parameterized macro and needs ≥1
  parameter: `def f(x) -> …` called `@f(x)`; for a no-arg named expression use `auto`."*

This is the same `errors.py`-hint approach already used for GROUP BY / FROM / join-condition slips.

## Note

None of these are planner bugs — the rejected forms are genuinely invalid; the issue is purely that
the catch-all message gives no actionable direction. Highest-value because these are the slips
agents make most often. (q02's `def` error additionally *masks* a clearer downstream `Syntax [102]`
subselect error, since the parser stops at the first failure — see
`q02_paramless_def_cryptic_parse_error.md`.)
