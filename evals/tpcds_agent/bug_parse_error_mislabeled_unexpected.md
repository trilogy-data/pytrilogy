# BUG: parse-time validation errors (HydrationError/ParseError) surface as "Unexpected error"

**Classification:** error-labeling defect (no wrong results, no crash). Surfaces an actionable,
user-fixable parse error as `Unexpected error in stdin: ...`, which reads to a CLI user / agent as
an internal crash to retry verbatim rather than a mistake to fix. Same defect class as the
planner-RecursionError label fix (`scripts/common.py`).

**NOT a resolution bug — the guard is correct (verified).** See "Is the query valid?" below.

**Status:** root-caused, minimal repro. STILL NOT fixed on HEAD (verified 2026-07-08 —
`handle_execution_exception` in `trilogy/scripts/common.py` has no `ParseError`/`HydrationError`
branch; `SyntaxError`/`InvalidSyntaxException` are labeled, but `HydrationError` still falls through
to "Unexpected error"). Small, contained fix.

**Sibling now fixed:** the `ImportError` mislabel (old q58 report) is resolved — `common.py` now has
an explicit `ImportError` branch. This report covers the remaining `ParseError`/`HydrationError`
family only.

---

## Symptom

A degenerate self-join (the discovered instance — an agent exploration probe) emits:

```
Unexpected error in stdin: Cannot join `wk2001.wk` to itself (`wk2001.wk` resolves to the
same key `wk2001.wk`), which degenerates to `1=1`. Join distinct keys (e.g. separate rowset
outputs or distinct expressions).
```

The message body is excellent and actionable — only the `Unexpected error` prefix is wrong. It
should read `Syntax error` (the other parse-time guards in the same function already do).

## Is the query actually valid? (the question to rule out a resolution bug)

No — the guard is correct AND precise. Tested against the q02 workspace model:

| probe | join | result |
|---|---|---|
| P1 | `inner join wk2001.wk = wk2001.wk` (both sides the SAME key) | **rejected** (correct — degenerates to `1=1`) |
| P2 | `inner join ws.date.week_seq = wk2001.wk` (shared lineage: `wk` is a union partly derived from `ws.date.week_seq`) | **resolves OK** |
| P3 | `where ws.date.week_seq in wk2001.wk` (membership form) | **resolves OK** |

P1 is genuinely degenerate (joining a column to itself). P2 confirms the guard does NOT
false-positive on shared lineage — it fires only when both sides resolve to the *identical*
address. There is no resolution bug; the agent simply wrote a throwaway `k = k` probe. (The agent's
real final query used a window `lead(...)` form and passed.)

## Minimal repro

```trilogy
import raw.web_sales as ws;
import raw.catalog_sales as cs;
with wk2001 as union(
  (select ws.date.week_seq as wk where ws.date.year = 2001),
  (select cs.sold_date.week_seq as wk where cs.sold_date.year = 2001)
) -> (wk);
select wk2001.wk as wk inner join wk2001.wk = wk2001.wk;   -- "Unexpected error", should be "Syntax error"
```

## Root cause

- The guard raises via `fail(node, ...)` → returns a `HydrationError`
  (`trilogy/parsing/v2/rules/select_statement_rules.py:239`, helper at `rules_context.py:144`).
- `HydrationError` MRO is `HydrationError → ParseError → Exception` — it is **not** a subclass of
  `SyntaxError` or `InvalidSyntaxException`.
- `handle_execution_exception` (`trilogy/scripts/common.py:616`) labels `SyntaxError`,
  `InvalidSyntaxException`, `UndefinedConceptException`, `DisconnectedConceptsException`,
  `UnresolvableQueryException`, `FunctionArgumentException`, `RecursionError` — but **not**
  `ParseError`/`HydrationError`, so it falls through to `else: print_error("Unexpected error...")`
  (line 656).

This affects the whole `ParseError`/`HydrationError` family, not just the self-join guard — any
parse-time validation that raises through `fail()` is mislabeled the same way.

## Fix direction

In `handle_execution_exception` (`trilogy/scripts/common.py`), add `ParseError` (covers
`HydrationError` and siblings) to the `Syntax error` branch:

```python
from trilogy.parsing.exceptions import ParseError
...
elif isinstance(e, (SyntaxError, InvalidSyntaxException, ParseError)):
    print_error(f"Syntax error{location}: {e}")
```

`ParseError` is the right scope: every subclass is a parse-time authoring mistake with a
user-facing message, none are internal crashes. Confirm no `ParseError` subclass is used as a
control-flow signal that should stay "unexpected" (grep `class .*ParseError`).

## Guardrail / test

Mirror the recursion-label test
(`tests/scripts/test_config.py::test_handle_execution_exception_labels_recursion_errors`): assert a
raised `HydrationError` is printed as `Syntax error...`, not `Unexpected error...`. Also relabel in
the eval analyzer (`evals/common/analyze_run.py`) if it buckets on the message prefix, so these
stop counting as framework "unexpected" churn.

## File pointers

- `trilogy/scripts/common.py:616` — `handle_execution_exception` (the fix).
- `trilogy/parsing/v2/rules/select_statement_rules.py:239` — the self-join guard (raises via `fail`).
- `trilogy/parsing/v2/rules_context.py:144` — `fail()` returns `HydrationError`.
- `trilogy/parsing/v2/model.py:48` / `trilogy/parsing/exceptions.py:1` — `HydrationError` / `ParseError`.
