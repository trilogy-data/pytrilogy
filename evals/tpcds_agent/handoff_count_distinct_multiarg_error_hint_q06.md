# Handoff: `count_distinct(a, b)` should not be an error loop (q06)

**Status: DONE (2026-08-11) — multi-arg `count_distinct` is now LEGAL SYNTAX,
desugared through `grain()` at hydration.** Originally from
`results/20260810-211903_enriched_noise/agent_log.q06.jsonl`.

Resolution evolved in two steps the same day: first landed as a targeted parse
error (Syntax [230] pointing at `count(grain(a, b))`), then superseded — why
force an error loop at all when the intent is unambiguous? The error path was
removed and the syntax accepted instead. The text is accepted **as authored**
(no source rewrite), so files round-trip; the desugar happens at the build
layer, exactly like `grain()` itself.

## What landed

- Grammars — both backends accept an argument list in `count_distinct(...)`
  AND the `count(distinct ...)` alias (which is the literal SQL habit):
  - `trilogy/parsing/trilogy.lark` `count_distinct` rule
  - `trilogy/scripts/dependency/src/trilogy.pest` `count_distinct` rule
    (**needs `maturin develop` from repo ROOT** — done for the dev venv)
- Hydration — `trilogy/parsing/v2/rules/function_rules.py`: `fgrain`'s desugar
  core extracted as `grain_hash(args, factory)`; `generic_aggregate` routes a
  multi-arg COUNT_DISTINCT through it, producing exactly the
  `count_distinct(grain(a, b))` function tree. The factory's
  `arg_count=1` validation for COUNT_DISTINCT is untouched (it still receives
  one argument — the hash).
- The interim Syntax [230] error code + `detect_count_distinct_multiarg`
  detector + backend wiring were removed (unreachable once the syntax parses).

## Semantics (deliberate, documented in the tests)

`count_distinct(a, b)` = `count_distinct(grain(a, b))`: distinct combinations,
and `grain()` is TOTAL over NULLs — a combination with a missing member still
counts. This differs from SQL's `COUNT(DISTINCT a, b)` (which drops rows where
any member is NULL) in the same way all `grain()` counting deliberately does.

## Validation

- `tests/complex/test_count_distinct_multiarg.py`: both backends parse all
  forms (multi-arg, 3-arg, `count(distinct a, b)`, spacing/case); generated
  SQL for the sugar forms is **byte-identical** to explicit
  `count_distinct(grain(a, b))`; single-arg picks up no grain wrapper; DuckDB
  execution counts NULL-member combinations.
- Regression: `tests/complex` + `tests/parsing` + `tests/rendering` +
  validation suites (729) and `tests/modeling/tpc_h` (30) all green on the
  rebuilt wheel. ruff/mypy/black clean.

## Original context

SQL habit: `COUNT(DISTINCT a, b)`. The agent wrote the direct translation,
`file write` refused it with a bare grammar dump at the comma, and the agent
recovered by hand-rolling `count_distinct(concat(i.id::string, '|',
i.category::string))` — the delimiter-collision pattern `grain(...)` exists to
replace. That authoring loop (and the error-message loop that replaced it) is
now gone: the direct translation simply works, with grain semantics.
