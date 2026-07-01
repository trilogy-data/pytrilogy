---
name: project_numeric_parameter_unsupported
description: parameter declared `numeric`/`numeric(N,M)` crashes (HydrationError "Unsupported datatype NUMERIC for parameter") — no NUMERIC branch in hydrate_parameter; surfaces as opaque "Unexpected error"
metadata:
  type: project
---

OPEN (handoff written). `parameter p numeric default 4305.50;` (and `numeric(15,2)`) → `HydrationError: Unsupported datatype NUMERIC for parameter p`, surfaced by the CLI as `Unexpected error: Unsupported datatype NUMERIC ...` (reads as a crash). `float`/`int`/`string`/`bool`/`date`/`datetime` params all work. NUMERIC is first-class everywhere else (model uses `0::numeric(15,2)`), so agents reach for it naturally.

Root cause: `hydrate_parameter` (`trilogy/parsing/v2/rules/concept_rules.py:151-166`) has per-type coercion branches (INTEGER/FLOAT/BOOL/STRING/DATE/DATETIME) and an `else: raise fail(...)` — NO NUMERIC branch. Fix=add `elif datatype == DataType.NUMERIC or isinstance(datatype, NumericType): parameter_value = Decimal(str(raw))` (catch both bare NUMERIC enum + parameterized Numeric(N,M) instance). Model-independent repro: `parameter p numeric default 4305.50; select p as x;`.

Discovered as a driver of the q14 token sink (3.76M tokens, rebaseline 20260701-013044) — agent used `parameter overall_avg_threshold numeric` and thrashed on the opaque crash. Compounds with the HydrationError→"Unexpected error" mislabel ([[project_parse_error_mislabeled_unexpected]]) — that fix makes it a clean "Syntax error" even before the NUMERIC branch lands, but the real fix is supporting numeric. q14's other churn (composite 3-way cross-channel intersection `not joinable`) was transient agent phrasing — the core intersection resolves cleanly now. Handoff: `evals/tpcds_agent/bug_numeric_parameter_unsupported.md`.
