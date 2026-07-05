---
name: project_parse_error_mislabeled_unexpected
description: parse-time HydrationError/ParseError surfaces as "Unexpected error" instead of "Syntax error" in the CLI — labeling fix in handle_execution_exception
metadata:
  type: project
---

OPEN (handoff written). Parse-time validation errors raised via `fail()` → `HydrationError(ParseError)` are surfaced by `handle_execution_exception` (`trilogy/scripts/common.py:616`) as `Unexpected error in stdin: ...` because `HydrationError` MRO is `HydrationError→ParseError→Exception` — NOT a subclass of `SyntaxError`/`InvalidSyntaxException`, so it hits the `else` branch. Reads to an agent as an internal crash to retry, not a fixable mistake. Discovered on q02: the degenerate self-join guard `inner join wk2001.wk = wk2001.wk` (select_statement_rules.py:239) — message body is excellent ("degenerates to 1=1. Join distinct keys"), only the prefix is wrong.

**Guard is CORRECT, not a resolution bug** (verified): P1 `k=k` (same address) rejected; P2 `ws.date.week_seq = wk2001.wk` (shared lineage, union-derived) resolves OK; P3 membership resolves OK. Fires only when both sides resolve to the identical address.

**Fix**: add `ParseError` to the Syntax-error branch in `handle_execution_exception` (`elif isinstance(e, (SyntaxError, InvalidSyntaxException, ParseError))`). Test mirrors the recursion-label test (`tests/scripts/test_config.py::test_handle_execution_exception_labels_recursion_errors`). Same defect class as the just-landed RecursionError label. Handoff: `evals/tpcds_agent/bug_parse_error_mislabeled_unexpected.md`. Analyzer at `evals/common/analyze_run.py`. Related: [[project_left_derived_rowset_join_recursion]].
