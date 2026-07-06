# Diagnosis: q87 FAIL in enriched leg (run 20260706-135542_enriched)

## Symptom

- Scorer: `id=87 status=fail ref_rows=1 cand_rows=1 detail="result set differs from reference"`.
- Reference (`tests/modeling/tpc_ds_duckdb/query87.sql`, EXCEPT-chain): **47,298**.
- Candidate (`workspace/query87.preql`, concat-key + `not in`): **45,689** (reproduced exactly on a DB copy).
- SQL legs pass: the sql_bare answer uses tuple `EXCEPT` with inner joins → 47,298.
- Canonical `query87.preql` (sum-case per channel, no membership) re-verified against the DB copy: **47,298** — still matches the reference. No regression there.

## Final-count decomposition (the 1,609 gap)

- Reference EXCEPT is **NULL-safe**: tuples with NULL last/first name are kept and
  compared with NULL = NULL. Exactly **1,609** ref-surviving combos contain a NULL
  first or last name (verified: restricting the reference EXCEPT to
  `c_last_name is null or c_first_name is null` = 1,609 = 47,298 − 45,689).
- The candidate's `store_key not in (catalog_key) and not in (web_key)` drops every
  one of those: the generated SQL renders `concat(...)` as the NULL-propagating
  `||` chain, so a NULL name → NULL key → `NULL NOT IN (...)` is never true.
- **Not agent-recoverable via membership**: emulating the composite tuple form
  `(ln, fn, dt) not in (select ...)` under Trilogy's render semantics
  (RHS null-filtered, LHS three-valued) ALSO returns **45,689**. Any `not in`
  formulation in Trilogy yields the wrong count; only an EXCEPT-like/aggregate
  formulation (what the canonical uses — GROUP BY treats NULLs as equal) matches.
- Null customer FKs (pattern #1) are NOT the cause here: reference inner-joins
  customer (drops null-FK rows) and the candidate's NULL keys drop them too — both
  sides exclude them consistently.

## Classification

**OVERALL: QUESTION/null-semantics** (family of pattern #1) — the task says "match
purely on the (last name, first name, date) combination" but never specifies that
NULL names must match each other; the reference's EXCEPT is null-safe while any
Trilogy `not in` is SQL three-valued. The agent's answer is a defensible reading;
the 1,609-row gap is entirely NULL-name combos.

**With heavy FRAMEWORK aggravation**: the agent first authored the natural
composite-tuple form twice and was crashed out by framework bug A, then tried the
obvious cast workaround and was crashed out by bug B — three "Unexpected error"
round-trips before retreating to the concat key. (The framework bugs did not change
the final number — the tuple form gives the same 45,689 — but they are engine
crashes by doctrine and burned the agent's iterations.)

## Framework defects (each = "Unexpected error", reproduced + minimized)

### A. Mixed-type composite membership tuple is unrepresentable (transcript lines 2213, 2424)

- Repro: `where (aname, adate) not in (bname, bdate) select count(aid) as cnt;`
  with `aname string; adate date;` →
  `HydrationError: Tuple elements have incompatible types STRING and DATE`.
  Same for (int, date) and (int, str). Homogeneous `(str,str) not in (str,str)` works.
- Root cause: `expr_tuple` hydration (`trilogy/parsing/v2/rules/subselect_rules.py:31-36`)
  calls `reduce_tuple_element_datatypes` (`trilogy/core/models/core.py:531-534`), which
  forces ALL tuple elements into one compatible type family. Its own docstring claims it
  serves "column-expression (composite-membership) tuples", but a row constructor for
  composite membership needs per-POSITION compatibility (left[i] vs right[i]), not
  intra-tuple homogeneity. The renderer (`render_composite_membership`,
  `trilogy/dialect/base.py:1520`) compares column-wise and would be fine.
- Surfacing: HydrationError printed with the "Unexpected error" prefix — the known CLI
  mislabeling (`evals/tpcds_agent/bug_parse_error_mislabeled_unexpected.md`). Even
  relabeled, the rejection itself is the defect: this is a legitimate construct.

### B. Cast expression inside composite-membership RHS asserts at render (transcript line 2335)

- Repro: `where (aname, adate::string) not in (bname, bdate::string) select count(aid) as cnt;`
  → parses and plans fine, then `AssertionError: composite membership operands must be
  concepts` at SQL-render time.
- Root cause: `render_composite_membership` asserts every RHS tuple element is a
  `BuildConcept` (`trilogy/dialect/base.py:1546-1548`); a `cast()` Function member hits the
  bare assert (fires via `render_cte_used_map`, `trilogy/core/optimizations/utils.py:23`).
  Expressions on the LEFT tuple work (`(aname, adate::string) not in (bname, bname)` is
  fine), and single-expression membership RHS has a dedicated path
  (`_render_expression_membership_subselect`, base.py:1560). Either support expression RHS
  members or reject at parse with an authored error — never an AssertionError.
- Cruel interaction: bug A tells the author the types are incompatible; casting to
  homogenize them (the only in-language response) triggers bug B.

### C. (Latent, found while root-causing) duplicate `CONCAT` key in DuckDB FUNCTION_MAP

- `trilogy/dialect/duckdb.py` defines `FunctionType.CONCAT` TWICE in the same dict
  literal: line 135-137 (`CONCAT(...)` — NULL-ignoring in DuckDB) and line 161
  (`(a || b || ...)` — NULL-propagating). The second silently overrides the first.
  Base dialect renders `concat(...)` (base.py:623). This flips concat's NULL semantics
  for DuckDB; with the line-135 rendering the candidate would return 47,270
  (still ≠ 47,298 — the residual 28 is concat-key conflation of NULL-vs-empty
  name encodings — but 20x closer). Owner should decide which semantics is the
  language contract and delete the other key.

### D. (Design-gap candidate) membership is not null-safe, unlike joins

- `render_composite_membership` null-filters the RHS (base.py:1554) so non-NULL LHS
  gets sane not-in, but a NULL LHS component still yields three-valued drop. Trilogy
  doctrine elsewhere ("joins do NOT drop nulls", IS NOT DISTINCT FROM join equality)
  treats NULL as a valid member; membership is the inconsistent corner. A null-safe
  membership rendering would have made both the tuple AND concat forms return the
  reference's 47,298. Judgment call for owner — noting, not asserting, as a defect.

## Transcript error inventory (5 tool errors)

1. line ~1982: `refused to write ... rowset store_tuples as` — agent's own syntax slip
   (`rowset X as` vs `rowset X <-`), correctly rejected as a parse error. AGENT, fine.
2. line 2213: Unexpected error — bug A (tuple `(str,str,date)` composite membership).
3. line 2335: Unexpected error — bug B (`::string` cast inside membership tuple).
4. line 2424: Unexpected error — bug A again (agent retried the identical tuple form).
5. (5th tool_error is the initial write refusal counted separately by the harness.)

## Verification notes

- All numbers computed on a scratch copy of `evals/tpcds_agent/.cache/tpcds_sf1.duckdb`
  (deleted after use); no run-dir database was opened.
- `d_year = 2000` ≡ `d_month_seq BETWEEN 1200 AND 1211` on this data (verified), so the
  agent's year filter is not a factor.
- Actual generated candidate SQL re-executed: 45,689 (matches the agent's reported
  farewell figure and the scorer's fail).
