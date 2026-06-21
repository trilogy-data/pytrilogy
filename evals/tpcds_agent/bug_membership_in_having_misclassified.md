# Bug #2: `x in <set-concept>` membership classified differently in HAVING vs WHERE

**Status:** FIXED 2026-06-19. Full membership-in-HAVING support now landed across all three layers:
#2 classification (this doc), #1 existence-sourcing
(`bug_invalid_reference_codegen_having_membership.md`), and #3 validator relaxation / no forced
hidden flag (`bug_membership_in_having_hidden_flag_discovery_crash.md`).

## Fix (2026-06-19)

Root cause: `select_finalize._substitute_having_aggregates`. When the SELECT has ANY aggregate,
`_validate_having_aggregates_match_select` builds a non-empty `sig_to_ref` and walks the entire
HAVING tree to substitute matching aggregates with their SELECT-alias refs. That walk rebuilt every
`Comparison` node as a **plain `Comparison(...)`** — even subtrees with nothing to substitute — so a
membership `SubselectComparison` was silently downgraded to a plain `Comparison`, dropping its
existence semantics. (The parse rule `expression_rules.comparison` correctly builds
`SubselectComparison` for `in`; the WHERE path never goes through this walk, hence the asymmetry.)

Fix: rebuild with `type(node)(...)` instead of `Comparison(...)` so the concrete subclass is
preserved. One-line change; regression test
`tests/test_parse_engine_v2.py::test_membership_classified_as_subselect_in_having_like_where`.

Now both WHERE and HAVING classify `x in <set>` as `SubselectComparison`. **This alone does NOT make
the auto-concept form render** — with classification fixed, those queries fall cleanly into #1
(existence not sourced at the HAVING application site, `bug_invalid_reference_codegen_having_membership.md`)
and still emit `INVALID_REFERENCE_BUG`. The validator (`_validate_syntax`) also still counts the set
as a required projection ref via `concept_arguments`; relaxing it to `row_arguments` (so an existence
RHS need not be projected, mirroring WHERE) should wait until #1 lands — otherwise a clean
"add it to the projection" error becomes a confusing render crash.

---

**Original status:** OPEN (found 2026-06-19)
**Severity:** medium — a valid membership filter either renders invalid SQL
(`INVALID_REFERENCE_BUG`) or is wrongly rejected by the HAVING-projection validator.
**Area:** parser / `select_finalize` — the conditional classification of `<x> in <concept>`.

## Symptom

The SAME membership expression is built as a `SubselectComparison` (existence semantics)
in a WHERE clause but as a plain `Comparison` in a HAVING clause. Because a plain
`Comparison` treats the right-hand set-concept as a scalar column reference, it:
- renders the set-concept as a bare column the output CTE doesn't carry →
  `INVALID_REFERENCE_BUG` (when the comparison survives to render), and
- is counted as a plain `concept_argument`, so `_validate_syntax` demands the set-concept
  appear in the SELECT projection (`HAVING references 'local.ws_2001', which is not in the
  SELECT projection`) — existence args should not need projecting (WHERE doesn't).

## Minimal reproduction

Needs the enriched TPC-DS model (`tests/modeling/tpc_ds_duckdb`, which has `all_sales.preql`).
No data required — the divergence is visible at parse/finalize time.

```trilogy
import all_sales as all_sales;
auto ws_2001 <- all_sales.date.week_seq ? all_sales.date.year = 2001;
select
    all_sales.date.week_seq,
    sum(all_sales.ext_sales_price) as sun,
    --all_sales.date.week_seq in ws_2001 as flag
where all_sales.date.week_seq in ws_2001
having all_sales.date.week_seq in ws_2001
;
```

Driver:
```python
from pathlib import Path
from trilogy.parsing.parse_engine_v2 import parse_text
ROOT = Path("tests/modeling/tpc_ds_duckdb")
_, parsed = parse_text(Path("repro.preql").read_text(), root=ROOT)
stmt = parsed[-1]
print(type(stmt.where_clause.conditional).__name__)   # SubselectComparison  (correct)
print(type(stmt.having_clause.conditional).__name__)  # Comparison           (BUG)
```

Output:
```
WHERE : SubselectComparison(left=ref:all_sales.date.week_seq, right=ref:local.ws_2001, IN)
HAVING: Comparison           ref:all_sales.date.week_seq in ref:local.ws_2001
```

## Expected

`<x> in <concept>` should classify identically regardless of clause. Both should be a
`SubselectComparison` so the membership carries existence semantics (the set is sourced as a
subselect, not required in the projection).

## Why it matters / how it surfaces

This is the dominant failure for the agent's `auto set <- …; select x as a … having a in set`
weekly-ratio shape (TPC-DS q02). The membership never reaches the existence machinery
(`append_existence_check` / the `basic_node` derived-boolean split), so it can't be sourced.
The `x as ws` alias form additionally renders `INVALID_REFERENCE_BUG`; the un-aliased grouping-key
form currently renders clean only because the const-fold/render layer (fixed 2026-06-02) folds it
against the projected flag.

## Relationship to siblings

- The CTE-handle form (`with cte … having cte.x in set`) IS classified as `SubselectComparison`
  correctly; its remaining gap (existence not sourced at the HAVING application site) is partly
  addressed by the `WhereSafetyNode` fold in
  `bug_invalid_reference_codegen_having_membership.md`.
- Once classification is unified, `_validate_syntax` should stop demanding the set-concept in the
  projection (the membership's RHS becomes an existence arg, not a row arg). Dropping the forced
  hidden flag dissolves the discovery crash tracked in
  `bug_membership_in_having_hidden_flag_discovery_crash.md` (#3).

## Next investigation

Find where the `in`-comparison is classified as `SubselectComparison` vs `Comparison` (the
finalize/condition-build path that inspects the RHS concept), and why the HAVING path takes the
plain branch. Compare the WHERE and HAVING finalize routes for the conditional.
