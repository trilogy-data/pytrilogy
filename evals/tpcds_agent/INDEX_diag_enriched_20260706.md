# Index: enriched second-half failure diagnosis — run 20260706-135542

17 enriched-leg failures that PASSED the SQL legs, each probed by an independent
read-only agent (full detail in `diag_qNN_enriched_20260706.md`). Canonical
`tests/modeling/tpc_ds_duckdb/queryNN.preql` re-verified passing in EVERY probe —
no framework wrong-result regression, no scorer issue.

## Verdict distribution

| cause | queries |
|---|---|
| AGENT error (proven by minimal counterfactual) | q60, q62, q64, q74, q75, q78, q88, q90, q94, q95, q81(primary) |
| QUESTION issue (reference computes an unstated restriction) | q70, q73, q76, q82, q87, q81(secondary), q67(joint) |
| MODEL issue (false/missing doc) | q67(joint) |
| FRAMEWORK bugs (loud, from q87; did NOT cause the score fail) | 3 defects, below |

## Recurring patterns (fix once, help many)

### P1 — Implicit inner-join null-drop (QUESTION) — q70, q73, q76, q67, q81, (q87 null-EXCEPT variant)
Reference SQL inner-joins a nullable FK (customer, store, date_dim, returning
address), silently excluding NULL-FK rows. The enriched model correctly marks the
FK nullable → Trilogy LEFT-joins and keeps them; task wording never states the
restriction. SQL legs pass "accidentally" (idiomatic `JOIN` = the implicit filter).
Every affected canonical .preql carries an explicit `... is not null` the task
never hints at. **Fix: amend task wording ("only sales with an identified
customer/store/date"), or LEFT-join the reference. Sweep ALL tasks whose reference
inner-joins a nullable FK.**

### P2 — ext_* vs per-unit measure choice (AGENT + one MODEL doc defect) — q64, q78, q67
Agent sums line-extended `ext_wholesale_cost`/`ext_list_price`/`ext_sales_price`
where the question wants per-unit. Twice pure agent slip; in q67 the model doc
actively misleads: `store_sales.preql:31` claims ext = "sales_price x quantity"
(false, 12,894 y2000 rows differ) and omits the `?` nullability marker (12,942
NULL ext rows). **Fix: correct store_sales.preql:31 doc + add "per-unit vs
line-extended" steering; consider question wording "per-unit".**

### P3 — Output shape (AGENT) — q74, q90, q94
Right numbers, wrong projection: helper columns left visible / wrong grain
(32 per-order rows vs 1 global row). Scorer hashes whole rows. **Fix: enriched
prompt guidance — "emit exactly the requested columns; hide helpers with `--`".**

### P4 — Semantic path/scope choice (AGENT) — q88, q95, q62, q60, q81
- q88: filtered via customer's CURRENT hd profile instead of sale-level hd FK.
- q95: evaluated exists-conditions inside the filtered rowset vs order-wide.
- q62: "recorded" → `name is not null` instead of `id is not null`.
- q60: surrogate `item.id` instead of `item.text_id` in output+semijoin.
- q81: dimension filter in row-grain WHERE co-grained the threshold average.
Model guidance exists and was often read; deepseek slipped anyway. **Fix:
prompt-level checklist + `trilogy explore` should surface doc comments for
imported concepts (q60 probe: guidance invisible in explore output).**

### P5 — SCD version co-occurrence (QUESTION) — q82
Reference conjoins all predicates on one `i_item_sk`; task's business wording
(and the model's own "use text_id" doc) implies text_id semantics → SCD-sibling
leak (4 rows vs 2). Latent extra: "appear in store sales" compiled away without
scanning the fact (non-binding at sf=1).

## Framework defects (from q87 probe — real bugs, file:line, need fixes + guards)

- **A. Mixed-type composite membership**: `(str, str, date) not in (select ...)`
  unrepresentable — `expr_tuple` forces intra-tuple type homogeneity
  (`parsing/v2/rules/subselect_rules.py:31-36` via
  `reduce_tuple_element_datatypes`, `core/models/core.py:531-534`); renderer
  compares column-wise and would be fine. "Unexpected error: Tuple elements have..."
- **B. Cast inside membership RHS tuple**: plans fine, dies at render on bare
  `assert isinstance(rc, BuildConcept)` (`dialect/base.py:1546-1548`). LHS casts
  work. Bug A's message steers agents into B.
- **C. Duplicate dict key `FunctionType.CONCAT`** in duckdb FUNCTION_MAP
  (`dialect/duckdb.py:135` null-ignoring `CONCAT()` vs `:161` null-propagating
  `||`; second silently wins) — flips concat NULL semantics.
- **D (design gap)**: membership (`in`/`not in`) rendering is not null-safe,
  inconsistent with the "joins do not drop nulls" doctrine; null-safe membership
  would have matched the reference EXCEPT semantics exactly (47,298).

## Notes
- q78's known OPEN `is_returned` model defect (store_sales.preql:92) checked and
  REFUTED as this run's cause — generated SQL used LEFT OUTER JOIN; still latent.
- All "cannot merge"/"Discovery error" sightings (q64×2, q74, q82) reproduced as
  the by-design disconnected-import guardrail with clear messages; agents
  recovered in one step each time. Working as intended.
